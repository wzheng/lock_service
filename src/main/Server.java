package main;
import java.util.*;
import java.io.*;


import com.thetransactioncompany.jsonrpc2.*;

/**
 * This simulates a machine that has several partitions of a table
 */

public class Server implements Runnable  {
	
	//enable deadlocking
	public static boolean DEADLOCK_DETECTION_WFG = true;
	public static boolean DEADLOCK_DETECTION_TIMEOUT = true;

    private ServerAddress address;
    private LockTable lockTable;
    private Data dataStore;
    private HashMap<Integer, ServerAddress> serverList;

    // transactions
    private int numThreads;
    private HashMap<TransactionId, CommunicationQ> activeWorkers;
    private HashMap<TransactionId, CommunicationQ> activeDeadlockWorkers;
	//private HashMap<TransactionId, CommunicationQ> activeWFGWorkers;

    // reconfiguration
    private boolean isConfiguring;
    private PartitionTable table;
    private HashMap<Integer, Integer> AF; // affinity factors
    private ReconfigState reconfigState; // current state of reconfiguration
    private boolean isMaster;
    private RPC rpc;
    private CommunicationQ queue;
    private AFTable af;


    // static variables
    private static int MAX_THREADS = 2;

    public Server(ServerAddress address, CommunicationQ queue,
		  PartitionTable config,
		  boolean isMaster, ArrayList<ServerAddress> servers) {

        this.address = address;
        lockTable = new LockTable(address);
        this.dataStore = new Data();
        this.queue = queue;
        this.serverList = new HashMap<Integer, ServerAddress>();
	this.table = config;

        Iterator servers_it = servers.iterator();
        while (servers_it.hasNext()) {
            ServerAddress sa = (ServerAddress) servers_it.next();
            serverList.put(new Integer(sa.getServerNumber()), sa);
        }

        this.isConfiguring = false;

        numThreads = 0;

	af = new AFTable();
        reconfigState = ReconfigState.READY;
        this.isMaster = isMaster;

	this.activeWorkers = new HashMap<TransactionId, CommunicationQ>();
	this.activeDeadlockWorkers = new HashMap<TransactionId, CommunicationQ>();
	//this.activeWFGWorkers = new HashMap<TransactionId, CommunicationQ>();
    }

    
    public HashSet<TransactionId> getGlobalWFG(TransactionId tid){
    	HashSet<TransactionId> g = new HashSet<TransactionId>();
    	for (ServerAddress s : serverList.values()){
    		HashMap<String, Object> args = new HashMap<String, Object>();
    		RPCRequest sendReq = new RPCRequest("get-wfg", address, tid, args);
    		synchronized(this){
    			RPC.send(s, "get-wfg", "001", args);
    			JSONRPC2Request x = null;
				try {
					x = JSONRPC2Request.parse((String)queue.get());
				} catch (JSONRPC2ParseException e) {
					e.printStackTrace();
				}
    			HashMap<String, Object> rep = (HashMap<String, Object>) x.getNamedParams();
    			HashSet<TransactionId> tmp = (HashSet<TransactionId>) rep.get("wfg");
    			for (TransactionId tmpTid : tmp){
    				g.add(tmpTid);
    			}
    		}
    	}
    	return g;
    }
    
    public HashMap<Integer, ServerAddress> getAllServers() {
	return this.serverList;
    }

    public PartitionTable getPartitionTable() {
        return table;
    }
    
    public HashSet<TransactionId> getWFG(TransactionId tid){
    	return lockTable.getWFG(tid);
    }

    // For a certain key, returns the partition that key belongs to
    public int hashKey(String key) {
        return (key.hashCode()) % (this.table.numPartitions);
    }

    public AFTable getAF() {
        return af;
    }

    public ReconfigState getReconfigState() {
        return reconfigState;
    }

    public synchronized void setReconfigState(ReconfigState state) {
        this.reconfigState = state;
    }

    public boolean isMaster() {
        return this.isMaster;
    }

    public ServerAddress getAddress() {
        return this.address;
    }

    public ServerAddress getServerAddress(int serverNum) {
        return this.serverList.get(new Integer(serverNum));
    }

    public int getServerNumber() {
        return this.address.getServerNumber();
    }

    public boolean lockW(String key, TransactionId tid) {
    	return this.lockTable.lockW(key, tid);
    }

    public void lockR(String key, TransactionId tid) {
        this.lockTable.lockR(key, tid);
    }

    public void unlockW(String key, TransactionId tid) {
        this.lockTable.unlockW(key, tid);
    }

    public void unlockR(String key, TransactionId tid) {
        this.lockTable.unlockR(key, tid);
    }

    public String get(int partition, String table, String key) {
        return this.dataStore.get(partition, table, key);
    }

    public void put(int partition, String table, String key, String value) {
        this.dataStore.put(partition, table, key, value);
    }

    public synchronized int getNumWorkers() {
	return activeWorkers.size();
    }

    public HashMap<String, HashMap<String, String> > getPartitionData(int partition) {
	return this.dataStore.getPartition(partition);
    }

    public void addPartitionData(int partition, HashMap<String, HashMap<String, String> > partitionData) {
	this.dataStore.addPartition(partition, partitionData);
    }

    public synchronized void threadDone(TransactionId tid) {
	this.activeWorkers.remove(tid);
	this.activeDeadlockWorkers.remove(tid);
	//this.activeWFGWorkers.remove(tid);
    }

    // check for incoming requests, spawn new worker threads as necessary
    public void run() {

        while (true) {

	    Object in = queue.get();
	    if (in.equals("")) {
	    	continue;
	    }

            JSONRPC2Request reqIn = (JSONRPC2Request) in;

            //System.out.println("Server " + address.getServerName() + " received request for " + reqIn.getMethod());
	    
            String method = reqIn.getMethod();
            Map<String, Object> params = reqIn.getNamedParams();
            RPCRequest rpcReq = new RPCRequest(method, params);
	    
            if (method.equals("start") && !this.activeWorkers.containsKey(rpcReq.tid)) {

		if (this.reconfigState == ReconfigState.CHANGE) {
		    // does not start new transactions during reconfiguration
		    // send "abort" to original server
		    if (rpcReq.tid.getServerAddress().equals(this.address)) {
			RPCRequest newReq = new RPCRequest("abort-done", this.address, rpcReq.tid, new HashMap<String, Object>());
			RPC.send(rpcReq.replyAddress, "abort-done", "001", newReq.toJSONObject());
		    } else {
			RPCRequest newReq = new RPCRequest("abort-reply", this.address, rpcReq.tid, new HashMap<String, Object>());
			RPC.send(rpcReq.replyAddress, "abort-reply", "001", newReq.toJSONObject());
		    }
		} else {
		    CommunicationQ q = new CommunicationQ();
		    this.activeWorkers.put(rpcReq.tid, q);
		    Worker w = new Worker(this, q);
		    (new Thread(w)).start();
		    
		    CommunicationQ q2 = new CommunicationQ();
		    if(Server.DEADLOCK_DETECTION_WFG){
		    this.activeDeadlockWorkers.put(rpcReq.tid, q2);
		    DeadlockWorker dw = new DeadlockWorker(this, q2, w);
		    (new Thread(dw)).start();
		    }
		    
		    //CommunicationQ q3 = new CommunicationQ();
		    //this.activeWFGWorkers.put(rpcReq.tid, q3);
		    //WFGWorker wf = new WFGWorker(this, q3);
		    //(new Thread(wf)).start();
		    //w.setWFGWorker(wf);
		    
		    //System.out.println("Started new worker");
			if (Server.DEADLOCK_DETECTION_WFG && rpcReq.method.equals("deadlock")){
				q2.put(rpcReq);
			}
			//else if ((rpcReq.method.equals("get-wfg") || rpcReq.method.equals("wfg-response")) && q3 != null){
			//	q3.put(rpcReq);
			//}
			else if (q != null) {
			    q.put(rpcReq);
			}
		}
		
            } else {
            	//System.out.println("Putting req " + method + " in queue " + rpcReq);
		CommunicationQ q = this.activeWorkers.get(rpcReq.tid);
		CommunicationQ q2 = this.activeDeadlockWorkers.get(rpcReq.tid);
		//CommunicationQ q3 = this.activeWFGWorkers.get(rpcReq.tid);
		if (Server.DEADLOCK_DETECTION_WFG && rpcReq.method.equals("deadlock") && q2 != null){
			q2.put(rpcReq);
		}
		//else if ((rpcReq.method.equals("get-wfg") || rpcReq.method.equals("wfg-response")) && q3 != null){
		//	q3.put(rpcReq);
		//}
		else if (q != null) {
		    q.put(rpcReq);
		}

            }
        }
    }

}
