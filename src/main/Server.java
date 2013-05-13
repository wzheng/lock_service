package main;
import java.util.*;
import java.io.*;


import com.thetransactioncompany.jsonrpc2.*;

/**
 * This simulates a machine that has several partitions of a table
 */

public class Server implements Runnable  {

    private ServerAddress address;
    private LockTable lockTable;
    private Data dataStore;
    private HashMap<Integer, ServerAddress> serverList;

    // transactions
    private int numThreads;
    private HashMap<TransactionId, CommunicationQ> activeWorkers;

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

    public void lockW(String key, TransactionId tid) {
        this.lockTable.lockW(key, tid);
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

    public String get(int partition, String key) {
        return this.dataStore.get(partition, key);
    }

    public void put(int partition, String key, String value) {
        this.dataStore.put(partition, key, value);
    }

    public synchronized int getNumWorkers() {
	return activeWorkers.size();
    }

    public HashMap<String, String> getPartitionData(int partition) {
	return this.dataStore.getPartition(partition);
    }

    public void addPartitionData(int partition, HashMap<String, String> partitionData) {
	this.dataStore.addPartition(partition, partitionData);
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
	    
            if (method.equals("start")) {

		if (this.reconfigState == ReconfigState.CHANGE) {
		    // does not start new transactions during reconfiguration
		    // send "abort" to original server
		    RPCRequest newReq = new RPCRequest("abort", this.address, rpcReq.tid, new HashMap<String, Object>());
		    RPC.send(rpcReq.replyAddress, "abort", "001", newReq.toJSONObject());
		} else {
		    CommunicationQ q = new CommunicationQ();
		    this.activeWorkers.put(rpcReq.tid, q);
		    (new Thread(new Worker(this, q))).start();
		    
		    //System.out.println("Started new worker");
		    q.put(rpcReq);
		}
		
            } else {
            	//System.out.println("Putting req " + method + " in queue " + rpcReq);
		CommunicationQ q = this.activeWorkers.get(rpcReq.tid);
		if (q != null) {
		    q.put(rpcReq);
		}
            }
        }
    }

}
