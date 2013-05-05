import java.util.*;
import java.io.*;

import com.thetransactioncompany.jsonrpc2.*;

import static PartitionTable.PartitionData;

/**
 *  This simulates a machine that has several partitions of a table
 */

public class Server {

    private ServerAddress address;
    private LockTable lockTable;
    private HashMap<Integer, PartitionTable.PartitionData> partitionTable;
    private Data dataStore;
    private HashMap<Integer, ServerAddress> serverList;

    // transactions
    private int numThreads;
    private HashMap<TransactionId, CommunicationQ> activeThreads;

    // reconfiguration
    private boolean isConfiguring;
    private PartitionTable.PartitionData partitionTable;
    private HashMap<Integer, Integer> AF;     // affinity factors
    private ReconfigState reconfigState;      // current state of reconfiguration
    private boolean isMaster;
	private RPC rpc;

    // static variables
    private static int MAX_THREADS = 2;

    public Server(ServerAddress address, HashMap<Integer, PartitionTable.PartitionData> config, boolean isMaster, ArrayList<ServerAddress> servers) {
	this.address = address;
	lockTable = new LockTable();
	this.rpc = new RPC(name, port1);
	this.partitionTable = config;
	this.dataStore = new Data();
	
	Iterator servers_it = servers.iterator();
	while (servers_it.hasNext()) {
	    ServerAddress sa = (ServerAddress) servers_it.next();
	    serverList.put(sa.getServerNumber(), sa);
	}
	
	Iterator<Integer> itr = config.getPartitions();
	while (itr.hasNext()) {
	    Integer i = (Integer)itr.next();
	    dataStore.addNewPartition(i);
	}
	
	this.isConfiguring = false;
	
	numThreads = 0;
		
	AF = new HashMap<Integer, Integer>();
	reconfigState = ReconfigState.NONE;
	this.isMaster = isMaster;
    }

    public PartitionTable getPartitionTable() {
    	return partitionTable;
    }

    // For a certain key, returns the partition that key belongs to
    public static int hashKey(String key) {
    	return (key.hashCode())%(serverList.size());
    }

    public HashMap<Integer, Integer> getAF() {
    	return AF;
    }

    public ReconfigState getReconfigState() {
    	return reconfigState;
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

    public void lockW(String key) {
    	this.lockTable.lockW(key);
    }

    public void lockR(String key) {
    	this.lockTable.lockR(key);
    }

    public void unlockW(String key) {
    	this.lockTable.unlockW(key);
    }

    public void unlockR(String key) {
    	this.lockTable.unlockR(key);
    }

    public get(String key) {
    	return this.dataStore.get(key);
    }

    public put(String key, String value) {
    	return this.dataStore.put(key, value);
    }

    // check for incoming requests, spawn new worker threads as necessary
    public void run() {
	
	while (true) {

	    String str = queue.get();
	    if (str.equals("")) {
	    	Thread.sleep((long)0.5);
	    	continue;
	    }
	    
	    JSONRPC2Request reqIn = null;
	    try {
	    	reqIn = JSONRPC2Request.parse(jsonString);
	    } catch (JSONRPC2ParseException e) {
	    	System.err.println("ERROR: " + e.getMessage());
	    }

	    // TODO: duplicate messages?

	    String method = reqIn.getMethod();
	    Map<String, Object> params = reqIn.getNamedParams();

	    if (method.equals("start")) {
	    	RPCRequest rpcReq = new RPCRequest(params);
		
	    	TransactionContext t = new TransactionContext();
	    	Transaction.parseJSON(params);
		
	    	CommunicationQ q = new CommunicationQ();
	    	this.activeWorkers.put(tid, q);
	    	(new Worker(this, q)).run();

	    	rpcReq.addArgs(t);
	    	q.put(rpcReq);

	    } else if (method.equals("abort")) {
		
		//TransactionId tid = ??;
		this.activeWorkers.get().put(rpcReq);
		
	    } else if (method.equals("commit")) {
		//TranscationId tid = ??;
		this.activeWorkers.get(new TransactionId(this.address, params.get("TID"))).put(rpcReq);
	
	    } else if (method.equals("start-reply")) {
	    	RPCRequest rpcReq = new RPCRequest(params);
	    	rpcReq.addArgs(params.get("Read Set"));
	    	this.activeWorkers.get(new TransactionId(this.address, params.get("TID"))).put(rpcReq);
	    }
	}
    }
}

