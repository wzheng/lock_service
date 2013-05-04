import java.util.*;
import java.io.*;

/**
 *  This simulates a machine that has several partitions of a table
 */

public class Server {

    public num ReconfigState {NONE, PREPARE, ACCEPT, CHANGE};

    private ServerAddress address;
    private LockTable lockTable;
    private HashMap<Integer, PartitionData> partitionTable;
    private Data dataStore;
    private int port2;

    // transactions
    private int numThreads;

    // reconfiguration
    private boolean isConfiguring;
    private PartitionTable partitionTable;
    private HashMap<Integer, Integer> AF;     // affinity factors
    private ReconfigState reconfigState;

    // static variables
    private static int MAX_THREADS = 2;

    public Server(String name, int port1, int port2, int numServers, HashMap<Integer, PartitionData> config) {
	this.address = new ServerAddress(name, port1);
	lockTable = new LockTable();
	this.rpc = new RPC(name, port1);
	this.partitionTable = config;
	this.dataStore = new Data();

	Iterator<Integer> itr = config.getPartitions();
	while (itr.hasNext()) {
	    Integer i = (Integer)itr.next();
	    dataStore.addNewPartition(i);
	}

	this.port2 = port2;
	this.isConfiguring = false;

	numThreads = 0;
	
	AF = new HashMap<Integer, Integer>();
    }

    public PartitionTable getPartitionTable() {
	synchronized(PartitionTable) {
	    return partitionTable;
	}
    }

    public int getServerPort() {
	return address.getPort();
    }

    public int getPort2() {
	return port2;
    }
    
    public void startTransaction() {
	// acquire all locks necessary
    }

    public void abort() {
	// atomically abort the transaction, release all locks held by the txn
	
    }

    public void commit() {
	// atomically commit the transaction, release all locks held by the txn
	
    }

    public void get(String key) {
	
    }

    public void put(String key, String value) {
	
    }

    // Reconfiguration
    public void configure(JSONRPC2Request request) {
	if (req == null) {
	    if 
	    
	    synchronized(this.AF) {

		ArrayList<Integer> partitions = new ArrayList<Integer>();

		Iterator itr = this.AF.entrySet().iterator();
		while (itr.hasNext()) {
		    Map.Entry pairs = (Map.Entry) itr.next();
		    Integer partitionNum = pairs.getKey();
		    Integer af = pairs.getValue();
		    
		    if (af.intValue() > THRESHOLD) {
			partitions.add(af);
		    }
		}
		
		// if there are partitions to be 
		if (!partitions.isEmpty()) {
		    
		}
	    }
	} else {
	    // process the messages
	    
	}
    }

    // check for incoming requests
    public void run() {
	
	while (true) {

	    String str = queue.get();
	    if (str.equals("")) {
		Thread.sleep(0.5);
		continue;
	    }
	    
	    JSONRPC2Request reqIn = null;
	    try {
		reqIn = JSONRPC2Request.parse(jsonString);
	    } catch (JSONRPC2ParseException e) {
		System.err.println("ERROR: " + e.getMessage());
	    }

	    switch (method) {
		
	    case "reconfigure":
		this.reconfigure(reqIn);
		break;

	    case "get": 
		break;

	    case "put": 
		break;

	    case "startTransaction": 
		break;

	    case "commit": 
		break;

	    case "abort": 
		break;

	    }

	}

    }
}

