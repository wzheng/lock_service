import java.util.*;
import java.io.*;

/**
 *  This simulates a machine that has several partitions of a table
 */

public class Server {

    private ServerAddress address;
    private LockTable lockTable;
    private HashMap<Integer, PartitionData> partitionTable;
    private Data dataStore;
    private ArrayList<ServerAddress> serverList;

    // transactions
    private int numThreads;
    private HashMap<TransactionId, CommunicationQ> activeThreads;

    // reconfiguration
    private boolean isConfiguring;
    private PartitionTable partitionTable;
    private HashMap<Integer, Integer> AF;     // affinity factors
    private ReconfigState reconfigState;      // current state of reconfiguration
    private boolean isMaster;

    // static variables
    private static int MAX_THREADS = 2;

    public Server(ServerAddress address, HashMap<Integer, PartitionData> config, boolean isMaster, ArrayList<ServerAddress> servers) {
	this.address = address;
	lockTable = new LockTable();
	this.rpc = new RPC(name, port1);
	this.partitionTable = config;
	this.dataStore = new Data();
	this.servers = 

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

    public ServerAddress getServerAddress() {
	return this.address;
    }

    // check for incoming requests
    // TODO: currently we have this main thread handle all of the 
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

	    
	    
	}

    }
}

