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
    private int port2;

    // for reconfiguration
    private boolean isConfiguring;

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

    // check for incoming requests
    public void run() {
	(new Thread(new PartitionUpdater(this, port2))).run();
	
	while (true) {

	    JSONRPC2Request reqIn = rpc.receive();
	    String method = reqIn.getMethod();

	    switch (method) {
		
	    case "reconfigure": 
		synchronized(this.isConfiguring) {
		    if (!this.isConfiguring) {
			this.isConfiguring = true;
			rpc.send(port2, reqIn);
		    }
		}
		break;

	    case "lock": 
		this.lock();
		break;

	    }

	}

    }
}

