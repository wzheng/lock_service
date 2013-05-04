import java.io.*;
import java.util.*;


public class Worker implements Runnable {
    
    private Server server;
    private CommunicationQ queue;

    public Worker(Server server, CommunicationQ queue) {
	this.server = server;
	this.queue = queue;
    }

    public void startTransaction(TransactionId tid, boolean isRW, HashMap<String, Record> write_set, HashSet<String> read_set) {
	if (this.server.getServerAddress().equals(tid.getServerAddress())) {
	    // need to make requests to get all locks from necessary servers
	    
	    
	} else {
	    // only get local locks
	    
	}
	
    }

    public void abort(TransactionId tid) {
	// atomically abort the transaction, release all locks held by the txn
	
    }

    public void commit(TransactionId tid) {
	// atomically commit the transaction, release all locks held by the txn
	
    }

    public void run() {

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