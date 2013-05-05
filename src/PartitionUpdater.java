import java.io.*;
import java.util.*;
import com.thetransactioncompany.jsonrpc2.*;

/**
 * This class takes care of initiating partition transfers
 */

public class PartitionUpdater implements Runnable {

    private Server server;
    private CommunicationQ queue;
    private HashMap<Integer, Integer> AF;
    private ReconfigState reconfigState;
    private boolean isMaster;
    
    public PartitionUpdater(Server server, CommunicationQ queue) {
	this.server = server;
	this.queue = queue;
	AF = this.server.getAF();
	reconfigState = this.server.getReconfigState();
	this.isMaster = this.server.isMaster();
    }

    // only run by the master
    public void configureMaster() {
	// get all AF information from other servers
	
	
    }
    
    // Reconfiguration
    public void configure(JSONRPC2Request request) {
	
	switch(call) {
		case "": 
	}
	
	//synchronized(this.AF) {
	// ArrayList<Integer> partitions = new ArrayList<Integer>();
	
	// Iterator itr = this.AF.entrySet().iterator();
	// while (itr.hasNext()) {
	//     Map.Entry pairs = (Map.Entry) itr.next();
	//     Integer partitionNum = pairs.getKey();
	//     Integer af = pairs.getValue();
	
	//     if (af.intValue() > THRESHOLD) {
	// 	partitions.add(af);
	//     }
	// }
	
	//}
	
    }
    
    public void run() {

	while (true) {

	    // v.1. only the master can construct graphs and perform re-partition
	    if (this.isMaster) {
	    	this.configureMaster();
	    }

	    String str = (String) queue.get();
	    if (str.equals("")) {
	    	Thread.sleep((long)0.5);
	    	continue;
	    }

	    JSONRPC2Request reqIn = null;
	    try {
	    	reqIn = JSONRPC2Request.parse(jsonString);
	    } catch (JSONRPC2ParseException e) {
	    	System.err.println("PartitionUpdater ERROR: " + e.getMessage());
	    }

	    this.configure(reqIn);
	    
	}

    }

}
    
