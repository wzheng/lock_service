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

    // master reconfiguration
    public void configureMaster() {
        // periodically get all AF information from other servers
	while (true) {

	    Object obj = queue.get();
	    if (str.equals("")) {
		continue;
	    }
	    RPCRequest reqIn = (RPCRequest) obj;

	}
    }

    // worker reconfiguration
    public void configure(JSONRPC2Request request) {
	
    }

    public void run() {

	if (this.isMaster) {
	    this.configureMaster();
	}
	
        while (true) {

            // v.1. only the master can construct graphs and perform
            // re-partition
            Object obj = queue.get();
            if (str.equals("")) {
                continue;
            }

            RPCRequest reqIn = (RPCRequest) obj;
            this.configure(reqIn);
        }

    }

}
