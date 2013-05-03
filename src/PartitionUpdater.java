import java.io.*;
import java.util.*;

/**
 * This class takes care of initiating partition transfers
 */

public class PartitionUpdater implements Runnable {

    private Server server;
    private int port;
    private RPC rpc;
    
    public PartitionUpdater(Server server, int port) {
	this.server = server;
	this.port = port;
	rpc = new RPC("", port);
    }
    
    while (true) {
	// check for partition data
	JSONRPC2Request reqIn = rpc.receive();
	
	
	
	Thread.sleep(1);
    }

}
    
