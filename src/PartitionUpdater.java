import java.io.*;
import java.util.*;

/**
 * This class takes care of initiating partition transfers
 */

public class PartitionUpdater implements Runnable {

    private Server server;
    private CommunicationQ queue;
    
    public PartitionUpdater(Server server, CommunicationQ queue) {
	this.server = server;
	this.queue = queue;
    }
    
    public void run() {

	while (true) {
	    String str = queue.get();
	    if (str.equals("")) {
		Thread.sleep(0.5);
		continue;
	    }
	    
	    
	    
	}

    }

}
    
