import java.io.*;
import java.util.*;

public class ServerStarter {

    private Server server;
    private PartitionUpdater pu;

    private CommunicationQ serverQueue;
    private CommunicationQ puQueue;
    
    public ServerStarter(String name, int port, int numServers, HashMap<Integer, PartitionData> config) {
	serverQueue = new CommunicationQ();
	puQueue = new CommunicationQ();
	server = new Server(name, numServers, config, );
	pu = new PartitionUpdater(server);
    }

    public void run() {
	server.run();
	pu.run();

	while (true) {

	    JSONRPC2Request req = rpc.receive();
	    if (reqIn.getMethod() == "reconfigure") {
		puQueue.put(req);
	    } else {
		serverQueue.put(req);
	    }
	    
	}
    }

}