import java.io.*;
import java.util.*;

import com.thetransactioncompany.jsonrpc2.*;

public class ServerStarter implements Runnable {

    private Server server;
    private PartitionUpdater pu;

    private CommunicationQ serverQueue;
    private CommunicationQ puQueue;

    private RPC rpc;

    public ServerStarter(ServerAddress sa, 
			 HashMap<Integer, PartitionTable.PartitionData> config, 
			 boolean isMaster, 
			 ArrayList<ServerAddress> servers) {

        serverQueue = new CommunicationQ();
        //puQueue = new CommunicationQ();
        server = new Server(sa, serverQueue, config, isMaster, servers);
        //pu = new PartitionUpdater(server, puQueue);
	rpc = new RPC(sa);
    }

    public void run() {
        (new Thread(server)).start();

        while (true) {

            JSONRPC2Request req = rpc.receive();

            if (req.getMethod() == "reconfigure") {
                //puQueue.put(req);
            } else {
		System.out.println("received something in ServerStarter");
                serverQueue.put(req);
            }

        }
    }

}