package main;
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
			 PartitionTable config, 
			 boolean isMaster, 
			 ArrayList<ServerAddress> servers) {

        serverQueue = new CommunicationQ();
        puQueue = new CommunicationQ();

        server = new Server(sa, serverQueue, config, isMaster, servers);
        pu = new PartitionUpdater(server, puQueue);
        rpc = new RPC(sa);
    }

    public void run() {
        (new Thread(server)).start();
	(new Thread(pu)).start();

        while (true) {

            JSONRPC2Request req = rpc.receive();

            if (req.getMethod().equals("reconfigure")) {
		String method = req.getMethod();
		Map<String, Object> params = req.getNamedParams();
		RPCRequest rpcReq = new RPCRequest(method, params);
                puQueue.put(rpcReq);
            } else {
		//System.out.println("received something in ServerStarter");
                serverQueue.put(req);
                //System.out.println("added " + req.toJSONString());
            }

        }
    }

}