import java.io.*;
import java.util.*;

import com.thetransactioncompany.jsonrpc2.*;

public class TPCTest {

    // Test for two-phase commit
    
    public static void main(String[] args) {

	// Basic test

	ServerAddress client = new ServerAddress(2, "C", 8002);
	RPC rpc = new RPC(client);

	ServerAddress sa1 = new ServerAddress(0, "S0", 4444);
	ServerAddress sa2 = new ServerAddress(1, "S1", 4445);
	
 	ArrayList<ServerAddress> servers = new ArrayList<ServerAddress>();
	servers.add(sa1);
	servers.add(sa2);

	ServerStarter s1 = new ServerStarter(sa1, null, false, servers);
	ServerStarter s2 = new ServerStarter(sa2, null, false, servers);

	(new Thread(s1)).start();
	(new Thread(s2)).start();

	HashMap<String, Object> rpcArgs = new HashMap<String, Object>();
	// rpcArgs.put("ServerName", sa1.getServerName());
	// rpcArgs.put("ServerNumber", sa1.getServerNumber());
	// rpcArgs.put("ServerPort", sa1.getServerPort());	
	HashMap<String, String> write_set = new HashMap<String, String>();
	write_set.put("a", "b");
	rpcArgs.put("Write Set", write_set);
	rpcArgs.put("Read Set", new HashMap<String, String>());

	TransactionId tid = new TransactionId(sa1, 1);

	System.out.println("Started request for transaction");
	
	RPCRequest newReq = new RPCRequest("start-done", client, tid, rpcArgs);
	RPC.send(sa1, "start", "001", newReq.toJSONObject());

	JSONRPC2Request resp = rpc.receive();
	System.out.println("Received response! " +resp.getParams());
    }

}