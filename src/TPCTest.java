import java.io.*;
import java.util.*;

import com.thetransactioncompany.jsonrpc2.*;

public class TPCTest {

    public static void startTxn(ServerAddress sa,
				ServerAddress client,
				int tidNum, 
				HashMap<String, String> write_set, 
				HashMap<String, String> read_set, 
				RPC rpc) {

	TransactionId tid = new TransactionId(sa, tidNum);
	HashMap<String, Object> rpcArgs = new HashMap<String, Object>();
	rpcArgs.put("Read Set", read_set);
	rpcArgs.put("Write Set", write_set);
	RPCRequest newReq = new RPCRequest("start", client, tid, rpcArgs);
	RPC.send(sa, "start", "001", newReq.toJSONObject());
	JSONRPC2Request resp = rpc.receive();       
    }

    public static void commit(ServerAddress sa, 
			      ServerAddress client, 
			      int tidNum, 
			      RPC rpc) {

	TransactionId tid = new TransactionId(sa, tidNum);
	RPCRequest newReq = new RPCRequest("commit", client, tid, new HashMap<String, Object>());
	RPC.send(sa, "commit", "001", newReq.toJSONObject());
	JSONRPC2Request resp = rpc.receive();
	HashMap<String, Object> params = (HashMap<String, Object>) resp.getParams();
	System.out.println("Returned read set " + params.get("Args"));
    }

    public static void abort(ServerAddress sa, 
			     ServerAddress client, 
			     int tidNum,
			     RPC rpc) {

	TransactionId tid = new TransactionId(sa, tidNum);
	RPCRequest newReq = new RPCRequest("abort", client, tid, new HashMap<String, Object>());
	RPC.send(sa, "abort", "001", newReq.toJSONObject());
	JSONRPC2Request resp = rpc.receive();
    }

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

	HashMap<String, String> write_set = new HashMap<String, String>();
	HashMap<String, String> read_set = new HashMap<String, String>();


	System.out.println("Started request for transaction 1");
	write_set.put("a", "b");
	write_set.put("b", "a");
	TPCTest.startTxn(sa1, client, 1, write_set, read_set, rpc);
	TPCTest.commit(sa1, client, 1, rpc);

	System.out.println("Started request for transaction 2");
	write_set.put("b", "c");
	TPCTest.startTxn(sa2, client, 2, write_set, read_set, rpc);
	TPCTest.abort(sa2, client, 2, rpc);

	System.out.println("Started request for transaction 3");
	write_set.clear();
	read_set.put("a", "");
	read_set.put("b", "");
	TPCTest.startTxn(sa1, client, 3, write_set, read_set, rpc);
	TPCTest.commit(sa1, client, 3, rpc);
    }

}