package main;
import java.io.*;
import java.util.*;
import java.lang.Math;

import com.thetransactioncompany.jsonrpc2.*;

// THIS IS DEPRECATED! THE INTERFACE HAS SINCE CHANGED

// Test for two-phase commit

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

    public class TPCTester implements Runnable{
	
	private ArrayList<ServerAddress> servers;
	private int seed;
	private RPC rpc;
	private ServerAddress address;

	public TPCTester(ArrayList<ServerAddress> servers, int seed, ServerAddress local_address) {
	    this.servers = servers;
	    this.seed = seed;
	    address = local_address;
	    rpc = new RPC(address);
	}

	public void run() {

	    HashMap<String, String> committedWrites = new HashMap<String, String>();
	    HashMap<String, String> abortedWrites = new HashMap<String, String>();

	    HashMap<String, String> wset = new HashMap<String, String>();
	    HashMap<String, String> rset = new HashMap<String, String>();
	    
	    int tid = seed;
	    
	    // do writes
	    for (int i = 0; i < 10; i++) {
		ServerAddress contact = servers.get((int) (Math.random() * servers.size()));
		String key = Integer.toString(seed + i);
		String value = Integer.toString(i);
		wset.put(key, value);
		TPCTest.startTxn(contact, address, tid, wset, rset, rpc);
		if (Math.random() < 0.5) {
		    // commit
		    TPCTest.commit(contact, address, tid, rpc);
		    committedWrites.put(key, value);
		} else {
		    // abort
		    TPCTest.abort(contact, address, tid, rpc);
		    abortedWrites.put(key, "");
		}
		tid += 1;
		wset.clear();
	    }

	    // do reads
	    Iterator it1 = committedWrites.entrySet().iterator();
	    while (it1.hasNext()) {
		ServerAddress contact = servers.get((int) (Math.random() * servers.size()));
		Map.Entry pair = (Map.Entry) it1.next();
		String key = (String) pair.getKey();
		String value = (String) pair.getValue();
		rset.put(key, "");
		System.out.println("Want " + key + ": " + value);
		TPCTest.startTxn(contact, address, tid, wset, rset, rpc);
		TPCTest.commit(contact, address, tid, rpc);
		rset.clear();
	    }

	    Iterator it2 = abortedWrites.entrySet().iterator();
	    while (it2.hasNext()) {
		ServerAddress contact = servers.get((int) (Math.random() * servers.size()));
		Map.Entry pair = (Map.Entry) it2.next();
		String key = (String) pair.getKey();
		String value = (String) pair.getValue();
		rset.put(key, "");
		System.out.println("Want " + key + ": " + value);
		TPCTest.startTxn(contact, address, tid, wset, rset, rpc);
		TPCTest.commit(contact, address, tid, rpc);
		rset.clear();		
	    }
	    
	}

    }

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

	TPCTest t = new TPCTest();

	TPCTest.TPCTester t1 = t.new TPCTester(servers, 10, new ServerAddress(3, "T1", 8888));
	TPCTest.TPCTester t2 = t.new TPCTester(servers, 100, new ServerAddress(4, "T2", 8889));

	(new Thread(t1)).start();
	(new Thread(t2)).start();
    }

}