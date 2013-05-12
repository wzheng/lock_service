import java.io.*;
import java.util.*;
import java.lang.Math;

import com.thetransactioncompany.jsonrpc2.*;

// Test for two-phase commit

public class PartitionTest {

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

    public class PartitionTester implements Runnable{
	
	private ArrayList<ServerAddress> servers;
	private int seed;
	private RPC rpc;
	private ServerAddress address;

	public PartitionTester(ArrayList<ServerAddress> servers, int seed, ServerAddress local_address) {
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

	// start client
	ServerAddress client = new ServerAddress(2, "C", 8002);
	RPC rpc = new RPC(client);

	// Re-partitioning Test
	PartitionTable pt = new PartitionTable();
	int numPartitions = 12;
	ServerAddress sa1 = new ServerAddress(0, "S0", 4444);
	ServerAddress sa2 = new ServerAddress(1, "S1", 4445);
	ServerAddress sa3 = new ServerAddress(2, "S2", 4446);

	for (int i = 0; i < 4; i++) {
	    pt.addPartition(i, sa1);
	}

	for (int i = 4; i < 8; i++) {
	    pt.addPartition(i, sa2);
	}

	for (int i = 8; i < 12; i++) {
	    pt.addPartition(i, sa3);
	}

 	ArrayList<ServerAddress> servers = new ArrayList<ServerAddress>();
	servers.add(sa1);
	servers.add(sa2);
	servers.add(sa3);

	ServerStarter s1 = new ServerStarter(sa1, pt, false, servers);
	ServerStarter s2 = new ServerStarter(sa2, pt, false, servers);
	ServerStarter s3 = new ServerStarter(sa3, pt, false, servers);

	(new Thread(s1)).start();
	(new Thread(s2)).start();
	(new Thread(s3)).start();

	HashMap<String, Object> rpcArgs = new HashMap<String, Object>();

	HashMap<String, String> write_set = new HashMap<String, String>();
	HashMap<String, String> read_set = new HashMap<String, String>();


	System.out.println("Started request for transaction 1");
	write_set.put("a", "b");
	write_set.put("b", "a");
	PartitionTest.startTxn(sa1, client, 1, write_set, read_set, rpc);
	PartitionTest.commit(sa1, client, 1, rpc);

	System.out.println("Started request for transaction 2");
	write_set.put("b", "c");
	PartitionTest.startTxn(sa2, client, 2, write_set, read_set, rpc);
	PartitionTest.abort(sa2, client, 2, rpc);

	System.out.println("Started request for transaction 3");
	write_set.clear();
	read_set.put("a", "");
	read_set.put("b", "");
	PartitionTest.startTxn(sa3, client, 3, write_set, read_set, rpc);
	PartitionTest.commit(sa3, client, 3, rpc);

    }

}