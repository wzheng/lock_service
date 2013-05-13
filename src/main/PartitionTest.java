package main;
import java.io.*;
import java.util.*;
import java.lang.Math;
import com.thetransactioncompany.jsonrpc2.*;

// Test for re-partitioning

public class PartitionTest {

    public static boolean startTxn(ServerAddress sa,
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
	HashMap<String, Object> params = (HashMap<String, Object>) resp.getNamedParams();
	if (resp.getMethod().equals("abort-done")) {
	    //System.out.println("Transaction " + tidNum + " start is aborted");
	    return false;
	} else if (resp.getMethod().equals("start-done")) {
	    System.out.println("Transaction " + tidNum + " start is done");
	    return true;
	}
	return false;
    }

    public static HashMap<String, Object> commit(ServerAddress sa, 
						 ServerAddress client, 
						 int tidNum, 
						 RPC rpc) {

	TransactionId tid = new TransactionId(sa, tidNum);
	RPCRequest newReq = new RPCRequest("commit", client, tid, new HashMap<String, Object>());
	RPC.send(sa, "commit", "001", newReq.toJSONObject());
	JSONRPC2Request resp = rpc.receive();
	HashMap<String, Object> params = (HashMap<String, Object>) resp.getNamedParams();

	if (resp.getMethod().equals("abort-done")) {
	    //System.out.println("Transaction " + tidNum + " commit is aborted");
	    return null;
	} else if (resp.getMethod().equals("commit-done")) {
	    HashMap<String, Object> args = (HashMap<String, Object>) resp.getNamedParams();
	    System.out.println("Transaction " + tidNum + " commit is done --> " + ((HashMap<String, Object>) args.get("Args")).get("Read Set"));
	    return ((HashMap<String, Object>) ((HashMap<String, Object>) args.get("Args")).get("Read Set"));
	}
	return null;
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
	private ArrayList<String> testKeys;
	private Random random;

	public PartitionTester(ArrayList<ServerAddress> servers, int seed, ServerAddress local_address, ArrayList<String> testKeys) {
	    this.servers = servers;
	    this.seed = seed;
	    address = local_address;
	    rpc = new RPC(address);
	    this.testKeys = testKeys;
	    random = new Random();
	}

	public void run() {

	    HashMap<String, String> committedWrites = new HashMap<String, String>();
	    HashMap<String, String> abortedWrites = new HashMap<String, String>();

	    HashMap<String, String> wset = new HashMap<String, String>();
	    HashMap<String, String> rset = new HashMap<String, String>();
	    
	    int tid = seed;
	    
	    for (int i = 0; i < 50; i++) {

		ServerAddress contact = servers.get((int) (Math.random() * servers.size()));

		// do writes
		for (int j = 0; j < testKeys.size(); j++) {
		    wset.put(testKeys.get(j).toString(), Integer.toString(random.nextInt(1000)));
		}
		
		while (!PartitionTest.startTxn(contact, address, tid, wset, rset, rpc)) {
		    System.out.println("Tries to start W txn " + tid);
		}

		if (Math.random() < 0.5 || tid == 0) {
		    // commit
		    System.out.println("TID " + tid + " start commit");
		    if (PartitionTest.commit(contact, address, tid, rpc) != null) {
			Iterator map_it = wset.entrySet().iterator();
			while (map_it.hasNext()) {
			    Map.Entry entry = (Map.Entry) map_it.next();
			    committedWrites.put((String) entry.getKey(), (String) entry.getValue());
			}
		    }
		} else {
		    // abort
		    System.out.println("TID " + tid + " start abort ");
		    PartitionTest.abort(contact, address, tid, rpc);
		    System.out.println("TID " + tid + " abort done ");
		}
		
		wset.clear();
		tid++;

		Iterator map_it = committedWrites.entrySet().iterator();
		while (map_it.hasNext()) {
		    Map.Entry entry = (Map.Entry) map_it.next();
		    rset.put((String) entry.getKey(), "");
		}

		while (!PartitionTest.startTxn(contact, address, tid, wset, rset, rpc)) {
		    //System.out.println("Tries to start R txn " + tid);
		}

		HashMap<String, Object> read = PartitionTest.commit(contact, address, tid, rpc);
		if (read != null) {
		    Iterator iter = committedWrites.entrySet().iterator();
		    while (iter.hasNext()) {
			Map.Entry entry = (Map.Entry) iter.next();
			if (!read.containsKey(entry.getKey())) {
			    System.out.println("committedWrites: " + committedWrites);
			    System.out.println("read: " + read);
			    System.out.println("1");
			    System.exit(-1);
			} else {
			    if (!read.get(entry.getKey()).equals(entry.getValue())) {
				System.out.println("committedWrites: " + committedWrites);
				System.out.println("read: " + read);
				System.out.println("2");
				System.exit(-1);	
			    } else {
				read.remove(entry.getKey());
			    }
			}
		    }
		    if (!read.isEmpty()) {
			System.out.println("committedWrites: " + committedWrites);
			System.out.println("read: " + read);
			System.out.println("3");
			System.exit(-1);
		    }
		}
		
		rset.clear();
		tid++;
	    }

	    // do reads
	    // wset.clear();
	    // tid++;

	    // Iterator map_it = committedWrites.entrySet().iterator();
	    // while (map_it.hasNext()) {
	    // 	Map.Entry entry = (Map.Entry) map_it.next();
	    // 	rset.put((String) entry.getKey(), "");
	    // }

	    // System.out.println("committedWrites is " + committedWrites);
	    // ServerAddress contact = servers.get((int) (Math.random() * servers.size()));
	    // PartitionTest.startTxn(contact, address, tid, wset, rset, rpc);
	    // PartitionTest.commit(contact, address, tid, rpc);
	}

    }

    public static void main(String[] args) {

	// start client
	ServerAddress client = new ServerAddress(2, "C", 8002);
	RPC rpc = new RPC(client);

	// Re-partitioning Test
	PartitionTable pt1 = new PartitionTable()
;	PartitionTable pt2 = new PartitionTable();
	PartitionTable pt3 = new PartitionTable();
	int numPartitions = 12;
	ServerAddress sa1 = new ServerAddress(0, "S0", 4444);
	ServerAddress sa2 = new ServerAddress(1, "S1", 4445);
	ServerAddress sa3 = new ServerAddress(2, "S2", 4446);

	for (int i = 0; i < 4; i++) {
	    pt1.addPartition(i, sa1);
	    pt2.addPartition(i, sa1);
	    pt3.addPartition(i, sa1);
	}

	for (int i = 4; i < 8; i++) {
	    pt1.addPartition(i, sa2);
	    pt2.addPartition(i, sa2);
	    pt3.addPartition(i, sa2);
	}

	for (int i = 8; i < 12; i++) {
	    pt1.addPartition(i, sa3);
	    pt2.addPartition(i, sa3);
	    pt3.addPartition(i, sa3);
	}

 	ArrayList<ServerAddress> servers = new ArrayList<ServerAddress>();
	servers.add(sa1);
	servers.add(sa2);
	servers.add(sa3);

	ServerStarter s1 = new ServerStarter(sa1, pt1, true, servers);
	ServerStarter s2 = new ServerStarter(sa2, pt2, false, servers);
	ServerStarter s3 = new ServerStarter(sa3, pt3, false, servers);

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

	PartitionTest t = new PartitionTest();

	ArrayList<String> keySet1 = new ArrayList<String>();
	keySet1.add("1");
	keySet1.add("5");
	PartitionTest.PartitionTester t1 = t.new PartitionTester(servers, 10, new ServerAddress(3, "T1", 8888), keySet1);

	ArrayList<String> keySet2 = new ArrayList<String>();
	keySet2.add("2");
	keySet2.add("6");
	PartitionTest.PartitionTester t2 = t.new PartitionTester(servers, 1000, new ServerAddress(4, "T2", 8889), keySet2);

	(new Thread(t1)).start();
	(new Thread(t2)).start();

    }

}