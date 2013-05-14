package main;
import java.io.*;
import java.util.*;
import java.lang.Math;
import com.thetransactioncompany.jsonrpc2.*;

// Test for re-partitioning

public class DeadlockTest {
	
	public static boolean verbose = true;
	
	public static void print(String s){
		if (verbose)
			System.out.println(s);
	}

	   public static boolean startTxn(ServerAddress sa,
			   ServerAddress client,
			   int tidNum, 
			   HashMap<String, HashMap<String, String> > write_set, 
			   HashMap<String, HashMap<String, String> > read_set, 
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
    //System.out.println("Transaction " + tidNum + " start is done");
    return true;
}
return false;
}

public static HashMap<String, HashMap<String, String> > commit(ServerAddress sa, 
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
    //System.out.println("Transaction " + tidNum + " commit is done --> " + ((HashMap<String, Object>) args.get("Args")).get("Read Set"));
    return ((HashMap<String, HashMap<String, String> >) ((HashMap<String, Object>) args.get("Args")).get("Read Set"));
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

    public class DeadlockTester implements Runnable{
	
	private ArrayList<ServerAddress> servers;
	private int seed;
	private RPC rpc;
	private ServerAddress address;
	private ArrayList<String> testKeys;
	private Random random;

	public DeadlockTester(ArrayList<ServerAddress> servers, int seed, ServerAddress local_address, ArrayList<String> testKeys) {
	    this.servers = servers;
	    this.seed = seed;
	    address = local_address;
	    rpc = new RPC(address);
	    this.testKeys = testKeys;
	    random = new Random();
	}

	public void run() {

	    HashMap<String, HashMap<String, String> > actualWrites = new HashMap<String, HashMap<String, String> >();
	    HashMap<String, HashMap<String, String> > actualReads = new HashMap<String, HashMap<String, String> >();

	    HashMap<String, String> committedWrites = new HashMap<String, String>();
	    HashMap<String, String> abortedWrites = new HashMap<String, String>();

	    HashMap<String, String> wset = new HashMap<String, String>();
	    HashMap<String, String> rset = new HashMap<String, String>();

	    actualWrites.put("table", wset);
	    actualReads.put("table", rset);
	    
	    int tid = seed;
	    
	    for (int i = 0; i < 50; i++) {

		ServerAddress contact = servers.get((int) (Math.random() * servers.size()));

		// do writes
		for (int j = 0; j < testKeys.size(); j++) {
		    wset.put(testKeys.get(j).toString(), Integer.toString(random.nextInt(1000)));
		}

		while (!DeadlockTest.startTxn(contact, address, tid, actualWrites, actualReads, rpc)) {
		    //System.out.println("Tries to start W txn " + tid);
		}

		if (Math.random() < 0.5 || tid == 0) {
		    // commit
		    System.out.println("TID " + tid + " start commit");
		    if (DeadlockTest.commit(contact, address, tid, rpc) != null) {
			Iterator map_it = wset.entrySet().iterator();
			while (map_it.hasNext()) {
			    Map.Entry entry = (Map.Entry) map_it.next();
			    committedWrites.put((String) entry.getKey(), (String) entry.getValue());
			}
		    }
		} else {
		    // abort
		    //System.out.println("TID " + tid + " start abort ");
		    DeadlockTest.abort(contact, address, tid, rpc);
		    //System.out.println("TID " + tid + " abort done ");
		}
		
		wset.clear();
		tid++;

		Iterator map_it = committedWrites.entrySet().iterator();
		while (map_it.hasNext()) {
		    Map.Entry entry = (Map.Entry) map_it.next();
		    rset.put((String) entry.getKey(), "");
		}

		while (!DeadlockTest.startTxn(contact, address, tid, actualWrites, actualReads, rpc)) {
		    //System.out.println("Tries to start R txn " + tid);
		}

		HashMap<String, HashMap<String, String> > read_map = DeadlockTest.commit(contact, address, tid, rpc);
		HashMap<String, String> read = read_map.get("table");
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

	    System.out.println("SUCCESS from seed " + seed);
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

	/*
	HashMap<String, Object> rpcArgs = new HashMap<String, Object>();

	HashMap<String, String> write_set = new HashMap<String, String>();
	HashMap<String, String> read_set = new HashMap<String, String>();


	System.out.println("Started request for transaction 1");
	HashMap<String, String> write_set1a = new HashMap<String, String>();
	write_set1a.put("1table", "a");
	DeadlockTest.startTxn(sa1, client, 1, write_set1a, read_set, rpc);
	
	System.out.println("Started request for transaction 2");
	
	HashMap<String, String> write_set2a = new HashMap<String, String>();
	write_set2a.put("2table", "b");
	DeadlockTest.startTxn(sa2, client, 2, write_set2a, read_set, rpc);
	
	HashMap<String, String> write_set2b = new HashMap<String, String>();
	write_set2b.put("1table", "d");
	DeadlockTest.getSingleLock(sa2, client, 2, write_set2b, rpc);
	
	HashMap<String, String> write_set1b = new HashMap<String, String>();
	write_set1b.put("2table", "c");
	DeadlockTest.getSingleLock(sa1, client, 1, write_set1b, rpc);

	System.out.println("Started request for transaction 3");
	write_set.clear();
	read_set.put("a", "");
	read_set.put("b", "");
	DeadlockTest.startTxn(sa3, client, 3, write_set, read_set, rpc);
	DeadlockTest.commit(sa3, client, 3, rpc);
	*/

	DeadlockTest t = new DeadlockTest();

	ArrayList<String> keySet1 = new ArrayList<String>();
	keySet1.add("1");
	keySet1.add("5");
	DeadlockTest.DeadlockTester t1 = t.new DeadlockTester(servers, 10, new ServerAddress(3, "T1", 8888), keySet1);

	ArrayList<String> keySet2 = new ArrayList<String>();
	keySet2.add("5");
	keySet2.add("1");
	DeadlockTest.DeadlockTester t2 = t.new DeadlockTester(servers, 1000, new ServerAddress(4, "T2", 8889), keySet2);

	(new Thread(t1)).start();
	(new Thread(t2)).start();

    }

}