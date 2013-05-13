package main;

import java.util.*;
import java.util.Map.Entry;
import java.lang.Math;
import com.thetransactioncompany.jsonrpc2.*;

// Test for re-partitioning

public class DeadlockTest {

	public static boolean startTxn(ServerAddress sa, ServerAddress client,
			int tidNum, HashMap<String, String> write_set,
			HashMap<String, String> read_set, RPC rpc) {

		TransactionId tid = new TransactionId(sa, tidNum);
		HashMap<String, Object> rpcArgs = new HashMap<String, Object>();
		rpcArgs.put("Read Set", read_set);
		rpcArgs.put("Write Set", write_set);
		RPCRequest newReq = new RPCRequest("start", client, tid, rpcArgs);
		RPC.send(sa, "start", "001", newReq.toJSONObject());
		JSONRPC2Request resp = rpc.receive();
		HashMap<String, Object> params = (HashMap<String, Object>) resp
				.getNamedParams();
		if (resp.getMethod().equals("abort")) {
			// System.out.println("Transaction " + tidNum +
			// " start is aborted");
			return false;
		} else if (resp.getMethod().equals("start-done")) {
			// System.out.println("Transaction " + tidNum + " start is done");
			return true;
		}
		return true;
	}

	public static boolean commit(ServerAddress sa, ServerAddress client,
			int tidNum, RPC rpc) {

		TransactionId tid = new TransactionId(sa, tidNum);
		RPCRequest newReq = new RPCRequest("commit", client, tid,
				new HashMap<String, Object>());
		RPC.send(sa, "commit", "001", newReq.toJSONObject());
		JSONRPC2Request resp = rpc.receive();
		HashMap<String, Object> params = (HashMap<String, Object>) resp
				.getNamedParams();

		if (resp.getMethod().equals("abort-done")) {
			// System.out.println("Transaction " + tidNum +
			// " commit is aborted");
			return false;
		} else if (resp.getMethod().equals("commit-done")) {
			HashMap<String, Object> args = (HashMap<String, Object>) resp
					.getNamedParams();
			System.out.println("Transaction "
					+ tidNum
					+ " commit is done --> "
					+ ((HashMap<String, Object>) args.get("Args"))
							.get("Read Set"));
			return true;
		}
		return true;
	}

	public static void abort(ServerAddress sa, ServerAddress client,
			int tidNum, RPC rpc) {

		TransactionId tid = new TransactionId(sa, tidNum);
		RPCRequest newReq = new RPCRequest("abort", client, tid,
				new HashMap<String, Object>());
		RPC.send(sa, "abort", "001", newReq.toJSONObject());
		JSONRPC2Request resp = rpc.receive();
		System.out.println(resp.toJSONString());
	}

	public class DeadlockTester implements Runnable {

		private ArrayList<ServerAddress> servers;
		private int seed;
		private RPC rpc;
		private ServerAddress address;
		private ArrayList<String> testKeys;
		private Random random;

		public DeadlockTester(ArrayList<ServerAddress> servers, int seed,
				ServerAddress local_address, ArrayList<String> testKeys) {
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

			// do writes
			for (int i = 0; i < 50; i++) {

				ServerAddress contact = servers
						.get((int) (Math.random() * servers.size()));

				for (int j = 0; j < testKeys.size(); j++) {
					wset.put(testKeys.get(j).toString(),
							Integer.toString(random.nextInt(1000)));
				}

				if (DeadlockTest.startTxn(contact, address, tid, wset, rset,
						rpc)) {
					if (Math.random() < 0.5) {
						// commit
						if (DeadlockTest.commit(contact, address, tid, rpc)) {
							Iterator<Entry<String, String>> map_it = wset.entrySet().iterator();
							while (map_it.hasNext()) {
								Map.Entry<String, String> entry = map_it.next();
								committedWrites.put((String) entry.getKey(),
										(String) entry.getValue());
							}
						}
					} else {
						// abort
						DeadlockTest.abort(contact, address, tid, rpc);
					}

					wset.clear();
				}

				tid++;
			}

			// do reads
			wset.clear();
			tid++;

			Iterator<Entry<String, String>> map_it = committedWrites.entrySet().iterator();
			while (map_it.hasNext()) {
				Map.Entry<String, String> entry = map_it.next();
				rset.put((String) entry.getKey(), "");
			}

			System.out.println("committedWrites is " + committedWrites);
			ServerAddress contact = servers.get((int) (Math.random() * servers
					.size()));
			DeadlockTest.startTxn(contact, address, tid, wset, rset, rpc);
			DeadlockTest.commit(contact, address, tid, rpc);
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

		ServerStarter s1 = new ServerStarter(sa1, pt, true, servers);
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
		DeadlockTest.startTxn(sa1, client, 1, write_set, read_set, rpc);
		
		System.out.println("Started request for transaction 2");
		DeadlockTest.startTxn(sa2, client, 2, write_set, read_set, rpc);
		//write_set.put("a", "c");
		DeadlockTest.commit(sa1, client, 1, rpc);
		DeadlockTest.commit(sa2, client, 2, rpc);

		System.out.println("Started request for transaction 3");
		write_set.clear();
		read_set.put("a", "");
		read_set.put("b", "");
		DeadlockTest.startTxn(sa3, client, 3, write_set, read_set, rpc);
		DeadlockTest.commit(sa3, client, 3, rpc);

		DeadlockTest t = new DeadlockTest();

		ArrayList<String> keySet = new ArrayList<String>();
		keySet.add("1");
		keySet.add("5");
		DeadlockTest.DeadlockTester t1 = t.new DeadlockTester(servers, 10,
				new ServerAddress(3, "T1", 8888), keySet);

		(new Thread(t1)).start();
	}

}