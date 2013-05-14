package main;
import java.io.*;
import java.util.*;

public class Demo {

    public static void main(String[] args) {

	// initialize the db

	// start client
	ServerAddress client = new ServerAddress(2, "C", 8002);
	RPC rpc = new RPC(client);

	// Re-partitioning Test
	PartitionTable pt1 = new PartitionTable();
	PartitionTable pt2 = new PartitionTable();
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

	// Variables
	HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	HashMap<String, HashMap<String, String> > reads = new HashMap<String, HashMap<String, String> >();

	HashMap<String, String> wset = new HashMap<String, String>();
	HashMap<String, String> rset = new HashMap<String, String>();

	// User input
	Console console = System.console();
	while (true) {
	    String input = console.readLine("> ");
	    String[] split = input.split(" ");

	    if (split[0].equals("START")) {

		int tid = Integer.parseInt(split[1]);
		PartitionTest.startTxn(sa1, client, tid, writes, reads, rpc);

	    } else if (split[0].equals("WRITE")) {

		int tid = Integer.parseInt(split[1]);
		String table = split[2];
		String key = split[3];
		String value = split[4];

		wset.put(key, value);
		writes.put(table, wset);
		
		PartitionTest.startTxn(sa1, client, tid, writes, reads, rpc);
		wset.clear();
		writes.clear();
		
	    } else if (split[0].equals("READ")) {

		int tid = Integer.parseInt(split[1]);
		String table = split[2];
		String key = split[3];

		rset.put(key, "");
		reads.put(table, rset);
		
		PartitionTest.startTxn(sa1, client, tid, writes, reads, rpc);
		rset.clear();
		reads.clear();
		
	    } else if (split[0].equals("COMMIT")) {
		
		int tid = Integer.parseInt(split[1]);
		System.out.println("Transaction " + tid + " committed");
		System.out.println(PartitionTest.commit(sa1, client, tid, rpc));

	    } else if (split[0].equals("ABORT")) {

		int tid = Integer.parseInt(split[1]);
		PartitionTest.abort(sa1, client, tid, rpc);
		System.out.println("Transaction " + tid + " aborted");
		
	    } else if (split[0].equals("QUIT")) {
		System.exit(0);
	    }

	    System.out.println(split[0]);
	}
	
    }
    
}