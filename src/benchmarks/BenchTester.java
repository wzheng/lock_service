package benchmarks;

import java.util.*;
import java.io.*;

import main.*;

public class BenchTester {

    public static void main(String[] args) {
	
	// start client
	ServerAddress client = new ServerAddress(2, "C", 8002);
	//RPC rpc = new RPC(client);

	// Re-partitioning Test
	PartitionTable pt1 = new PartitionTable();
	PartitionTable pt2 = new PartitionTable();
	PartitionTable pt3 = new PartitionTable();
	ServerAddress sa1 = new ServerAddress(0, "S0", 4444);
	ServerAddress sa2 = new ServerAddress(1, "S1", 4445);
	ServerAddress sa3 = new ServerAddress(2, "S2", 4446);

	int numPartitions = 12;

	for (int i = 0; i < numPartitions/3; i++) {
	    pt1.addPartition(i, sa1);
	    pt2.addPartition(i, sa1);
	    pt3.addPartition(i, sa1);
	}

	for (int i = numPartitions/3; i < 2*numPartitions/3; i++) {
	    pt1.addPartition(i, sa2);
	    pt2.addPartition(i, sa2);
	    pt3.addPartition(i, sa2);
	}

	for (int i = 2*numPartitions/3; i < numPartitions; i++) {
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

	// start benchmark
	Bench b = new Bench(client, 0, 100, servers, 0);
	(new Thread(b)).start();
    }
    
}