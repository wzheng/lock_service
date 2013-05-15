package main;
import java.util.*;
import java.io.*;

import com.thetransactioncompany.jsonrpc2.*;

/**
 * MainApp is responsible for all of the initialization for this database
 */

public class MainApp {

    public static void main(String[] args) {

	// Start servers
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

    }

}