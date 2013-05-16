package benchmarks;

import java.io.*;
import java.util.*;

import main.*;


public class Bench implements Runnable {

    private ServerAddress client;
    private int tid;
    private int keyLower;
    private int keyHigher;
    private ArrayList<ServerAddress> servers;
    private ServerAddress contact;
    private RPC rpc;
    private int master;

    public Bench(ServerAddress client, int keyLower, int keyHigher, ArrayList<ServerAddress> servers, int master) {
	this.client = client;
	tid = 0;
	this.keyLower = keyLower;
	this.keyHigher = keyHigher;
	this.servers = servers;
	contact = null;
	rpc = new RPC(client);
	this.master = master;
    }

    public void startMaster() {
	ServerAddress masterSA = servers.get(master);
	HashMap<String, Object> args = new HashMap<String, Object>();
	args.put("Method", "start-configuration");
	RPCRequest req = new RPCRequest("reconfigure", client, new TransactionId(masterSA, -1), args);
	RPC.send(masterSA, "reconfigure", "001", req.toJSONObject());

	try {
	    Thread.sleep(1000);
	} catch (InterruptedException e) {

	}
    }
    
    public void load() {
	// just load through one server
	
	contact = servers.get(Util.randNum(0, servers.size() - 1));
	
	HashMap<String, String> w = new HashMap<String, String>();
	HashMap<String, String> r = new HashMap<String, String>();

	HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	HashMap<String, HashMap<String, String> > reads = new HashMap<String, HashMap<String, String> >();

	writes.put("table", w);
	reads.put("table", r);
	
	for (int i = keyLower; i <= keyHigher; i++) {
	    String key = Integer.toString(i);
	    String value = Integer.toString(i);
	    w.put(key, value);
	    PartitionTest.startTxn(contact, client, tid, writes, reads, rpc);
	    PartitionTest.commit(contact, client, tid, rpc);
	    tid++;
	    w.clear();
	}
    }

    public void execute(int iteration) {
	int i = iteration;

	while (i > 0) {

	    tid++;

	    HashMap<String, String> w = new HashMap<String, String>();
	    HashMap<String, String> r = new HashMap<String, String>();
	    
	    HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	    HashMap<String, HashMap<String, String> > reads = new HashMap<String, HashMap<String, String> >();
	    
	    writes.put("table", w);
	    writes.put("table", r);
	    
	    int readRec = Util.randNum(5, 10);
	    int writeRec = Util.randNum(5, 10);
	    
	    // read 5 - 10 records
	    while (readRec > 0) {
		int rand = Util.randNum(keyLower, keyHigher);
		String key = Integer.toString(rand);
		if (r.containsKey(key)) {
		    continue;
		}
		r.put(key, "hello");
		readRec--;
	    }
	    
	    // update 5 - 10 records
	    while (writeRec > 0) {
		int rand = Util.randNum(keyLower, keyHigher);
		String key = Integer.toString(rand);
		if (w.containsKey(key) || r.containsKey(key)) {
		    continue;
		}
		w.put(key, "hello");
		writeRec--;
	    }

	    //System.out.println("start txn");

	    ServerAddress contact = servers.get(Util.randNum(0, servers.size() - 1));

	    //System.out.println(tid + " tries to start");
	    
	    while (!PartitionTest.startTxn(contact, client, tid, writes, reads, rpc)) {
		try {
		    Thread.sleep(50);
		} catch (InterruptedException e) {

		}
	    }

	    w.clear();
	    r.clear();

	    //System.out.println(tid + " tries to commit");

	    PartitionTest.commit(contact, client, tid, rpc);

	    //System.out.println(tid + " committed");
	    i--;
	}
    }

    public void run() {

	int numTxn = 500;

	System.out.println("Loading");
	// load data
	this.load();

	System.out.println("Start reconfiguration");
	// tell master to start gather reconfiguration info
	this.startMaster();

	System.out.println("Run benchmark");
	// run benchmark
	long startTime = System.nanoTime();
	this.execute(numTxn);
	long endTime = System.nanoTime();
	long duration = endTime - startTime;

	System.out.println("Transactions per second: " + ((double) numTxn/ (duration/(Math.pow(10, 9)))) );
	System.exit(0);
    }
}