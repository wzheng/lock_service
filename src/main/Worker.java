package main;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import com.thetransactioncompany.jsonrpc2.JSONRPC2Request;


/**
 * This class implements a worker that is spawned every time a transaction
 * starts at a server Currently uses SS2PL, 2PC Assuming no network packet
 * failures!
 */

public class Worker implements Runnable {

    private Server server;
    private CommunicationQ queue;
    private TransactionContext txn;
    
	CMHProcessor cmhProcessor;
	Thread cmhThread;

    // Table: { key value pairs }
    private HashMap<String, HashMap<String, String> > readSet;
    // Partition: {Table: { key value pairs } }
    private HashMap<Integer, HashMap<String, HashMap<String, String> > > writeSet;
    private TransactionId txnId;

    // local locks held
    // <table, key>
    private HashMap<String, HashSet<String> > readLocked;
    private HashMap<String, HashSet<String> > writeLocked;

    // for coordinator
    private HashSet<ServerAddress> cohorts;

    // thread state
    private boolean done;

    // for reconfiguration
    private ArrayList<Integer> partitions;
    
    //private HashSet<TransactionId> wfg;
    //private int wfgCounter = 0;

    private WFGWorker wfgWorker;
    
    public boolean isDeadlocked;
    public Worker(Server server, CommunicationQ queue) {
        this.server = server;
        this.queue = queue;
        txn = null;
        cohorts = new HashSet<ServerAddress>();
        done = false;
        //wfg = new HashSet<TransactionId>();
        wfgWorker = null;
        isDeadlocked = false;

	this.writeLocked = new HashMap<String, HashSet<String> >();
	this.readLocked = new HashMap<String, HashSet<String> >();
	this.cohorts = new HashSet<ServerAddress>();
	
	this.readSet = new HashMap<String, HashMap<String, String> >();
	this.writeSet = new HashMap<Integer, HashMap<String, HashMap<String, String> > >();
	
	this.partitions = new ArrayList<Integer>();
	this.cmhProcessor = new CMHProcessor(server);
	cmhThread = new Thread(this.cmhProcessor);
	cmhThread.start();
    }

    public void setWFGWorker(WFGWorker w){
    	this.wfgWorker = w;
    }
    public void getSingleLock(RPCRequest rpcReq){
    	HashMap<String, Object> args = (HashMap<String, Object>) rpcReq.args;
    	TransactionContext txnContext = new TransactionContext(rpcReq.tid, (HashMap<String, Object>) rpcReq.args);
    	//String key = (String) args.get("write-lock");
    	System.out.println("called getSingleLock via RPC");
    	String key = (String) txnContext.write_set.keySet().toArray()[0];
    	lockOneFromWriteSet(key, rpcReq.tid, txnContext);
    }
    
    public void lockOneFromWriteSet(String key, TransactionId tid, TransactionContext txnContext){
    	// //System.out.println("called lockOneFromWriteSet for key " + key + " and transaction " + tid.getTID());
        // int partNum = this.server.hashKey(key);
        // ServerAddress sendSA = this.server.getPartitionTable().getServer(partNum);

        // // if coordinator
        // if (this.server.getAddress().equals(tid.getServerAddress())) {
	//     partitions.add(new Integer(partNum));
	//     if (!sendSA.equals(this.server.getAddress())) {
	// 	cohorts.add(sendSA);
	//     }
        // }

        // if (sendSA.equals(this.server.getAddress())) {
        //     this.server.lockW(key, tid);
        //     writeLocked.add(key);
	//     Integer part = new Integer(partNum);
	//     HashMap<String, String> temp = writeSet.get(part);
	//     if (temp == null) {
	// 	temp = new HashMap<String, String>();
	//     }
	//     temp.put(key, (String) txnContext.write_set.get(key));
	//     writeSet.put(part, temp);
        // }
    	
    }
    
    private class LockTask extends TimerTask {
    	private Server server;
    	private String table;
    	private String key;
    	private TransactionId tid;
    	private int partNum;
    	private TransactionContext txnContext;
    	private boolean locked;
    	private HashMap<String, HashSet<String>> writeLocked;
    	private CMHProcessor cmhProcessor;
    	private HashSet<TransactionId> wfg;
    	private boolean deadlocked;
    	RPCRequest rpcReq;

    	public LockTask(Server server, String table, String key, int partNum,
    			TransactionId tid, TransactionContext txnContext,
    			HashMap<String, HashSet<String>> writeLocked,
    			CMHProcessor cmhProcessor, RPCRequest rpcReq) {
			// TODO Auto-generated constructor stub
    		this.server = server;
    		this.table = table;
    		this.key = key;
    		this.partNum = partNum;
    		this.tid = tid;
    		this.txnContext = txnContext;
    		this.writeLocked = writeLocked;
    		this.cmhProcessor = cmhProcessor;
    		this.wfg = null;
    		locked = false;
    		this.deadlocked = false;
    		this.rpcReq = rpcReq;
		}
    	public boolean isLocked(){
    		return locked;
    	}
    	
    	public HashSet<TransactionId> getWFG(){
    		return wfg;
    	}
    	public boolean deadlocked(){
    		return deadlocked;
    	}

		@Override
    	public void run(){

			// periodically lock it and allow the thread to return
			// so we can get deadlock detection
		    locked = this.server.lockW(key + table, tid);
		    wfg = this.server.getWFG(tid);
	        boolean flag = true;
	        long startMillis = System.currentTimeMillis();
	        int attempts = 0;
	        boolean checkingForLock = true;
	        while (checkingForLock){
	        	try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        if (isLocked()){
		        	System.out.println("LOCK ACQUIRED");
		        	//cancel();
		        	break;
		        }
		        else{
		        	System.out.println("NOT ACQUIRED");
	        		if (System.currentTimeMillis() - startMillis > 500){
	        			checkingForLock = false;
		                HashMap<String, Object> args = new HashMap<String, Object>();
		                RPCRequest newReq = new RPCRequest("abort", server.getAddress(), rpcReq.tid, args);
		                RPC.send(rpcReq.replyAddress, "abort", "001", newReq.toJSONObject());
		                System.out.println("Deadlock detected, abort sent");
		                deadlocked = true;
		                cancel();
		                break;
	        		}
		        	/*
		        	if  (startMillis - System.currentTimeMillis() > 100){
		        		System.out.println("Deadlock detected");
		        		cancel();
		        	}
		        	*/
		        	/*
		        	if (attempts++ > 5){
		        		System.out.println("Deadlock");
		        		deadlocked = true;
		        		cancel();
		                HashMap<String, Object> args = new HashMap<String, Object>();
		                RPCRequest newReq = new RPCRequest("abort", server.getAddress(), rpcReq.tid, args);
		                RPC.send(rpcReq.replyAddress, "abort", "001", newReq.toJSONObject());
		        		break;
		        	}
		        	*/
		        	if (flag){
		        		HashSet<TransactionId> wf = server.getWFG(tid);
		                DeadlockTest.print("WFG for TID " + tid.getTID());
		                String s = "";
		                for (TransactionId x : wf){
		                	s += x.getTID() + ", ";
		                	/*
		                	if (server.getWFG(x).contains(tid)){
		                		System.out.println("DEADLOCK");
		                	}
		                	*/
		                	//getGlobalWFG(x);
		                	if (wfg.contains(tid)){
		                		System.out.println("DEADLOCK");
		                	}
		                }
		                DeadlockTest.print(s);
		        		cmhProcessor.generateMessage(tid, wf);
		        		flag = true;
		        		//break;
		        	}
		        	
		        }
	        }
		    //this.server.lockW(key + table, tid);
		    //System.out.println("lockW " + key + table);
//		    HashSet<String> set = writeLocked.get(table);
//		    if (set == null) {
//		    	set = new HashSet<String>();
//		    }
//		    set.add(key);
//		    writeLocked.put(table, set);
//		    Integer part = new Integer(partNum);
//		    HashMap<String, HashMap<String, String> > temp = writeSet.get(part);
//		    if (temp == null) {
//		    	temp = new HashMap<String, HashMap<String, String> >();
//		    	temp.put(table, new HashMap<String, String>());
//		    }
//		    HashMap<String, String> kv = temp.get(table);
//		    kv.put(key, (String) txnContext.write_set.get(table).get(key));
//		    temp.put(table, kv);
//		    writeSet.put(part, temp);
		    cancel();
    	}
    }
    
    public void setDeadlocked(boolean v){
    	isDeadlocked = v;
    }
    

    public void startTransaction(RPCRequest rpcReq) {
        TransactionContext txnContext = new TransactionContext(rpcReq.tid, (HashMap<String, Object>) rpcReq.args);

	//System.out.println("Start transaction " + rpcReq.tid.getTID());
	txnId = rpcReq.tid;

        if (txn == null) {
            txn = txnContext;
        }

        TransactionId tid = rpcReq.tid;

        Iterator<String> write_set_it = txnContext.write_set.keySet().iterator();
        Iterator<String> read_set_it = txnContext.read_set.keySet().iterator();

        while (write_set_it.hasNext()) {
            String table = write_set_it.next();
	    HashMap<String, String> kvSets = txnContext.write_set.get(table);
	    Iterator writekv_it = kvSets.entrySet().iterator();
	    while (writekv_it.hasNext()) {
		Map.Entry entry = (Map.Entry) writekv_it.next();
		String key = (String) entry.getKey();
		int partNum = this.server.hashKey(key);
		ServerAddress sendSA = this.server.getPartitionTable().getServer(partNum);
		
		// if coordinator
		if (this.server.getAddress().equals(tid.getServerAddress())) {
		    partitions.add(new Integer(partNum));
		    if (!sendSA.equals(this.server.getAddress())) {
			cohorts.add(sendSA);
		    }
		}
		
		//Timer timer = new Timer();  //At this line a new Thread will be created
        //timer.schedule(new LockTask(this.server, key + table, tid), 0, 100); //delay in milliseconds
		if (sendSA.equals(this.server.getAddress())) {
			long start = System.currentTimeMillis();
			while(!this.server.lockW(key+table, tid)){
				if (System.currentTimeMillis() - start > 300){
					break;
				}
				System.out.println("not locking " + key+table + " for tid " + tid.getTID());
				//wfgWorker.getGlobalWFG(tid);
				if (server.getWFG(tid) != null){
				cmhProcessor.generateMessage(tid, server.getWFG(tid));
				}
				if (isDeadlocked){
					break;
				}
				
			}
			if (isDeadlocked){
				System.out.println("should abort now");
                HashMap<String, Object> args = new HashMap<String, Object>();
                RPCRequest newReq = new RPCRequest("abort", server.getAddress(), rpcReq.tid, args);
                RPC.send(server.getAddress(), "abort", "001", newReq.toJSONObject());
                return;
                
			}
			/*
			Timer timer = new Timer();  //At this line a new Thread will be created
			LockTask task = new LockTask(this.server, key, table, partNum, tid, txnContext, writeLocked, cmhProcessor, rpcReq);
	        timer.schedule(task, 0, 10); //delay in milliseconds
			
	        

			// periodically lock it and allow the thread to return
			// so we can get deadlock detection
	        
	        // wait 1 second to see if deadlock happened
			
	        try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
	        if (!task.isLocked()){
	        	return;
	        }
	        */
			

	        
	        //if (){
                //HashMap<String, Object> args = new HashMap<String, Object>();
                //RPCRequest newReq = new RPCRequest("abort", server.getAddress(), rpcReq.tid, args);
                //RPC.send(rpcReq.replyAddress, "abort", "001", newReq.toJSONObject());
                //return;
	        //}
	        	
		    System.out.println("lockW " + key + table + " by TID " + tid.getTID());
		    HashSet<String> set = writeLocked.get(table);
		    if (set == null) {
			set = new HashSet<String>();
		    }
		    set.add(key);
		    writeLocked.put(table, set);
		    Integer part = new Integer(partNum);
		    HashMap<String, HashMap<String, String> > temp = writeSet.get(part);
		    if (temp == null) {
			temp = new HashMap<String, HashMap<String, String> >();
			temp.put(table, new HashMap<String, String>());
		    }
		    HashMap<String, String> kv = temp.get(table);
		    kv.put(key, (String) txnContext.write_set.get(table).get(key));
		    temp.put(table, kv);
		    writeSet.put(part, temp);
		    
		}
	    }
	        
        }

        while (read_set_it.hasNext()) {
            String table = (String) read_set_it.next();
	    HashMap<String, String> kvSets = txnContext.read_set.get(table);
	    Iterator kvSets_it = kvSets.entrySet().iterator();
	    while (kvSets_it.hasNext()) {
		Map.Entry entry = (Map.Entry) kvSets_it.next();
		String key = (String) entry.getKey();
		int partNum = this.server.hashKey(key);
		ServerAddress sendSA = this.server.getPartitionTable().getServer(partNum);
		
		// if coordinator
		if (this.server.getAddress().equals(tid.getServerAddress())) {
		    partitions.add(new Integer(partNum));
		    if (!sendSA.equals(this.server.getAddress())) {
			cohorts.add(sendSA);
		    }
		}
		
		if (sendSA.equals(this.server.getAddress())) {
		    this.server.lockR(key+table, tid);
		    //System.out.println("lockR " + key + table);
		    String value = this.server.get(partNum, table, key);
		    HashMap<String, String> kv = readSet.get(table);
		    if (kv == null) {
			kv = new HashMap<String, String>();
		    }
		    if (value != null) {
			kv.put(key, value);
		    } else {
			kv.put(key, "");
		    }
		    readSet.put(table, kv);
		    HashSet<String> set = readLocked.get(table);
		    if (set == null) {
			set = new HashSet<String>();
		    }
		    set.add(key);
		    readLocked.put(table, set);
		}
	    }
        }

        // TODO: this can definitely be optimized
        if (this.server.getAddress().equals(tid.getServerAddress())) {
            // need to make requests to get all locks from necessary servers
            // go ahead and sends txnContext to all

            Iterator<ServerAddress> it = cohorts.iterator();
            HashSet<ServerAddress> waitServers = new HashSet<ServerAddress>();
            ServerAddress thisSA = this.server.getAddress();

            while (it.hasNext()) {
                ServerAddress sa = (ServerAddress) it.next();
                HashMap<String, Object> args = txnContext.toJSONObject();
		args.put("Partition Version", new Integer(this.server.getPartitionTable().version));
                waitServers.add(sa);

                // TODO: what if packets are dropped?
                RPCRequest newReq = new RPCRequest("start", thisSA, tid, args);
                RPC.send(sa, "start", "001", newReq.toJSONObject());
            }

	    // receive replies from all of the cohorts, reply to client

	    HashSet<ServerAddress> receivedSA = new HashSet<ServerAddress>();

            while (!waitServers.isEmpty()) {
                Object obj = queue.get();

                if (obj.equals("")) {
		    //System.out.println("Stuck");
		    try {
			Thread.sleep(100);
		    } catch (InterruptedException e) {
		    }
		    continue;
                }

                RPCRequest receivedReq = (RPCRequest) obj;
		if (receivedReq.method.equals("abort-reply")) {
		    //System.out.println("Received abort in startTxn for tid " + rpcReq.tid.getTID());		    
		    this.cohorts.remove(receivedReq.replyAddress);
		    this.cohorts = receivedSA;
		    this.abort(rpcReq);
		    return ;
		} else if (receivedReq.method.equals("start-reply")) {
		    HashMap<String, Object> receivedArgs = (HashMap<String, Object>) receivedReq.args;
		    HashMap<String, HashMap<String, String> > rset = (HashMap<String, HashMap<String, String> >) receivedArgs.get("Read Set");
		    //System.out.println(receivedReq.method + " -> Read set is " + rset);
		    Iterator rit = rset.entrySet().iterator();
		    while (rit.hasNext()) {
			Map.Entry kv = (Map.Entry) rit.next();
			String table = (String) kv.getKey();
			HashMap<String, String> kvPairs = (HashMap<String, String>) kv.getValue();
			Iterator kvPairs_it = kvPairs.entrySet().iterator();
			while (kvPairs_it.hasNext()) {
			    Map.Entry<String, String> kv_pair = (Map.Entry<String, String>) kvPairs_it.next();
			    if (readSet.get(table) == null) {
				readSet.put(table, new HashMap<String, String>());
			    }
			    HashMap<String, String> map = readSet.get(table);
			    map.put(kv_pair.getKey(), kv_pair.getValue());
			    readSet.put(table, map);
			}
		    }
		    receivedSA.add(receivedReq.replyAddress);
		    waitServers.remove(receivedReq.replyAddress);
		} else {
		    queue.put(obj);
		}
            }

	    // reply to client
	    HashMap<String, Object> args = new HashMap<String, Object>();
	    args.put("State", true);
            RPCRequest newReq = new RPCRequest("start-done", thisSA, tid, args);
	    RPC.send(rpcReq.replyAddress, "start-done", "001", newReq.toJSONObject());
	    
        } else {

            // reply to original server with read-set information
	    Long version = (Long) ((HashMap<String, Object>) rpcReq.args).get("Partition Version");
	    if (version.intValue() != this.server.getPartitionTable().version) {

                HashMap<String, Object> args = new HashMap<String, Object>();
                RPCRequest newReq = new RPCRequest("abort-reply", this.server.getAddress(), rpcReq.tid, args);
                RPC.send(rpcReq.replyAddress, "abort-reply", "001", newReq.toJSONObject());
		
	    } else {

		ServerAddress thisSA = this.server.getAddress();
		HashMap<String, Object> args = new HashMap<String, Object>();
		args.put("State", "OK");
		args.put("Read Set", readSet);
		RPCRequest newReq = new RPCRequest("start-reply", thisSA, tid, args);

		RPC.send(rpcReq.replyAddress, "start-reply", "001", newReq.toJSONObject());
		readSet.clear();

	    }
        }

    }

    // TODO: write to log?
    public void abort(RPCRequest rpcReq) {
        // abort the transaction, release all locks held by the txn
    	//System.out.println("ABORTING");
        Iterator it1 = writeLocked.entrySet().iterator();
        while (it1.hasNext()) {
	    Map.Entry entry = (Map.Entry) it1.next();
	    String table = (String) entry.getKey();
	    HashSet<String> keys = (HashSet<String>) entry.getValue();
	    Iterator keys_it = keys.iterator();
	    while (keys_it.hasNext()) {
		String n = (String) keys_it.next();
		//System.out.println("unlockW " + n + table);
		this.server.unlockW(n + table, rpcReq.tid);
	    }
        }

        Iterator it2 = readLocked.entrySet().iterator();
        while (it2.hasNext()) {
	    Map.Entry entry = (Map.Entry) it2.next();
	    String table = (String) entry.getKey();
	    HashSet<String> keys = (HashSet<String>) entry.getValue();
	    Iterator keys_it = keys.iterator();
	    while (keys_it.hasNext()) {
		String n = (String) keys_it.next();
		//System.out.println("unlockR " + n + table);
		this.server.unlockR(n + table, rpcReq.tid);
	    }	    
        }

        // TODO: reply to replyAddress, exit
        if (this.server.getAddress().equals(rpcReq.tid.getServerAddress())) {
            // sends "abort" to all servers
            HashSet<ServerAddress> waitServers = new HashSet<ServerAddress>();
	    ServerAddress thisSA = this.server.getAddress();

	    //System.out.println("Server " + thisSA + " cohorts: " + cohorts);

            Iterator<ServerAddress> it = cohorts.iterator();
            while (it.hasNext()) {

                HashMap<String, Object> args = new HashMap<String, Object>();
                RPCRequest newReq = new RPCRequest("abort", thisSA, rpcReq.tid, args);

                ServerAddress sentServer = (ServerAddress) it.next();
                RPC.send(sentServer, "abort", "001", newReq.toJSONObject());
                waitServers.add(sentServer);
            }

            while (!waitServers.isEmpty()) {
                Object obj = queue.get();

                if (obj.equals("")) {
                    //Thread.sleep(50);
		    continue;
                }

                RPCRequest req = (RPCRequest) obj;
		if (req.method.equals("abort-reply")) {
		    //System.out.println("Tid " + rpcReq.tid.getTID() + " abort received");
		    waitServers.remove(req.replyAddress);
		} else {
		    queue.put(obj);
		}
            }

	    // reply to client
	    HashMap<String, Object> args = new HashMap<String, Object>();
	    args.put("State", true);
            RPCRequest newReq = new RPCRequest("abort-done", thisSA, rpcReq.tid, args);
	    RPC.send(rpcReq.replyAddress, "abort-done", "001", newReq.toJSONObject());
	    this.done = true;

        } else {
            // sends "ack" back to original server
            HashMap<String, Object> args = new HashMap<String, Object>();
            ServerAddress thisSA = this.server.getAddress();
            args.put("State", true);

	    //System.out.println("Server " + thisSA + " abort received for tid " + rpcReq.tid.getTID());

            RPCRequest newReq = new RPCRequest("abort-reply", thisSA, rpcReq.tid, args);
            RPC.send(rpcReq.replyAddress, "abort-reply", "001", newReq.toJSONObject());
	    this.done = true;
        }
    }

    // TODO: in the future, when there are machine failures/other failures, should
    // have the option to reply "abort" instead of "commit-prepare-done"
    public void commitPrepare(RPCRequest rpcReq) {
	ServerAddress thisSA = this.server.getAddress();
	HashMap<String, Object> args = new HashMap<String, Object>();
	RPCRequest newReq = new RPCRequest("commit-prepare-done", thisSA, rpcReq.tid, args);
	RPC.send(rpcReq.replyAddress, "commit-prepare-done", "001", newReq.toJSONObject());
    }

    // TODO: write to log?
    public void commit(RPCRequest rpcReq) {
	//System.out.println("Ready to commit");
	
        // commit the transaction, release all locks held by the txn
        // write everything from write set to data store
        if (this.server.getAddress().equals(rpcReq.tid.getServerAddress())) {
            // sends "commit-prepare" to all servers
	    ServerAddress thisSA = this.server.getAddress();

	    // Phase 1 - commit prepare
            HashSet<ServerAddress> waitServers = new HashSet<ServerAddress>();
            Iterator<ServerAddress> c_it = cohorts.iterator();
            while (c_it.hasNext()) {

                HashMap<String, Object> args = new HashMap<String, Object>();
                RPCRequest newReq = new RPCRequest("commit-prepare", thisSA, rpcReq.tid, args);

                ServerAddress sentServer = (ServerAddress) c_it.next();
                RPC.send(sentServer, "commit-prepare", "001", newReq.toJSONObject());
                waitServers.add(sentServer);
            }

            while (!waitServers.isEmpty()) {
                Object obj = queue.get();

                if (obj.equals("")) {
                    //Thread.sleep(50);
		    continue;
                }

                RPCRequest req = (RPCRequest) obj;
		if (req.method.equals("abort-reply")) {
		    this.abort(rpcReq);
		    return ;
		} else if (req.method.equals("commit-prepare-done")) {
		    waitServers.remove(req.replyAddress);
		} else {
		    queue.put(obj);
		}
            }

	    // If it has reached this point, the transaction has to commit
	    // Phase 2 - actual commit

	    Iterator it = writeSet.entrySet().iterator();
	    while (it.hasNext()) {
		Map.Entry kv = (Map.Entry) it.next();
		Integer partition = (Integer) kv.getKey();
		HashMap<String, HashMap<String, String> > partitionData = (HashMap<String, HashMap<String, String> >) kv.getValue();
		Iterator map_it = partitionData.entrySet().iterator();
		while (map_it.hasNext()) {
		    Map.Entry pair = (Map.Entry) map_it.next();
		    String table = (String) pair.getKey();
		    HashMap<String, String> key_values = (HashMap<String, String>) pair.getValue();
		    Iterator key_values_it = key_values.entrySet().iterator();
		    while (key_values_it.hasNext()) {
			Map.Entry kvpair = (Map.Entry) key_values_it.next();
			this.server.put(partition, table, (String) kvpair.getKey(), (String) kvpair.getValue());
		    }
		}
	    }

	    // release all locks
	    Iterator it1 = writeLocked.entrySet().iterator();
	    while (it1.hasNext()) {
		Map.Entry entry = (Map.Entry) it1.next();
		String table = (String) entry.getKey();
		HashSet<String> keys = (HashSet<String>) entry.getValue();
		Iterator keys_it = keys.iterator();
		while (keys_it.hasNext()) {
 		    String n = (String) keys_it.next();
		    //System.out.println("unlockW " + n + table);
		    this.server.unlockW(n + table, rpcReq.tid);
		}
	    }

	    Iterator it2 = readLocked.entrySet().iterator();
	    while (it2.hasNext()) {
		Map.Entry entry = (Map.Entry) it2.next();
		String table = (String) entry.getKey();
		HashSet<String> keys = (HashSet<String>) entry.getValue();
		Iterator keys_it = keys.iterator();
		while (keys_it.hasNext()) {
 		    String n = (String) keys_it.next();
		    //System.out.println("unlockR " + n + table);
		    this.server.unlockR(n + table, rpcReq.tid);
		}
	    }

            c_it = cohorts.iterator();
            while (c_it.hasNext()) {

                HashMap<String, Object> args = new HashMap<String, Object>();
                RPCRequest newReq = new RPCRequest("commit", thisSA, rpcReq.tid, args);

                ServerAddress sentServer = (ServerAddress) c_it.next();
                RPC.send(sentServer, "commit", "001", newReq.toJSONObject());
                waitServers.add(sentServer);
            }

            while (!waitServers.isEmpty()) {
                Object obj = queue.get();

                if (obj.equals("")) {
                    //Thread.sleep(50);
		    continue;
                }

                RPCRequest req = (RPCRequest) obj;
		if (req.method.equals("commit-accept")) {
		    waitServers.remove(req.replyAddress);
		} else {
		    queue.put(obj);
		}
            }

	    // TODO: figure out best way to increment the AF table
	    if (partitions.size() > 1) {
		for (int j = 0; j < partitions.size() - 1; j++) {
		    for (int k = j+1; k < partitions.size(); k++) {
			this.server.getAF().increment(partitions.get(j), partitions.get(k));
		    }
		}
	    }

	    // reply to client
	    HashMap<String, Object> args = new HashMap<String, Object>();
	    args.put("State", true);
	    args.put("Read Set", readSet);
            RPCRequest newReq = new RPCRequest("commit-done", thisSA, rpcReq.tid, args);
	    RPC.send(rpcReq.replyAddress, "commit-done", "001", newReq.toJSONObject());
	    this.done = true;

        } else {

	    Iterator it = writeSet.entrySet().iterator();
	    while (it.hasNext()) {
		Map.Entry kv = (Map.Entry) it.next();
		Integer partition = (Integer) kv.getKey();
		HashMap<String, HashMap<String, String> > partitionData = (HashMap<String, HashMap<String, String> >) kv.getValue();
		Iterator map_it = partitionData.entrySet().iterator();
		while (map_it.hasNext()) {
		    Map.Entry pair = (Map.Entry) map_it.next();
		    String table = (String) pair.getKey();
		    HashMap<String, String> key_values = (HashMap<String, String>) pair.getValue();
		    Iterator key_values_it = key_values.entrySet().iterator();
		    while (key_values_it.hasNext()) {
			Map.Entry kvpair = (Map.Entry) key_values_it.next();
			this.server.put(partition, table, (String) kvpair.getKey(), (String) kvpair.getValue());
		    }
		}
	    }

	    // release all locks
	    Iterator it1 = writeLocked.entrySet().iterator();
	    while (it1.hasNext()) {
		Map.Entry entry = (Map.Entry) it1.next();
		String table = (String) entry.getKey();
		HashSet<String> keys = (HashSet<String>) entry.getValue();
		Iterator keys_it = keys.iterator();
		while (keys_it.hasNext()) {
 		    String n = (String) keys_it.next();
		    //System.out.println("unlockW " + n + table);
		    this.server.unlockW(n + table, rpcReq.tid);
		}
	    }

	    Iterator it2 = readLocked.entrySet().iterator();
	    while (it2.hasNext()) {
		Map.Entry entry = (Map.Entry) it2.next();
		String table = (String) entry.getKey();
		HashSet<String> keys = (HashSet<String>) entry.getValue();
		Iterator keys_it = keys.iterator();
		while (keys_it.hasNext()) {
 		    String n = (String) keys_it.next();
		    //System.out.println("unlockR " + n + table);
		    this.server.unlockR(n + table, rpcReq.tid);
		}	    
	    }

            // sends "ack" back to original server
            HashMap<String, Object> args = new HashMap<String, Object>();
            ServerAddress thisSA = this.server.getAddress();

            args.put("State", true);
	    //args.put("Read Set", read_set);
            RPCRequest newReq = new RPCRequest("commit-accept", thisSA, rpcReq.tid, args);

            RPC.send(rpcReq.replyAddress, "commit-accept", "001", newReq.toJSONObject());
	    this.done = true;
        }
    }
    /*
    public void getThisWFG(RPCRequest req){
    	HashMap<String, Object> args = (HashMap<String, Object>) req.args;
    	ServerAddress initAddr = req.replyAddress;
    	TransactionId tid = req.tid;
        HashMap<String, Object> args2 = new HashMap<String, Object>();
        args.put("wfg", server.getWFG(tid));
        RPCRequest newReq = new RPCRequest("wfg-response", server.getAddress(), tid, args2);
        RPC.send(initAddr, "wfg-response", "001", newReq.toJSONObject());
    }
    
    public void getGlobalWFG(TransactionId tid){
    	wfgCounter = 0;
    	wfg.clear();
    	//HashSet<TransactionId> g = new HashSet<TransactionId>();
    	for (ServerAddress s : server.getAllServers().values()){
    		HashMap<String, Object> args = new HashMap<String, Object>();
    		//args.put("method", "get-wfg");
    		RPCRequest sendReq = new RPCRequest("get-wfg", s, tid, args);
    		RPC.send(s, "get-wfg", "001", sendReq.toJSONObject());
    	}
    }
    
    public void processWFG(RPCRequest rpcReq){
    	if (this.server.getAddress().equals(rpcReq.tid.getServerAddress())) {
    		HashMap<String, Object> args = (HashMap<String, Object>)rpcReq.args;
    		HashSet<TransactionId> t = (HashSet<TransactionId>) args.get("wfg");
    		for (TransactionId tid : t){
    			wfg.add(tid);
    		}
    	}
    	wfgCounter++;
    	if (wfgCounter == server.getAllServers().size()){
        DeadlockTest.print("WHOLEWFG for TID " + rpcReq.tid.getTID());
        String s = "";
        for (TransactionId x : wfg){
        	s += x.getTID() + ", ";
        }
        DeadlockTest.print(s);
		cmhProcessor.generateMessage(rpcReq.tid, wfg);
    	}

    }
    */

    // public void receive(RPCRequest req) {
    //     // received reads from another machine, update readSet
    //     HashMap<String, String> rcvdSet = (HashMap<String, String>) req.args;
    //     Iterator<Entry<String, String>> it = rcvdSet.entrySet().iterator();
    //     while (it.hasNext()) {
    //         Map.Entry<String, String> kv = it.next();
    //         this.readSet.put((String) kv.getKey(), (String) kv.getValue());
    //     }
    // }
    

    /**
     * Called when a Chandy-Misra-Haas message is received. Determines if a deadlock
     * exists or if not, sends messages to other resources it's waiting for.
     * @param req
     */
    public void cmhDeadlockReceiveMessage(RPCRequest req){
    	//CMHProcessor cmhProcessor = new CMHProcessor(server);
    	HashMap<String, Object> args = (HashMap<String, Object>) req.args;
    	TransactionId initiatorTid = (TransactionId) args.get("initiatorTid");
    	int initiator = ((Long) args.get("initiator")).intValue();
    	TransactionId toTid = (TransactionId) args.get("toTid");
    	int to = ((Long) args.get("to")).intValue();
    	TransactionId fromTid = (TransactionId) args.get("fromTid");
    	int from = ((Long) args.get("from")).intValue();
    	System.out.println("cmh message received initiaitor: " + initiator + " from: " + from + " to: " + to);
    	if ((initiator == to && to != from) || (server.getWFG(req.tid) != null && server.getWFG(req.tid).contains(new TransactionId(server.getAddress(), to)))){
    		//System.out.println("Deadlock detected by messages");
    	} else {
    		// continue to send messages
    		cmhProcessor.propagateMessage(initiatorTid, req.tid, server.getWFG(req.tid));
    	}
    }
    
    public void processcmhMessage(RPCRequest rpcReq){
        if (this.server.getAddress().equals(rpcReq.tid.getServerAddress())) {
        	cmhDeadlockReceiveMessage(rpcReq);
        	
        }
    }

    public void run() {

        while (!done) {
            Object obj = queue.get();

            if (obj.equals("")) {
		continue;
            }

            RPCRequest rpcReq = (RPCRequest) obj;
	    //System.out.println("rpcReq: " + rpcReq.method);

            if (rpcReq.method.equals("start")) {
                this.startTransaction(rpcReq);
            } else if (rpcReq.method.equals("abort")) {
                this.abort(rpcReq);
            } else if (rpcReq.method.equals("commit-prepare")) {
                this.commitPrepare(rpcReq);
            } else if (rpcReq.method.equals("commit")) {
            	this.commit(rpcReq);
            } else if (rpcReq.method.equals("deadlock")){
            	this.processcmhMessage(rpcReq);
            } else if (rpcReq.method.equals("single-lock")){
            	this.getSingleLock(rpcReq);
            } //else if (rpcReq.method.equals("get-wfg")){
            	//this.getThisWFG(rpcReq);
            //} else if (rpcReq.method.equals("wfg-response")){
            	//this.processWFG(rpcReq);
            //}
        }

	this.server.threadDone(txnId);
	this.cmhProcessor.stopThread();

    }

}