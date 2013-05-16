package main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.TimerTask;

import net.minidev.json.JSONObject;

    public class DeadlockWorker implements Runnable {
        private Server server;
        private CommunicationQ queue;
        private TransactionContext txn;
        private Worker worker;
        
    	CMHProcessor cmhProcessor;
    	//Thread cmhThread;

        // Table: { key value pairs }
        private HashMap<String, HashMap<String, String> > readSet;
        // Partition: {Table: { key value pairs } }
        private HashMap<Integer, HashMap<String, HashMap<String, String> > > writeSet;
        private TransactionId txnId;

        // local locks held
        // <table, key>
        //private HashMap<String, HashSet<String> > readLocked;
        //private HashMap<String, HashSet<String> > writeLocked;

        // for coordinator
        //private HashSet<ServerAddress> cohorts;

        // thread state
        private boolean done;

        // for reconfiguration
        //private ArrayList<Integer> partitions;
        
        //private HashSet<TransactionId> wfg;

        public DeadlockWorker(Server server, CommunicationQ queue, Worker worker) {
            this.server = server;
            this.queue = queue;
            this.worker = worker;
        	this.cmhProcessor = new CMHProcessor(server);
    	//this.writeLocked = new HashMap<String, HashSet<String> >();
    	//this.readLocked = new HashMap<String, HashSet<String> >();
    	//this.cohorts = new HashSet<ServerAddress>();
    	
    	//this.readSet = new HashMap<String, HashMap<String, String> >();
    	//this.writeSet = new HashMap<Integer, HashMap<String, HashMap<String, String> > >();
    	
    	//this.partitions = new ArrayList<Integer>();

        }
    	/*
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
    	boolean deadlocked;
    	RPCRequest rpcReq;

    	public DeadlockWorker(Server server, String table, String key, int partNum,
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
    		deadlocked = false;
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
    	*/

		@Override
    	public void run(){
	        while (!done) {
	            Object obj = queue.get();

	            if (obj.equals("")) {
	            	continue;
	            }
	            RPCRequest rpcReq = (RPCRequest) obj;
	    	    //System.out.println("rpcReq: " + rpcReq.method);

                if (rpcReq.method.equals("deadlock")){
                	//System.out.println("deadlock worker got message");
                	this.processcmhMessage(rpcReq);
                }
			// periodically lock it and allow the thread to return
			// so we can get deadlock detection
			
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
		    //cancel();
	        }
    	}
	    /**
	     * Called when a Chandy-Misra-Haas message is received. Determines if a deadlock
	     * exists or if not, sends messages to other resources it's waiting for.
	     * @param req
	     */
	    public void cmhDeadlockReceiveMessage(RPCRequest req){
	    	//CMHProcessor cmhProcessor = new CMHProcessor(server);
	    	HashMap<String, Object> args = (HashMap<String, Object>) req.args;
	    	int initiator = ((Long) args.get("initiator")).intValue();
	    	TransactionId initiatorTid = cmhProcessor.genTid("initiator", args);
	    	int to = ((Long) args.get("to")).intValue();
	    	TransactionId toTid = cmhProcessor.genTid("to", args);
	    	int from = ((Long) args.get("from")).intValue();
	    	TransactionId fromTid = cmhProcessor.genTid("from", args);
//	    	/System.out.println("cmh message received initiator: " + initiator + " from: " + from + " to: " + to);
	    	if (initiator == to ){
	    		//(server.getWFG(req.tid) != null && server.getWFG(req.tid).contains(new TransactionId(server.getAddress(), to)))
	    		System.out.println("Deadlock detected by messages");
	    		worker.isDeadlocked = true;
	    	} else {
	    		// continue to send messages
	    		//System.out.println("propaganda: " + initiatorTid.getTID() + ", " + toTid.getTID() + server.getWFG(toTid));
	    		
	    		//cmhProcessor.propagateMessage(initiatorTid, req.tid, server.getWFG(req.tid));
	    		if (server.getWFG(toTid) == null || server.getWFG(toTid).size() < 1){
	    			HashSet<TransactionId> s = new HashSet<TransactionId>();
	    			s.add(fromTid);
	    			cmhProcessor.propagateMessage(initiatorTid, toTid, s);
	    		}
	    		else{
	    			cmhProcessor.propagateMessage(initiatorTid, toTid, server.getWFG(toTid));
	    		}
	    	}
	    }
	    
	    public void processcmhMessage(RPCRequest rpcReq){
	    	HashMap<String, Object> args = (HashMap<String, Object>) rpcReq.args;
	    	int initiator = ((Long) args.get("initiator")).intValue();
	    	TransactionId initiatorTid = cmhProcessor.genTid("initiator", args);
	    	int to = ((Long) args.get("to")).intValue();
	    	TransactionId toTid = cmhProcessor.genTid("to", args);
	    	int from = ((Long) args.get("from")).intValue();
	    	TransactionId fromTid = cmhProcessor.genTid("from", args);
	    	//System.out.println("cmh message received initiator: " + initiator + " from: " + from + " to: " + to);
	        if (this.server.getAddress().equals(toTid.getServerAddress())) {
	        	cmhDeadlockReceiveMessage(rpcReq);
	        	
	        }
	    }
    }