import java.io.*;
import java.util.*;


/**
 *  This class implements a worker that is spawned every time a transaction starts at a server
 *  Currently uses SS2PL, 2PC
 *  Assuming no network packet failures!
 */

public class Worker implements Runnable {
    
    private Server server;
    private CommunicationQ queue;
    private TransactionContext txn;
    private HashMap<String, String> readSet;
    private HashMap<String, String> writeSet;

    private HashSet<String> readLocked;
    private HashSet<String> writeLocked;

    public Worker(Server server, CommunicationQ queue) {
	this.server = server;
	this.queue = queue;
	txn = null;
	readSet = new HashMap<String, String>();
    }

    public void startTransaction(RPCRequest rpcReq) {
	TransactionContext txnContext = (TransactionContext) rpcReq.getArgs();

	if (txn == null) {
	    txn = txnContext;
	}

	TransactionID tid = txn.tid;

	HashSet<Integer> contactPartitions = new ArrayList<Integer>();

	Iterator write_set_it = txnContext.write_set.keySet().iterator();
	Iterator read_set_it = txnContext.read_set.keySet().iterator();

	while (write_set_it.hasNext()) {
	    String key = (String) write_set_it.next();
	    int serverNum = this.server.hashKey(key);
	    if (serverNum == this.server.getServerNumber()) {
		this.server.lockW(key);
		writeLocked.add(key);
		writeSet.put(key, value);
	    } else {
		contactPartitions.add(new Integer(serverNum));
	    }
	}

	while (read_set_it.hasNext()) {
	    String key = (String) read_set_it.next();
	    int serverNum = this.server.hashKey(key);
	    if (serverNum == this.server.getServerNumber()) {
		this.server.lockR(key);
		String value = this.server.get(key);
		readSet.put(key, value);
		readLocked.add(key);
	    } else {
		contactPartitions.add(new Integer(serverNum));
	    }
	}

	// TODO: this can definitely be optmized
	if (this.server.getServerAddress().equals(tid.getServerAddress())) {
	    // need to make requests to get all locks from necessary servers
	    // go ahead and sends txnContext to all 
	    
	    Iterator it = contactPartitions.iterator();

	    while (it.hasNext()) {
		ServerAddress sa = this.server.getServerAddress((int) it.next());
		HashMap<String, Object> args = txnContext.toJSONObject();

		ServerAddress thisSA = this.server.getAddress();
		args.put("Reply Port", thisSA.getServerPort());
		args.put("Reply Name", thisSA.getServerName());
		args.put("Reply Number", thisSA.getServerNumber());
		
		// TODO: what if packets are dropped?
		RPC.send(sa.getServerName(), sa.getServerPort(), "start", "001", args);
	    }

	} else {
	    // reply to original server with read-set information
	    HashMap<String, Object> args = new HashMap<String, Object>();
	    args.put("Reply Port", thisSA.getServerPort());
	    args.put("Reply Name", thisSA.getServerName());
	    args.put("Reply Number", thisSA.getServerNumber());
	    args.put("State", "OK");
	    args.put("Read Set", readSet);
	    args.put("TID", tid.getTID());

	    RPC.send(rpcReq.replyAddress.getName(), rpcReq.replyAddress.getPort(), "start-reply", "001", args);
	    readSet.clear();
	}
	
    }

    // TODO: write to log?
    public void abort(TransactionContext txnContext) {
	// abort the transaction, release all locks held by the txn
	Iterator it1 = writeLocked.iterator();
	while (it1.hasNext()) {
	    this.server.unlockW((String) it1.next());
	}

	Iterator it2 = readLocked.iterator();
	while (it2.hasNext()) {
	    this.server.unlockR((String) it2.next());
	}
	// TODO: reply to replyAddress, exit
    }

    // TODO: write to log?
    public void commit(TransactionContext txnContext) {
	// commit the transaction, release all locks held by the txn
	// write everything from write set to data store
	Iterator it = writeSet.entrySet().iterator();
	while (it.hasNext()) {
	    Map.Entry kv = (Map.Entry) it.next();
	    this.server.put((String) kv.getKey(), (String) kv.getValue());
	}

	// release all locks
	Iterator it1 = writeLocked.iterator();
	while (it1.hasNext()) {
	    this.server.unlockW((String) it1.next());
	}

	Iterator it2 = readLocked.iterator();
	while (it2.hasNext()) {
	    this.server.unlockR((String) it2.next());
	}
	// TODO: return read set to original server, exit thread
	// OR: reply to replyAddress, exit
    }

    public void receive(RPCRequest req) {
	// received reads from another machine, update readSet
	HashMap<String, String> rcvdSet = req.getArgs();
	Iterator it = rcvdSet.entrySet().iterator();
	while (it.hasNext()) {
	    Map.Entry kv = (Map.Entry) it.next();
	    this.readSet.put((String) kv.getKey(), (String) kv.getValue());
	}
	
    }

    public void run() {

	while (true) {
	    Object obj = queue.get();
	    
	    if (obj.equals("")) {
		Thread.sleep(0.5);
	    }
	    
	    RPCRequest rpcReq = (rpcReq) obj;

	    if (rpcReq.getArgs() instanceof TransactionContext) {
		this.startTransaction(rpcReq);
	    } else if (rpcReq.getArgs() instanceof Abort) {
		this.abort();
	    } else if (rpcReq.getArgs() instanceof Commit) {
		// TODO: should there be a state for "if ready, then commit?"
		this.commit();
	    } else {
		// this has to be "start-reply"
		this.receive(rpcReq);
	    }

	}
	
    }

}