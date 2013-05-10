import java.util.*;
import java.io.*;

import com.thetransactioncompany.jsonrpc2.*;

/**
 * This simulates a machine that has several partitions of a table
 */

public class Server implements Runnable  {

    private ServerAddress address;
    private LockTable lockTable;
    private HashMap<Integer, PartitionTable.PartitionData> partitionTable;
    private Data dataStore;
    private HashMap<Integer, ServerAddress> serverList;

    // transactions
    private int numThreads;
    private HashMap<TransactionId, CommunicationQ> activeWorkers;

    // reconfiguration
    private boolean isConfiguring;
    private PartitionTable table;
    private HashMap<Integer, Integer> AF; // affinity factors
    private ReconfigState reconfigState; // current state of reconfiguration
    private boolean isMaster;
    private RPC rpc;
    private CommunicationQ queue;

    // static variables
    private static int MAX_THREADS = 2;

    public Server(ServerAddress address, CommunicationQ queue,
		  HashMap<Integer, PartitionTable.PartitionData> config,
		  boolean isMaster, ArrayList<ServerAddress> servers) {

        this.address = address;
        lockTable = new LockTable(address);
        this.partitionTable = config;
        this.dataStore = new Data();
        this.queue = queue;
        this.serverList = new HashMap<Integer, ServerAddress>();

        Iterator servers_it = servers.iterator();
        while (servers_it.hasNext()) {
            ServerAddress sa = (ServerAddress) servers_it.next();
            serverList.put(new Integer(sa.getServerNumber()), sa);
        }
	
	// Iterator<Integer> itr = config.getPartitions();
        // while (itr.hasNext()) {
        //     Integer i = (Integer) itr.next();
        //     dataStore.addNewPartition(i);
        // }

        this.isConfiguring = false;

        numThreads = 0;

        AF = new HashMap<Integer, Integer>();
        reconfigState = ReconfigState.NONE;
        this.isMaster = isMaster;

	this.activeWorkers = new HashMap<TransactionId, CommunicationQ>();
    }

    public PartitionTable getPartitionTable() {
        return table;
    }
    
    public HashSet<TransactionId> getWFG(TransactionId tid){
    	return lockTable.getWFG(tid);
    }

    // For a certain key, returns the partition that key belongs to
    public int hashKey(String key) {
        return (key.hashCode()) % (serverList.size());
    }

    public HashMap<Integer, Integer> getAF() {
        return AF;
    }

    public ReconfigState getReconfigState() {
        return reconfigState;
    }

    public boolean isMaster() {
        return this.isMaster;
    }

    public ServerAddress getAddress() {
        return this.address;
    }

    public ServerAddress getServerAddress(int serverNum) {
        return this.serverList.get(new Integer(serverNum));
    }

    public int getServerNumber() {
        return this.address.getServerNumber();
    }

    public void lockW(String key, TransactionId tid) {
        this.lockTable.lockW(key, tid);
    }

    public void lockR(String key, TransactionId tid) {
        this.lockTable.lockR(key, tid);
    }

    public void unlockW(String key, TransactionId tid) {
        this.lockTable.unlockW(key, tid);
    }

    public void unlockR(String key, TransactionId tid) {
        this.lockTable.unlockR(key, tid);
    }

    public String get(String key) {
        return this.dataStore.get(key);
    }

    public void put(String key, String value) {
        this.dataStore.put(key, value);
    }

    // check for incoming requests, spawn new worker threads as necessary
    public void run() {

        while (true) {

		    Object in = queue.get();
		    if (in.equals("")) {
		    	continue;
		    }

            JSONRPC2Request reqIn = (JSONRPC2Request) in;

            //System.out.println("Server " + address.getServerName() + " received request for " + reqIn.getMethod());

            // TODO: duplicate messages?
	    
            String method = reqIn.getMethod();
            Map<String, Object> params = reqIn.getNamedParams();
            RPCRequest rpcReq = new RPCRequest(method, params);
	    
            if (method.equals("start")) {
		
                CommunicationQ q = new CommunicationQ();
                this.activeWorkers.put(rpcReq.tid, q);
                (new Thread(new Worker(this, q))).start();

                //System.out.println("Started new worker");

                q.put(rpcReq);

            } else {
            	//System.out.println("Putting req " + method + " in queue " + rpcReq);
                this.activeWorkers.get(rpcReq.tid).put(rpcReq);
            }
        }
    }

}
