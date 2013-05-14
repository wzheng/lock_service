package main;
import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class LockTable {

    // a hashmap with record-level locking -- locks acquired on the primary key
    private HashMap<String, HashSet<TransactionId>> read_locks;
    private HashMap<String, TransactionId> write_locks;
    private HashMap<TransactionId, HashSet<String>> state;
    private HashMap<TransactionId, HashSet<TransactionId>> wfg;
    private ServerAddress sa;

    private HashSet<TransactionId> aborts;
    

    // TODO: locking performance?

    public LockTable(ServerAddress sa) {
        read_locks = new HashMap<String, HashSet<TransactionId>>();
        write_locks = new HashMap<String, TransactionId>();
        state = new HashMap<TransactionId, HashSet<String>>();
        wfg = new HashMap<TransactionId, HashSet<TransactionId>>();
        this.sa = sa;
    }
    
    public HashSet<TransactionId> getWFG(TransactionId tid){
    	return wfg.get(tid);
    }

    public boolean lockW(String key, TransactionId tid) {
    	long startTime = System.currentTimeMillis();
    	long timeout = 1000;
    	boolean flag = true;
        //while (true) {

            synchronized (this) {
                TransactionId wtid = write_locks.get(key);
                HashSet<TransactionId> rtid = read_locks.get(key);
                // Lock upgrade
                if (rtid != null && rtid.contains(tid) && rtid.size() == 1) {
                    rtid.remove(tid);
                    read_locks.put(key, rtid);
                    write_locks.put(key, tid);
                    // updateState(tid, key, false);
                    synchronized(this){
                    DeadlockTest.print("lock successfully obtained by TID " + tid.getTID() + " for key " + key);
                    DeadlockTest.print("locks held:");
                    for (Map.Entry<String, TransactionId> e : write_locks.entrySet()){
                    	DeadlockTest.print(e.getKey() + " : " + e.getValue().getTID());
                    }
                    }
                    return true;
                    //return;
                }
                else if ((wtid == null || wtid == tid) && (rtid == null || rtid.isEmpty())) {
                    write_locks.put(key, tid);
                    // updateState(tid, key, false);
                    synchronized(this){
                    DeadlockTest.print("lock successfully obtained by TID " + tid.getTID() + " for key " + key);
                    DeadlockTest.print("locks held:");
                    for (Map.Entry<String, TransactionId> e : write_locks.entrySet()){
                    	DeadlockTest.print(e.getKey() + " : " + e.getValue().getTID());
                    }
                    }
                    return true;
                    //return;
                }
            }
            synchronized (this) {
                TransactionId wtid = write_locks.get(key);
                HashSet<TransactionId> rtid = read_locks.get(key);
                HashSet<TransactionId> ret = wfg.get(tid);
                if (ret == null) {
                    ret = new HashSet<TransactionId>();
                }
                if (wtid != null) {
                    ret.add(wtid);
                }
                if (rtid != null && !rtid.isEmpty()) {
                    Iterator<TransactionId> it = rtid.iterator();
                    while (it.hasNext()) {
                        ret.add(it.next());
                    }
                }
                //System.out.println("locking " + key + " for tid " + tid.getTID() + " stuck");
                wfg.put(tid, ret);
       	     if (System.currentTimeMillis() - startTime > timeout){
     	     	RPCRequest args = new RPCRequest("abort", tid.getServerAddress(), tid,
     	     					 new HashMap<String, Object>());
     	     	RPC.send(tid.getServerAddress(), "abort", "001", args.toJSONObject());
     	     	DeadlockTest.print("deadlock detected by timeout");
     	     	//break;
     	     }
                if (flag){
                	//cmhDeadlockInitiate(tid);
                	flag = false;
                } else {
                	
                }
                synchronized(this){
                DeadlockTest.print("lock NOT obtained by TID " + tid.getTID() + " for key " + key);
                DeadlockTest.print("locks held:");
                for (Map.Entry<String, TransactionId> e : write_locks.entrySet()){
                	DeadlockTest.print(e.getKey() + " : " + e.getValue().getTID());
                }
                DeadlockTest.print("wfg:");
                for (Entry<TransactionId, HashSet<TransactionId>> e : wfg.entrySet()){
                	String s = "";
                	for (TransactionId t : e.getValue()){
                		s += t.getTID() + ", ";
                	}
                	DeadlockTest.print(e.getKey().getTID() + " : " + s);
                }
                }
                return false;
                //checkDeadLock(tid);
            }
            

			
        //}
    }

    public void lockR(String key, TransactionId tid) {
    	long startTime = System.currentTimeMillis();
    	long timeout = 500;
        while (true) {
//        	if (System.currentTimeMillis() - startTime > timeout) {
//        		RPCRequest args = new RPCRequest("abort", tid.getServerAddress(), tid,
//        				new HashMap<String, Object>());
//        		RPC.send(tid.getServerAddress(), "abort", "001", args.toJSONObject());
//        		System.out.println("deadlock detected by timeout");
//        		break;
//        	}
            synchronized (this) {
                TransactionId wtid = write_locks.get(key);
                if (wtid == null || wtid == tid) {
                    HashSet<TransactionId> rtid = read_locks.get(key);
                    if (rtid == null) {
                        rtid = new HashSet<TransactionId>();
                        rtid.add(tid);
                        read_locks.put(key, rtid);
                        // updateState(tid, key, false);
                        return;
                    } else {
                        rtid.add(tid);
                        read_locks.put(key, rtid);
                        // updateState(tid, key, false);
                        return;
                    }
                }
            }

            synchronized (this) {
                TransactionId wtid = write_locks.get(key);
                HashSet<TransactionId> rtid = read_locks.get(key);
                HashSet<TransactionId> ret = wfg.get(tid);
                if (ret == null) {
                    ret = new HashSet<TransactionId>();
                }
                if (wtid != null) {
                    ret.add(wtid);
                }
                wfg.put(tid, ret);
                cmhDeadlockInitiate(tid);
                //checkDeadLock(tid);
            }
        }

    }

    public synchronized void unlockR(String key, TransactionId tid) {
        HashSet<TransactionId> rtid = read_locks.get(key);
        if (rtid != null && rtid.contains(tid)) {
            rtid.remove(tid);
            read_locks.put(key, rtid);
            // updateState(tid, key, true);
        }
    }

    public synchronized void unlockW(String key, TransactionId tid) {
        if (write_locks.get(key) != null && write_locks.get(key).equals(tid)) {
            write_locks.remove(key);
            // updateState(tid, key, true);
            System.out.println("key " + key + " was unlocked by transaction " + tid.getTID());
        }
    }

    public synchronized boolean holdsLock(String pid, TransactionId tid) {
        return (write_locks.get(pid) == tid || read_locks.get(pid).contains(tid));
    }

    public synchronized HashSet<String> getPages(TransactionId tid) {
        return state.get(tid);
    }

    // private void updateState(TransactionId tid, PageId pid, boolean isDone) {
    // HashSet<PageId> hs = state.get(tid);
    // if (hs == null) {
    // hs = new HashSet<PageId>();
    // }
    // if (!isDone) {
    // hs.add(pid);
    // } else {
    // hs.remove(pid);
    // }
    // state.put(tid, hs);
    // }

    private void updateWFG(TransactionId tid) {
        wfg.remove(tid);
        Iterator<TransactionId> it = wfg.keySet().iterator();
        while (it.hasNext()) {
            TransactionId t = (TransactionId) it.next();
            HashSet<TransactionId> hs = (HashSet<TransactionId>) wfg.get(t);
            hs.remove(tid);
            wfg.put(t, hs);
        }
    }
    
    /**
     * Checks if the current lock acquire operation is in deadlock.
     * Sends a Chandy-Misra-Haas message to the processes that hold locks to processes in WFG.
     */
    public void cmhDeadlockInitiate(TransactionId tid){
    	
    	CMHProcessor cmhProcessor = new CMHProcessor();
    	// generate initial message from this TID and any others it's waiting on
    	cmhProcessor.generateMessage(tid, getWFG(tid));
    	
    }
    
    private void checkDeadLock(TransactionId tid) {
        // check to see if there are loops in the wfg
        if (aborts.contains(tid)) {
            updateWFG(tid);
            aborts.remove(tid);
            // throw new TransactionAbortedException();
        }

        HashSet<TransactionId> alreadySeenTids = new HashSet<TransactionId>();
        Stack<TransactionId> stack = new Stack<TransactionId>();

        stack.push(tid);
        while (true) {
            if (stack.isEmpty()) {
                break;
            }
            TransactionId tempid = stack.pop();

            if (!alreadySeenTids.contains(tempid)) {
                alreadySeenTids.add(tempid);
            } else {
                // System.out.println("Aborting " + tid + " Found id is " +
                // tempid);
                if (tempid.equals(tid)) {
                    updateWFG(tid);
                    // throw new TransactionAbortedException();
                } else {
                    aborts.add(tempid);
                    return;
                }
            }

            HashSet<TransactionId> temp = wfg.get(tempid);
            if (temp != null) {
                Iterator<TransactionId> it = temp.iterator();
                while (it.hasNext()) {
                    stack.push((TransactionId) it.next());
                }
            }
        }
    }

}
