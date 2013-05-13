package main;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


import com.thetransactioncompany.jsonrpc2.*;
import com.thetransactioncompany.jsonrpc2.util.*;

/**
 * Class for processing Chandy-Misra-Haas messages Designed to be set up on a
 * single "process" upon creation
 */
public class CMHProcessor {
    private ServerAddress currentServer;

    private Set<CMHMessage> messagesToSend;
    private Set<ServerAddress> nextServerAddresses;
    
    private RPC rpc;

    boolean deadlocked = false;
    
    //RPC rpc = null;
    
    

    // testing
    /**
     * A set of TIDs that holds locks for processes this is waiting for
     */
    private Set<Integer> waitingForTIDs;

    public CMHProcessor(TransactionId transaction) {
        //this.currentServer = transaction.getServerAddress();
        this.messagesToSend = new HashSet<CMHMessage>();
    	//this.messageToSend = null;
        this.nextServerAddresses = new HashSet<ServerAddress>();
        this.waitingForTIDs = new HashSet<Integer>();
    }
    
    public CMHProcessor(ServerAddress sa){
        this.currentServer = sa;
        this.messagesToSend = new HashSet<CMHMessage>();
    	//this.messageToSend = null;
        this.nextServerAddresses = new HashSet<ServerAddress>();
        this.waitingForTIDs = new HashSet<Integer>();
    }

    /**
     * Called when a JSON message is received by this process
     * 
     * @param message
     *            String JSON message to be processed
     */
    public void processMessage(String message) {
        JSONRPC2Notification req = null;
        try {
            req = JSONRPC2Notification.parse(message);

        } catch (JSONRPC2ParseException e) {
            e.printStackTrace();
        }

        NamedParamsRetriever np = new NamedParamsRetriever(req.getNamedParams());
        /** The TID of the process that started this chain of messages */
        int initiatorTID;

        /** The TID of the process that sent a message here */
        int fromTID;

        /** The TID of the current process */
        int thisTID;

        try {
            initiatorTID = np.getInt("initiator");
            fromTID = np.getInt("from");
            thisTID = np.getInt("to");
            detectDeadlock(initiatorTID, fromTID, thisTID);
        } catch (JSONRPC2Error e) {
            e.printStackTrace();
            System.out.println("Could not process message!");
        }

    }

    public void addWaitingForTid(int tid) {
        waitingForTIDs.add(tid);
    }

    public void removeWaitingForTid(int tid) {
        waitingForTIDs.remove(tid);
    }

    public void clearWaitingForTid() {
        waitingForTIDs.clear();
    }
    
    /**
     * Determines if a deadlock exists by checking the initiator and to fields
     * Sends a message if no deadlock detected but is still waiting for
     * resources
     * 
     * @return true if deadlock exists
     */
    public boolean detectDeadlock(int initiatorTID, int fromTID, int thisTID) {
        // deadlock if cycle is complete
        if (initiatorTID == thisTID) {
            deadlocked = true;
            return true;
        }

        // figure out which processes are holding locks to resources it's
        // requesting

        // if it is waiting for another resource already, forward the next
        // message in the chain
        if (waitingForTIDs.size() > 0) {
            for (int nextTID : waitingForTIDs) {
                CMHMessage nextMessage = new CMHMessage(initiatorTID, thisTID,
                        nextTID);
                nextMessage.sendMessage();
            }
        }

        // if not waiting for resources, not deadlocked
        deadlocked = false;
        return false;
    }
    
    /**
     * Generates initial Chandy-Misra-Haas message(s)
     * @param currentTransaction The TID that generates this message
     * @param waitingForTransactions Transactions which this TID is waiting for
     */
    public void generateMessage(TransactionId currentTransaction, Set<TransactionId> waitingForTransactions) {
    	//rpc = new RPC(currentTransaction.getServerAddress());
    	for (TransactionId waitingFortid: waitingForTransactions) {
    		CMHMessage msg = new CMHMessage(currentTransaction, currentTransaction, waitingFortid);
    		messagesToSend.add(msg);
    		HashMap<String, Object> args = msg.getArgs();
    		RPCRequest req = new RPCRequest("deadlock", waitingFortid.getServerAddress(), currentTransaction, args);
    		RPC.send(waitingFortid.getServerAddress(), "deadlock", "001", req.toJSONObject());
    		System.out.println("CMH message sent from " + currentTransaction.getTID() + " to " + waitingFortid.getTID());
    		//JSONRPC2Request resp = rpc.receive();
    	}
    }
    
    /**
     * Sends a Chandy-Misra-Haas message along the path
     */
    public void propagateMessage(int initiator, TransactionId currentTransaction, Set<TransactionId> waitingForTransactions){
    	//rpc = new RPC(currentTransaction.getServerAddress());
    	for (TransactionId waitingFortid: waitingForTransactions) {
    		CMHMessage msg = new CMHMessage(initiator, currentTransaction.getTID(), waitingFortid.getTID());
    		messagesToSend.add(msg);
    		HashMap<String, Object> args = msg.getArgs();
    		RPCRequest req = new RPCRequest("deadlock", waitingFortid.getServerAddress(), currentTransaction, args);
    		RPC.send(waitingFortid.getServerAddress(), "deadlock", "001", req.toJSONObject());
    		//JSONRPC2Request resp = rpc.receive();
    	}
    }

}