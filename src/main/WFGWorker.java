package main;

import java.util.HashMap;
import java.util.HashSet;

public class WFGWorker implements Runnable {
	private Server server;
	private CommunicationQ queue;
	int wfgCounter;
    private HashSet<TransactionId> wfg;
    public boolean isReady;
    public WFGWorker(Server server, CommunicationQ queue) {
        this.server = server;
        this.queue = queue;
        wfgCounter = 0;
        wfg = new HashSet<TransactionId>();
        isReady = false;
    }
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
    	isReady = false;
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
    		isReady = true;
	        DeadlockTest.print("WHOLEWFG for TID " + rpcReq.tid.getTID());
	        String s = "";
	        for (TransactionId x : wfg){
	        	s += x.getTID() + ", ";
	        }
	        DeadlockTest.print(s);
			//cmhProcessor.generateMessage(rpcReq.tid, wfg);
    	}

    }
    

    public void run(){
    	
    }
}
