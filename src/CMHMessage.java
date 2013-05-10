import com.thetransactioncompany.jsonrpc2.*;
import java.util.*;

/**
 * Representation of a Chandy-Misra-Haas message
 */
public class CMHMessage {
    private int initiator;
    private int from;
    private int to;

    /**
     * 
     * @param initiatorTID TID from which this message originated
     * @param fromTID TID which last received this message
     * @param toTID Next TID to receive message
     */
    public CMHMessage(TransactionId initiatorTID, TransactionId fromTID,
            TransactionId toTID) {
        this.initiator = initiatorTID.getTID();
        this.from = fromTID.getTID();
        this.to = toTID.getTID();
    }

    public CMHMessage(int initiator, int from, int to) {
        this.initiator = initiator;
        this.from = from;
        this.to = to;
    }

    /**
     * Prepares a JSON message
     * 
     * @return JSON message string
     */
    public String getJSONMessage() {
        String method = "processMessage";
        JSONRPC2Notification req = new JSONRPC2Notification(method, getArgs());
        return req.toString();
    }
    
    /**
     * Returns a Map of appropriate JSON message arguments
     * @return Map<String, Object> for JSON encoding
     */
    public HashMap<String, Object> getArgs(){
        HashMap<String, Object> params = new HashMap<String, Object>();
        params.put("initiator", initiator);
        params.put("from", from);
        params.put("to", to);
        return params;
    }

    /**
     * Sends Chandy-Misra-Haas message
     */
    public void sendMessage() {
        testSendMessage();
    }

    /**
     * In test cases, sends a message. Should be changed to allow RPC, but for
     * now it accesses them through the static CMHHandler.
     */
    public void testSendMessage() {
        CMHHandler.processes.get(to).processMessage(getJSONMessage());
    }

}
