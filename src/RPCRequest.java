import java.io.*;
import java.util.*;

/**
 * This class is only used for transmitting information between a server and a
 * worker
 */

public class RPCRequest {

    public ServerAddress replyAddress;
    public String method;
    public Object args;
    public TransactionId tid;

    public RPCRequest(String method, Map<String, Object> rpc) {

        HashMap<String, Object> address = (HashMap<String, Object>) rpc.get("Address");

        int port = ((Integer) address.get("Reply Port")).intValue();
        String name = (String) address.get("Reply Name");
        int number = ((Integer) address.get("Reply Number")).intValue();
        replyAddress = new ServerAddress(number, name, port);

        this.args = rpc.get("Args");
        this.method = method;

        HashMap<String, Object> tid = (HashMap<String, Object>) rpc.get("TID");
        port = ((Integer) tid.get("Port")).intValue();
        name = (String) tid.get("Name");
        number = ((Integer) tid.get("Number")).intValue();

        this.tid = new TransactionId(new ServerAddress(number, name, port), ((Integer) tid.get("TID")).intValue());
    }

    public RPCRequest(String method, ServerAddress sa, TransactionId tid,
		      HashMap<String, Object> args) {
        this.replyAddress = sa;
        this.method = method;
        this.tid = tid;
        this.args = args;
    }

    public HashMap<String, Object> toJSONObject() {
        HashMap<String, Object> ret = new HashMap<String, Object>();
        HashMap<String, Object> address = new HashMap<String, Object>();
        address.put("Reply Port", replyAddress.getServerPort());
        address.put("Reply Name", replyAddress.getServerName());
        address.put("Reply Number", replyAddress.getServerNumber());
        ret.put("Address", address);

        HashMap<String, Object> ret_tid = new HashMap<String, Object>();
        ret_tid.put("Port", tid.getServerAddress().getServerPort());
        ret_tid.put("Number", tid.getServerAddress().getServerNumber());
        ret_tid.put("Name", tid.getServerAddress().getServerName());
        ret_tid.put("TID", tid.getTID());
        ret.put("TID", ret_tid);

        ret.put("Args", this.args);

        return ret;
    }
}