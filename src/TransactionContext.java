import java.util.*;

public class TransactionContext {

    public TransactionId tid;
    public boolean isRW;
    public HashMap<String, String> write_set;
    public HashMap<String, String> read_set;
    

    public TransactionContext(TransactionId tid, Map<String, Object> params) {
	
    	this.tid = tid;

        //this.isRW = ((Boolean) params.get("RW")).booleanValue();
        this.write_set = (HashMap<String, String>) params.get("Write Set");
        this.read_set = (HashMap<String, String>) params.get("Read Set");

    }

    public HashMap<String, Object> toJSONObject() {
        HashMap<String, Object> ret = new HashMap<String, Object>();

        ServerAddress sa = tid.getServerAddress();

        // ret.put("ServerName", sa.getServerName());
        // ret.put("ServerNumber", sa.getServerNumber());
        // ret.put("ServerPort", sa.getServerPort());

        // ret.put("TID", tid.getTID());

        //ret.put("RW", isRW);
        ret.put("Write Set", write_set);
        ret.put("Read Set", read_set);
        return ret;
    }

}