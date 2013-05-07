import java.io.*;
import java.util.*;

public class TransactionContext {

    public TransactionId tid;
    public boolean isRW;
    public HashMap<String, String> write_set;
    public HashMap<String, String> read_set;

    public void parseJSON(Map<String, Object> params) {

        String serverName = (String) params.get("ServerName");
        int serverNumber = ((Integer) params.get("ServerNumber")).intValue();
        int serverPort = ((Integer) params.get("ServerPort")).intValue();

        int tidNum = ((Integer) params.get("TID")).intValue();

        this.tid = new TransactionId(new ServerAddress(serverNumber,serverName, serverPort), tidNum);
        this.isRW = ((Boolean) params.get("RW")).booleanValue();
        this.write_set = (HashMap<String, String>) params.get("Write Set");
        this.read_set = (HashMap<String, String>) params.get("Read Set");

    }

    public HashMap<String, Object> toJSONObject() {
        HashMap<String, Object> ret = new HashMap<String, Object>();

        ServerAddress sa = tid.getServerAddress();

        ret.put("ServerName", sa.getServerName());
        ret.put("ServerNumber", sa.getServerNumber());
        ret.put("ServerPort", sa.getServerPort());

        ret.put("TID", tid.getTID());

        ret.put("RW", isRW);
        ret.put("Write Set", write_set);
        ret.put("Read Set", read_set);
        return ret;
    }

}