import java.util.*;
import java.io.*;

public class TransactionId {
    
    private String serverName;
    private int tid;

    public TransactionId(String serverName, int tid) {
	this.serverName = serverName;
	this.tid = tid;
    }

    public String getServerName() {
	return serverName;
    }

    public int getTID() {
	return tid;
    }

}