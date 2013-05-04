import java.util.*;
import java.io.*;

public class TransactionId {
    
    private ServerAddress sa;
    private int tid;

    public TransactionId(ServerAddress sa, int tid) {
	this.sa = sa;
	this.tid = tid;
    }

    public ServerAddress getServerAddress() {
	return sa;
    }

    public int getTID() {
	return tid;
    }

    public boolean equals(Object obj) {
	if (obj == null) {
	    return false;
	}
	if (obj == this) {
	    return true;
	}
	if (!(obj instanceof TransactionId)) {
	    return false;
	}

	TransactionId tid = (TransactionId) obj;
	if (tid.getServerAddress().equals(this.sa) && (tid.getTID() == this.tid)) {
	    return true;
	}
	return false;

    }

}