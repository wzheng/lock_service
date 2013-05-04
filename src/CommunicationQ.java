import java.io.*;
import java.util.*;

public class CommunicationQ {
    
    private ArrayList<String> queue;

    public CommunicationQ {
	queue = new ArrayList<String>();
    }

    public synchronized String get() {
	if (queue.isEmpty()) {
	    return "";
	} else {
	    String ret = queue.get(0);
	    queue.remove(0);
	    return ret;
	}
    }

    public synchronized void put(String item) {
	queue.add(item);
    }
    
}