import java.io.*;
import java.util.*;

public class CommunicationQ {
    
    private ArrayList<Object> queue;

    public CommunicationQ {
	queue = new ArrayList<Object>();
    }

    public synchronized Object get() {
	if (queue.isEmpty()) {
	    return "";
	} else {
	    Object ret = queue.get(0);
	    queue.remove(0);
	    return ret;
	}
    }

    public synchronized void put(Object item) {
	queue.add(item);
    }
    
}