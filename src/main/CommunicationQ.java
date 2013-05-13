package main;
import java.util.*;

public class CommunicationQ {

    private ArrayList<Object> queue;

    public CommunicationQ() {
        queue = new ArrayList<Object>();
    }

    public synchronized Object get() {
	//System.out.println("Get item");
        if (queue.isEmpty()) {
            return "";
        } else {
            Object ret = queue.get(0);
            queue.remove(0);
            return ret;
        }
    }

    public synchronized void put(Object item) {
	//System.out.println("Added item");
        queue.add(item);
    }

}