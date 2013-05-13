package main;
import java.io.*;
import java.util.*;

/**
 *  This is a table that stores affinity factors betwwen partitions.
 *  The higher the AF, the more related the partitions
 */

public class AFTable {

    private HashMap<Pair, Integer> afTable;
    
    public AFTable() {
	afTable = new HashMap<Pair, Integer>();
    }

    // increment by 1
    public synchronized void increment(int partition1, int partition2) {
	Pair p = new Pair(partition1, partition2);
	Integer i = afTable.get(p);
	if (i == null) {
	    afTable.put(p, new Integer(1));
	} else {
	    afTable.put(p, new Integer(i.intValue() + 1));
	}
    }

    public synchronized HashMap<String, Object> toJSONObject() {
	HashMap<String, Object> ret = new HashMap<String, Object>();

	Iterator it = afTable.entrySet().iterator();
	int i = 0;
	while (it.hasNext()) {
	    HashMap<String, Object> temp = new HashMap<String, Object>();
	    Map.Entry kv = (Map.Entry) it.next();
	    temp.put("af", kv.getValue());
	    temp.put("p1", ((Pair) kv.getKey()).p1);
	    temp.put("p2", ((Pair) kv.getKey()).p2);
	    ret.put(Integer.toString(i), temp);
	    i++;
	}

	return ret;
    }

    public String toString() {
	String ret = new String();
	Iterator it = afTable.entrySet().iterator();
	ret += "Start AF Table --> \n";
	while (it.hasNext()) {
	    Map.Entry entry = (Map.Entry) it.next();
	    ret += ((Pair) entry.getKey()).toString() + " " + ((Integer) entry.getValue()).toString() + "\n";
	}
	ret += " <-- End AF Table\n";
	return ret;
    }

}