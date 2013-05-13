package main;
import java.io.*;
import java.util.*;

/**
 * Basic class for in-memory data storage just a key value store for now,
 * separated by partitions
 */

public class Data {

    private HashMap<Integer, HashMap<String, String>> kvStore;

    public Data() {
        kvStore = new HashMap<Integer, HashMap<String, String>>();
    }

    public synchronized String get(int partition, String key) {
	Integer partNum = new Integer(partition);
	if (kvStore.containsKey(partNum)) {
	    return kvStore.get(partNum).get(key);
	} else {
	    return "";
	}
    }

    public synchronized void put(int partition, String key, String value) {
	Integer partNum = new Integer(partition);
	if (!kvStore.containsKey(partNum)) {
	    HashMap<String, String> partitionData = new HashMap<String, String>();
	    partitionData.put(key, value);
	    kvStore.put(partNum, partitionData);
	} else {
	    kvStore.get(partNum).put(key, value);
	}
    }

    public synchronized HashMap<String, String> getPartition(int partition) {
	return kvStore.get(new Integer(partition));
    }

    public synchronized void addPartition(int partition, HashMap<String, String> partitionData) {
	kvStore.put(new Integer(partition), partitionData);
    }
}