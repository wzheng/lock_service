package main;
import java.io.*;
import java.util.*;

/**
 * Basic class for in-memory data storage just a key value store for now,
 * separated by partitions
 */

public class Data {

    // Parition: { Table: { key value pairs } }
    private HashMap<Integer, HashMap<String, HashMap<String, String> > > kvStore;

    public Data() {
        kvStore = new HashMap<Integer, HashMap<String, HashMap<String, String> >>();
    }

    public synchronized String get(int partition, String table, String key) {
	Integer partNum = new Integer(partition);
	if (kvStore.containsKey(partNum)) {
	    HashMap<String, HashMap<String, String> > partitionData = kvStore.get(partNum);
	    if (partitionData.containsKey(table)) {
		HashMap<String, String> data = partitionData.get(table);
		if (data.containsKey(key)) {
		    return data.get(key);
		} else {
		    return "";
		}
	    } else {
		return "";
	    }
	} else {
	    return "";
	}
    }

    public synchronized void put(int partition, String table, String key, String value) {
	Integer partNum = new Integer(partition);
	if (!kvStore.containsKey(partNum)) {
	    HashMap<String, HashMap<String, String> > partitionData = new HashMap<String, HashMap<String, String> >();
	    HashMap<String, String> data = new HashMap<String, String>();
	    data.put(key, value);
	    partitionData.put(table, data);
	    kvStore.put(partNum, partitionData);
	} else {
	    HashMap<String, HashMap<String, String> > partitionData = kvStore.get(partNum);
	    if (!partitionData.containsKey(table)) {
		HashMap<String, String> data = new HashMap<String, String>();
		data.put(key, value);
		partitionData.put(table, data);
	    } else {
		partitionData.get(table).put(key, value);
	    }
	}
    }

    public synchronized HashMap<String, HashMap<String, String> > getPartition(int partition) {
	return kvStore.get(new Integer(partition));
    }

    public synchronized void addPartition(int partition, HashMap<String, HashMap<String, String> > partitionData) {
	kvStore.put(new Integer(partition), partitionData);
    }
}