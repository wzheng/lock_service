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

    public synchronized String get(String key) {
	return kvStore.get(new Integer(0)).get(key);
    }

    public synchronized String put(String key, String value) {
	return kvStore.get(new Integer(0)).put(key, value);
    }

}