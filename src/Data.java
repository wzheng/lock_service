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

    public void addNewPartition(int partitionNum) {
        kvStore.put(new Integer(partitionNum), new HashMap<String, String>());
    }

    public void insert(int partitionNum, String key, String value) {

    }

}