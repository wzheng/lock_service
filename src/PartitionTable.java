import java.util.*;
import java.io.*;

/**
 * Partition table stores all of the information about which server(s) serve(s)
 * which partition The version vector tells how old the partition table is
 */

public class PartitionTable {

    private HashMap<Integer, ServerAddress> table;

    public PartitionTable() {
	table = new HashMap<Integer, ServerAddress>();
    }

    public void addPartition(int pNum, ServerAddress server) {
        Integer num = new Integer(pNum);
	table.put(num, server);
    }

    public ServerAddress getServer(int pNum) {
	return table.get(new Integer(pNum));
    }
}