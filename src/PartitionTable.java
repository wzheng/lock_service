import java.util.*;
import java.io.*;

/**
 * Partition table stores all of the information about which server(s) serve(s)
 * which partition The version vector tells how old the partition table is
 */

public class PartitionTable {

    // partition -> server
    public HashMap<Integer, ServerAddress> psTable;
    // server -> partition
    public HashMap<ServerAddress, ArrayList<Integer> > spTable;
    

    public PartitionTable() {
    	psTable = new HashMap<Integer, ServerAddress>();
    	spTable = new HashMap<ServerAddress, ArrayList<Integer> >();
    }

    // this supports both adding partition and changing partition
    // TODO: changing partitions?
    public void addPartition(int pNum, ServerAddress server) {
        Integer num = new Integer(pNum);
        psTable.put(num, server);
        ArrayList<Integer> value = spTable.get(num);
		if (value == null) {
		    value = new ArrayList<Integer>();
		}
		value.add(num);
		spTable.put(server, value);
    }

    public ServerAddress getServer(int pNum) {
    	return psTable.get(new Integer(pNum));
    }

    
}