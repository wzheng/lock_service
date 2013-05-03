import java.util.*;
import java.io.*;

/**
 *  Partition table stores all of the information about which server(s) serve(s) which partition
 *  The version vector tells how old the partition table is
 */

public class PartitionTable {

    public class PartitionData {
	
	private int partitionNumber;
	private ArrayList<ServerAddress> servers; // contains a list of servers to contact
	
	public PartitionData(int num) {
	    this.partitionNumber = num;
	}
	
	public void addServer(ServerAddress server) {
	    this.servers.add(server);
	}
	
	public int getPartitionNumber() {
	    return this.partitionNumber;
	}
	
    }

    private HashMap<Integer, PartitionData> table;
    private VersionVector vv;

    public PartitionTable(int size) {
	vv = new VersionVector(size);
    }

    public addPartition(int pNum, ServerAddress server) {
	Integer num = new Integer(pNum);
	
	if (table.contains(num)) {
	    PartitionData pd = table.get(num);
	    pd.addServer(server);
	    table.put(num, pd);
	} else {
	    PartitionData pd = new PartitionData(pNum);
	    pd.addServer(server);
	    table.put(num, pd);
	}
    }

    public Iterator getPartitions() {
	return table.keySet().iterator();
    }
    
}