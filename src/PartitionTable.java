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
    public void addPartition(int pNum, ServerAddress server) {
        Integer num = new Integer(pNum);
	ServerAddress prevServer = psTable.get(num);
	if (prevServer == null) {
	    psTable.put(num, server);
	    ArrayList<Integer> values = new ArrayList<Integer>();
	    values.add(num);
	    spTable.put(server, values);
	} else {
	    psTable.put(num, server);
	    ArrayList<Integer> values = spTable.get(prevServer);
	    values.remove(num);
	    spTable.put(prevServer, values);

	    values = spTable.get(server);
	    if (values == null) {
		values = new ArrayList<Integer>();
	    }
	    values.add(num);
	    spTable.put(server, values);
	}
    }

    public ServerAddress getServer(int pNum) {
	return psTable.get(new Integer(pNum));
    }

}