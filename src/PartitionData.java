import java.util.*;
import java.io.*;

public class PartitionData {
    private int partitionNumber;
    private ArrayList<String> servers; // contains a list of servers to contact
    

    public PartitionData(int num) {
	this.partitionNumber = num;
    }

    public void addServer(String server) {
	this.servers.add(server);
    }

    public int getPartitionNumber() {
	return this.partitionNumber;
    }
}