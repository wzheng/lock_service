import java.util.HashMap;
import java.util.HashSet;

public class View {

    private HashMap<String, HashSet<Integer>> view;
    private HashMap<Integer, PartitionTable.PartitionData> partitions;

    public void addServer(String serverName) {
        view.put(serverName, new HashSet<Integer>());
    }

    public void addPartition(String serverName,
            PartitionTable.PartitionData data) {
        Integer num = new Integer(data.getPartitionNumber());
        if (!partitions.containsKey(num)) {
            HashSet<Integer> old_list = view.get(serverName);
            if (old_list == null) {
                old_list = new HashSet<Integer>();
            }
            old_list.add(num);
            view.put(serverName, old_list);
            partitions.put(num, data);
        }
    }

    public void changePartition(int partitionNum, String old_server,
            String new_server) {
        HashSet<Integer> from_list = view.get(old_server);
        HashSet<Integer> to_list = view.get(new_server);
        from_list.remove(new Integer(partitionNum));
        to_list.add(new Integer(partitionNum));
    }

}