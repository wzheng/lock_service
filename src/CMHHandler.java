import java.util.HashMap;

/**
 * Class used for testing; provides way to access separate processes without RPC
 * 
 */
public class CMHHandler {
    public static HashMap<Integer, CMHProcessor> processes;

    public CMHHandler() {
        processes = new HashMap<Integer, CMHProcessor>();
    }

    public void add(int i, CMHProcessor p) {
        processes.put(i, p);
    }

    public CMHProcessor get(int i) {
        return processes.get(i);
    }

    public void sendMessage(int dest, String message) {
        CMHProcessor p = processes.get(dest);
        p.processMessage(message);
    }

    public void resetAllWaitingForTid() {
        for (CMHProcessor p : processes.values()) {
            p.clearWaitingForTid();
            p.deadlocked = false;
        }
    }

}
