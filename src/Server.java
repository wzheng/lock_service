import java.util.*;
import java.io.*;


public class Server {
    
    private String name;
    private int port;
    private LockTable lockTable;

    public Server(String name, int port) {
	this.name = name;
	this.port = port;
	lockTable = new LockTable();
    }

    // locking will lock a key from a particular partition
    public void lock(int partitionNum, String key) {
	
    }

    public void unlock(int partitionNum, String key) {
	
    }

    public void reconfigure(View oldview, View newview) {
	
    }

}