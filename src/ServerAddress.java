import java.io.*;
import java.util.*;

public class ServerAddress {
    private String serverName;
    private int port;

    public ServerAddress(String name, int port) {
	this.serverName = name;
	this.port = port;
    }

    public String getServerName() {
	return this.name;
    }

    public int getPort() {
	return this.port;
    }
}