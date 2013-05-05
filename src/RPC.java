import java.io.*;
import java.net.*;
import java.util.*;

import com.thetransactioncompany.jsonrpc2.*;

/**
 * This is a wrapper around the json-rpc library to facilitate RPC 
 */

public class RPC {

    private String serverName;
    private int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    

    public RPC(String serverName, int port) {
	this.serverName = serverName;
	this.port = port;

	try {
            socket = new Socket("localhost", port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (UnknownHostException e) {
            System.err.println("Server " + serverName + " initialize failed");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for the connection to server " + name);
            System.exit(1);
        }
    }

    private void send(int port, String message) {
	Socket echoSocket = null;
        PrintWriter out = null;

        try {
            echoSocket = new Socket("localhost", port);
            out = new PrintWriter(echoSocket.getOutputStream(), true);
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host: taranis.");
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for "
                               + "the connection to: taranis.");
            System.exit(1);
        }
	out.println(message);
    }
    
    public static void send(ServerAddress sa, String methodName, String uid, List<Object> args) {
		JSONRPC2Request reqOut = new JSONRPC2Request(methodName, args, uid);
		String jsonString = reqOut.toString();
		this.send(sa.getServerPort(), jsonString);
    }

    public static void send(int port, JSONRPC2Request rpcObject) {
    	this.send(port, rpcObject.toString());	
    }

    // this call blocks
    public JSONRPC2Request receive() {
		String request = in.readLine();
		JSONRPC2Request reqIn = null;
		try {
		    reqIn = JSONRPC2Request.parse(jsonString);
		    return reqIn;
		} catch (JSONRPC2ParseException e) {
		    System.err.println(e.getMessage());
		}
    }
    
}