package main;

import java.io.*;
import java.net.*;
import java.util.*;

import com.thetransactioncompany.jsonrpc2.*;

/**
 * This is a wrapper around the json-rpc library to facilitate RPC
 */

public class RPC {

    private ServerAddress address;
    private ServerSocket server;
    private PrintWriter out;
    private BufferedReader in;

    public RPC(ServerAddress sa) {
	this.address = sa;

	try {
	    this.server = new ServerSocket(address.getServerPort());
	} 
	catch (IOException e) {
	    System.out.println("Could not listen on port " + address.getServerPort());
	    System.exit(-1);
	}

    }

    private static void send(int port, String message) {
        Socket echoSocket = null;
        PrintWriter out = null;

        try {
            echoSocket = new Socket("localhost", port);
            out = new PrintWriter(echoSocket.getOutputStream(), true);
	    //System.out.println("Trying to send message " + message + " to port " + port);
	    out.println(message);
	    //System.out.println("Message sent");
        } catch (UnknownHostException e) {
            System.err.println("Don't know about host " + port);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Couldn't get I/O for " + "the connection " + port);
            System.exit(1);
        }
    }

    public static void send(ServerAddress sa, String methodName, String uid,
			    HashMap<String, Object> args) {
        JSONRPC2Request reqOut = new JSONRPC2Request(methodName, args, uid);
        String jsonString = reqOut.toString();
        RPC.send(sa.getServerPort(), jsonString);
    }

    public static void send(int port, JSONRPC2Request rpcObject) {
        RPC.send(port, rpcObject.toString());
    }

    // this call blocks
    public JSONRPC2Request receive() {

	Socket clientSocket = null;
	try {
	    //System.out.println("Try to receive on port " + address.getServerPort());
	    clientSocket = server.accept();

	    //PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
	    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
	    
	    String request = null;
	    try {
		request = in.readLine();
	    } catch (IOException e1) {
		e1.printStackTrace();
	    }
	    
	    JSONRPC2Request reqIn = null;
	    try {
		reqIn = JSONRPC2Request.parse(request);
		return reqIn;
	    } catch (JSONRPC2ParseException e) {
		System.err.println(e.getMessage());
	    }
	    
	}  catch (IOException e) {
            System.err.println("I/O failed when accepting a connection from port " + address.getServerPort());
        }
	return null;
    }

}
