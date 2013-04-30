package com.oltpbenchmark;

import java.util.*;
import java.io.*;
import java.net.*;

public class DBConnect {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    
    public DBConnect(String url, int port) {
	try {
	    socket = new Socket(url, port);
	    out = new PrintWriter(socket.getOutputStream(), true);
	    in = new BufferedReader( new InputStreamReader(socket.getInputStream()) );
	} catch (UnknownHostException e) {
	    System.err.println("Don't know about host: " + url + ":" + port);
            System.exit(1);
	} catch (IOException e) {
	    System.err.println("Couldn't get I/O for the connection to: " + url + ":" + port);
            System.exit(1);
	}
    }

    public void commit() {
	this.out.println("Commit");
    }

    public void rollBack() {
	this.out.println("Rollback");
    }

    public boolean executeQuery(String sql) {
	this.out.println("Start");
	this.out.println(sql);
	this.out.println("End");

	return true;
    }

    public void setAutoCommit(boolean ac) {
	
    }

    public boolean getAutoCommit() {
	return false;
    }

    public void rollback() {

    }

    public void close() {

    }

    public void executeBatch(ArrayList<String> sqlStatements) {
	
    }
    
}