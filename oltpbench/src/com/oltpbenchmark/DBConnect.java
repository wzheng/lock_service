package com.oltpbenchmark;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.*;
import java.io.*;
import java.net.*;

import main.*;

public class DBConnect {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private ServerAddress sa;
    private RPC rpc;
    
    public DBConnect(String url, int port) {
	sa = new ServerAddress(2, "C", port);
	RPC rpc = new RPC(sa);
    }

    public void commit() throws SQLException {
    	System.out.println("committing");
    	this.out.println("Commit");
    }

    public boolean executeQuery(HashMap<String, HashMap<String, String> > writes) {
	return true;
    }

    public void setAutoCommit(boolean ac) {
	
    }

    public boolean getAutoCommit() {
	return false;
    }

    public void rollback() throws SQLException {
    	System.out.println("aborting");
    	this.out.println("Rollback");
    }

    public void close() throws SQLException {
    	System.out.println("closing");
    }

    public void executeBatch(HashMap<String, HashMap<String, String> > writes) {
	
    }

    public Statement createStatement() {
	return null;
    }

    public void setTransactionIsolation(int isolationMode) {
		
    }

    public void rollback(Savepoint savepoint) throws SQLException {
	
    }

    public PreparedStatement prepareStatement(String sql) {
	return null;
    }


    public PreparedStatement prepareStatement(String sql, int[] is) {
	return null;
    }
}