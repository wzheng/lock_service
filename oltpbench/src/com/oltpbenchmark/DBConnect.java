package com.oltpbenchmark;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.*;
import java.io.*;
import java.net.*;

import com.mysql.jdbc.Connection;

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

    public void commit() throws SQLException {
    	this.out.println("Commit");
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

    public void rollback() throws SQLException {
    	this.out.println("Rollback");
    }

    public void close() throws SQLException {

    }

    public void executeBatch(ArrayList<String> sqlStatements) {
	
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