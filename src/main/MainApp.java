package main;
import java.util.*;
import java.io.*;

import com.thetransactioncompany.jsonrpc2.*;

/**
 * MainApp is responsible for all of the initialization for this database
 */

public class MainApp {

    public static void main(String[] args) {
        System.out.println("Starting the program...");
        ServerStarter s = new ServerStarter("Server 1", 8000);
    }

}