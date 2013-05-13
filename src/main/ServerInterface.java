package main;
import java.io.*;
import java.util.*;
import java.net.*;

public class ServerInterface {
    private ServerSocket serverSocket;
    private int port;

    public ServerInterface(int port) {
        try {
            this.port = port;
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            System.err.println("Server construction failed on port " + port);
            System.exit(-1);
        }
    }

    public String processInput(String input) {
        if (input.equals("Commit")) {

        } else if (input.equals("Start")) {

        } else if (input.equals("End")) {

        }
        return "";
    }

    public void run() {
        Socket clientSocket = null;

        try {
            clientSocket = serverSocket.accept();
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),
                    true);
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    clientSocket.getInputStream()));
            String inputLine, outputLine;

            while ((inputLine = in.readLine()) != null) {
                this.processInput(inputLine);
            }

        } catch (IOException e) {
            System.err.println("Error occurred when accepting from port "
                    + port);
            System.exit(-1);
        }
    }

    public static void main(String[] args) {

    }
}