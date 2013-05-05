import java.io.*;
import java.util.*;

public class RPCRequest {

    public ServerAddress replyAddress;
    private Object args;

    public RPCRequest(Map<String, Object> rpc) {
	
	int port = (int) rpc.get("Reply Port");
	String name = (String) rpc.get("Reply Name");
	int number = (int) rpc.get("Reply Number");

	replyAddress = new ServerAddress(number, name, port);
    }

    public void addArgs(Object obj) {
	args = obj;
    }

    public Object getArgs() {
	return args;
    }

}