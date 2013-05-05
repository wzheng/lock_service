import com.thetransactioncompany.jsonrpc2.*;
import java.util.*;

/**
 * Representation of a Chandy-Misra-Haas message
 */
public class CMHMessage {
	private int initiator;
	private int from;
	private int to;

	public CMHMessage(ServerAddress initiatorServerAddress,
			ServerAddress fromServerAddress,
			ServerAddress toServerAddress){
		this.initiator = initiatorServerAddress.getServerNumber();
		this.from = fromServerAddress.getServerNumber();
		this.to = toServerAddress.getServerNumber();
	}
	
	public CMHMessage(int initiator, int from, int to){
		this.initiator = initiator;
		this.from = from;
		this.to = to;
	}
	/**
	 * Prepares a JSON message
	 * @return JSON message string
	 */
	public String getJSONMessage(){
		String method = "processMessage";
		Map<String,Object> params = new HashMap<String,Object>();
		params.put("initiator", initiator);
		params.put("from", from);
		params.put("to", to);
		JSONRPC2Notification req = new JSONRPC2Notification(method, params);
		return req.toString();
	}
	
	public void sendMessage(){
		
	}
}
