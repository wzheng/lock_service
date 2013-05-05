import com.thetransactioncompany.jsonrpc2.*;
import com.thetransactioncompany.jsonrpc2.util.*;

/**
 * Class for processing Chandy-Misra-Haas messages
 * @author ben
 *
 */
public class CMHProcessor {
	private ServerAddress currentServer;
	private int initiator;
	private int from;
	private int to;
	public CMHProcessor(ServerAddress currentServer){
		this.currentServer = currentServer;
	}
	public void processMessage(String message){
		JSONRPC2Notification req = null;
		try {
			req = JSONRPC2Notification.parse(message);


		} catch (JSONRPC2ParseException e) {
			e.printStackTrace();
		}
		
		NamedParamsRetriever np = new NamedParamsRetriever(req.getNamedParams());
		try {
			initiator = np.getInt("initiator");
			from = np.getInt("from");
			to = np.getInt("to");
		} catch (JSONRPC2Error e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Determines if a deadlock exists by checking the initiator and to fields
	 * Sends a message if no deadlock detected but is still waiting for resources
	 * @return true if deadlock exists
	 */
	public boolean detectDeadlock(){
		if (initiator == to){
			return true;
		}
		// do some stuff to send out message here
		
		// figure out which processes are holding locks to resources it's requesting
		
		//if needs to send message to next resource:
		if(true){
			
		}
		return false;
	}
}
