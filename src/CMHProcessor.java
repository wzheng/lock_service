import java.util.HashSet;
import java.util.Set;

import com.thetransactioncompany.jsonrpc2.*;
import com.thetransactioncompany.jsonrpc2.util.*;

/**
 * Class for processing Chandy-Misra-Haas messages
 * @author ben
 *
 */
public class CMHProcessor {
	private ServerAddress currentServer;
	private int initiatorServerNumber;
	private int fromServerNumber;
	private int toServerNumber;
	private Set<CMHMessage> messagesToSend;
	private Set<ServerAddress> nextServerAddresses;
	public CMHProcessor(ServerAddress currentServer){
		this.currentServer = currentServer;
		this.messagesToSend = new HashSet<CMHMessage>();
		this.nextServerAddresses = new HashSet<ServerAddress>();
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
			initiatorServerNumber = np.getInt("initiator");
			fromServerNumber = np.getInt("from");
			toServerNumber = np.getInt("to");
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
		if (initiatorServerNumber == toServerNumber){
			return true;
		}
		// do some stuff to send out message here
		
		// figure out which processes are holding locks to resources it's requesting
		
		// if it needs to send message to next resource:
		if (true) {
			for (ServerAddress nextServerAddress : nextServerAddresses){
				CMHMessage nextMessage = new CMHMessage(initiatorServerNumber, toServerNumber, nextServerAddress.getServerNumber());
				nextMessage.sendMessage();
			}
		}
		return false;
	}
}
