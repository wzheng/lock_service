import java.io.*;
import java.util.*;
import java.util.Map.Entry;

import com.thetransactioncompany.jsonrpc2.*;

/**
 * This class takes care of initiating partition transfers One server is the
 * master that takes care of reconfiguring
 */

public class PartitionUpdater implements Runnable {

	private Server server;
	private ServerAddress thisSA;
	private CommunicationQ queue;
	private ReconfigState reconfigState;
	private boolean isMaster;

	private HashMap<Integer, ServerAddress> servers;

	public PartitionUpdater(Server server, CommunicationQ queue) {
		this.server = server;
		this.queue = queue;
		reconfigState = server.getReconfigState();
		this.isMaster = server.isMaster();
		this.servers = server.getAllServers();
		thisSA = server.getAddress();
	}

	// run the reconfiguration algorithm
	public HashMap<Integer, ServerAddress> configAlgo(HashMap<Pair, Integer> input) {
		// v.1: get the heaviest edge for now
		int max = 0;
		Pair max_pair = null;
		Iterator it = input.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry kv = (Map.Entry) it.next();
			int temp = ((Integer) kv.getValue()).intValue();
			if (max < temp) {
				max = temp;
				max_pair = (Pair) kv.getKey();
			}
		}

		HashMap<Integer, ServerAddress> ret = new HashMap<Integer, ServerAddress>();
		if (max_pair != null) {
			// TODO: calculate a list of partition -> new server mapping
		}

	}

	// master reconfiguration
	public void configureMaster() {

		// periodically contact all ther servers for AF information

		while (true) {

			Iterator<Entry<Integer, ServerAddress>> it = servers.entrySet()
					.iterator();
			HashSet<ServerAddress> waitAddresses = new HashSet<ServerAddress>();
			while (it.hasNext()) {
				Map.Entry<Integer, ServerAddress> kv = it.next();
				if (!kv.getValue().equals(thisSA)) {
					waitAddresses.add((ServerAddress) kv.getValue());
					HashMap<String, Object> args = new HashMap<String, Object>();
					args.put("Method", "getAF");
					RPCRequest req = new RPCRequest("reconfigure", thisSA,
							new TransactionId(thisSA, -1), args);
					RPC.send((ServerAddress) kv.getValue(), "reconfigure",
							"001", req.toJSONObject());
				}
			}

			HashMap<Pair, Integer> newAFTable = new HashMap<Pair, Integer>();

			while (!waitAddresses.isEmpty()) {
				Object obj = queue.get();
				if (obj.equals("")) {
					continue;
				}
				RPCRequest reqIn = (RPCRequest) obj;
				HashMap<String, Object> replyArgs = (HashMap<String, Object>) reqIn.args;
				if (replyArgs.get("Method").equals("getAF-reply")) {
					Iterator ra_it = replyArgs.iterator();
					while (ra_it.hasNext()) {
						// aggregate the edge weights together
						Pair newPair = new Pair((Integer) ra_it.get("p1"),
								(Integer) ra_it.get("p2"));
						Integer i = newAFTable.get(newPair);
						if (i == null) {
							newAFTable.put(newPair, (Integer) ra_it.get("af"));
						} else {
							newAFTable.put(newPair, (Integer) ra_it.get("af")
									+ i);
						}
						waitAddress.remove(reqIn.replyAddress);
					}
				}
			}

			PartitionTable newTable = this.configAlgo(newAFTable);

			if (newTable != null) {
				// a new configuration is available, send to all of the servers

			}
		}

	}

	// worker reconfiguration
	public void configureWorker(JSONRPC2Request request) {

		while (true) {

			Object obj = queue.get();
			if (obj.equals("")) {
				continue;
			}

			RPCRequest reqIn = (RPCRequest) obj;
			HashMap<String, Object> args = (HashMap<String, Object>) obj.args;
			if (args.get("Method").equals("getAF")) {

				AFTable af = this.server.getAF();
				HashMap<String, Object> args = af.toJSONObject();
				args.put("Method", "getAF-reply");
				RPCRequest retReq = new RPCRequest("reconfigure", thisSA,
						reqIn.tid, args);
				RPC.send(reqIn.replyAddress, "reconfigure", "001",
						retReq.toJSONObject());

			} else if (args.get("Method").equals("changeConfig")) {
				this.server.set(ReconfigState.PREPARE);
			}
		}
	}

	public void run() {

		if (this.isMaster) {
			this.configureMaster();
		} else {
			this.configureWorker();
		}

	}

}
