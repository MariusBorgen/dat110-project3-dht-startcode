/**
 * 
 */
package no.hvl.dat110.middleware;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * @author tdoy
 *
 */
public class MutualExclusion {
		
	private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
	/** lock variables */
	private boolean CS_BUSY = false;						// indicate to be in critical section (accessing a shared resource) 
	private boolean WANTS_TO_ENTER_CS = false;				// indicate to want to enter CS
	private List<Message> queueack; 						// queue for acknowledged messages
	private List<Message> mutexqueue;						// queue for storing process that are denied permission. We really don't need this for quorum-protocol
	
	private LamportClock clock;								// lamport clock
	private Node node;
	private int requestClock = -1;
	
	public MutualExclusion(Node node) throws RemoteException {
		this.node = node;
		
		clock = new LamportClock();
		queueack = new ArrayList<Message>();
		mutexqueue = new ArrayList<Message>();
	}
	
	public synchronized void acquireLock() {
		CS_BUSY = true;
	}

	public void releaseLocks() {
		WANTS_TO_ENTER_CS = false;
		CS_BUSY = false;
		requestClock = -1;
		mutexqueue.clear();
	}

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {

		logger.info(node.nodename + " wants to access CS");

		queueack.clear();
		mutexqueue.clear();

		// increment clock
		clock.increment();
		requestClock = clock.getClock();
		message.setClock(requestClock);

		// want to enter CS
		WANTS_TO_ENTER_CS = true;

		// hent unike peers
		List<Message> activenodes = removeDuplicatePeersBeforeVoting();

		// send request
		multicastMessage(message, activenodes);

		// vent på alle ACK
		boolean permission = false;

		for (int i = 0; i < 20; i++) {
			if (areAllMessagesReturned(activenodes.size())) {
				permission = true;
				break;
			}

			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
		}
		if (permission) {
			acquireLock();
			node.broadcastUpdatetoPeers(updates);
			multicastReleaseLocks(Set.copyOf(activenodes));
		}

		return permission;
	}

	// multicast message to other processes including self
	private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {

		logger.info("Number of peers to vote = " + activenodes.size());

		for (Message m : activenodes) {

			NodeInterface stub = Util.getProcessStub(m.getNodeName(), m.getPort());

			if (stub != null) {
				stub.onMutexRequestReceived(message);
			}
		}
	}

	public void onMutexRequestReceived(Message message) throws RemoteException {

		// update clock
		int newClock = Math.max(clock.getClock(), message.getClock()) + 1;
		clock.adjustClock(newClock);

		// hvis fra seg selv → auto ACK
		if (message.getNodeName().equals(node.getNodeName())) {
			onMutexAcknowledgementReceived(message);
			return;
		}

		int caseid = -1;

		if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
			caseid = 0;
		}
		else if (CS_BUSY) {
			caseid = 1;
		}
		else if (WANTS_TO_ENTER_CS) {
			caseid = 2;
		}

		doDecisionAlgorithm(message, mutexqueue, caseid);
	}

	public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {

		String procName = message.getNodeName();
		int port = message.getPort();


		NodeInterface stub = Util.getProcessStub(procName, port);

		switch (condition) {

			case 0: {
				if (stub != null) {
					stub.onMutexAcknowledgementReceived(message);
				}
				break;
			}

			case 1: {
				queue.add(message);
				break;
			}

			case 2: {

					int senderClock = message.getClock();
					int myClock = requestClock;

					boolean senderWins = false;

					if (senderClock < myClock) {
						senderWins = true;
					} else if (senderClock == myClock) {
						senderWins = message.getNodeID().compareTo(node.getNodeID()) < 0;
					}

					if (senderWins) {
						if (stub != null) {
							stub.onMutexAcknowledgementReceived(message);
						}
					} else {
						queue.add(message);
					}

					break;
				}
			}
		}


	public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
		queueack.add(message);
	}
	
	// multicast release locks message to other processes including self
	public void multicastReleaseLocks(Set<Message> activenodes) throws RemoteException {

		logger.info("Releasing locks from = " + activenodes.size());

		for (Message m : activenodes) {
			NodeInterface stub = Util.getProcessStub(m.getNodeName(), m.getPort());
			if (stub != null) {
				stub.releaseLocks();
			}
		}
	}

	private boolean areAllMessagesReturned(int numvoters) throws RemoteException {

		logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());

		if (queueack.size() == numvoters) {
			queueack.clear();
			return true;
		}

		return false;
	}
	
	private List<Message> removeDuplicatePeersBeforeVoting() {
		
		List<Message> uniquepeer = new ArrayList<Message>();
		for(Message p : node.activenodesforfile) {
			boolean found = false;
			for(Message p1 : uniquepeer) {
				if(p.getNodeName().equals(p1.getNodeName())) {
					found = true;
					break;
				}
			}
			if(!found)
				uniquepeer.add(p);
		}		
		return uniquepeer;
	}
}
