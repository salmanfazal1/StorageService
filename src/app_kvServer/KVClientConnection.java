package app_kvServer;

import java.io.*;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.security.*;

//import app_kvServer.Cache;
//import app_kvServer.CacheValue;

import java.io.Serializable;

//import java.nio.file;
//import logger.LogSetup;

import org.apache.log4j.*;
import common.messages.KVMessage;

import app_kvServer.Memory;
import app_kvServer.CacheValue;

import java.util.*;


/**
 * Represents a connection end point for a particular client that is 
 * connected to the server. This class is responsible for message reception 
 * and sending. 
 * The class also implements the echo functionality. Thus whenever a message 
 * is received it is going to be echoed back to the client.
 */
public class KVClientConnection implements Runnable {

	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
		
	private static Logger logger = Logger.getRootLogger();
	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;
	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	boolean adminPriveleges;

	public static boolean serverDropped;

	private int inputreads;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * @param clientSocket the Socket object for the client connection.
	 */
	public KVClientConnection(Socket clientSocket) { //, Cache mainCache) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		inputreads=0;	
		serverDropped = false;	
	}
	

	/**
	 * GET
	 */

	public String get(String key) {

		
		String getValue = new String();
		//PUT REQUEST : Put into a data structure.
		String fileName = "Data/" +KVServer.thisServerLocation + "/" + key + ".txt";
		
		try {

			File varTmpDir = new File(fileName);
			boolean exists = varTmpDir.exists();
			if (!exists) throw new IOException();

			BufferedReader br = new BufferedReader(new FileReader(fileName));
			try {
			    StringBuilder sb = new StringBuilder();
			    String line = br.readLine();

			    while (line != null) {
			        sb.append(line);
			        sb.append(System.lineSeparator());
			        line = br.readLine();
			    }
			    String everything = sb.toString();
			    getValue = everything;
			} finally {
			    br.close();
			}
			getValue = getValue.trim();
			return getValue;	

		}	catch (IOException x) {

			return null;	
		}

	}


	/**
	 * PUT
	 */
	public KVMessage.StatusType put (String key, String value) {
		logger.info ("Server is putting: " + key + " " + value);
		/*
		KVServer.mainMemory.put( key, value, md5Hash(key) );
		return  KVMessage.StatusType.PUT_SUCCESS;
		*/
		
		String directoryName = new String(KVServer.thisServerLocation);
		directoryName = "Data/" + directoryName;
		File directory = new File(String.valueOf(directoryName));
	    if (! directory.exists()){
	        directory.mkdirs();
	        // If you require it to make the entire directory path including parents,
	        // use directory.mkdirs(); here instead.
	    }

		String fileName ="Data/" + KVServer.thisServerLocation + "/" + key + ".txt";

		// Delete Request.
    	if (value.equals("") || value == null || value.equals("null")) {

    		File varTmpDir = new File(fileName);
			boolean exists = varTmpDir.delete();
		    if (exists) {
		    	return KVMessage.StatusType.DELETE_SUCCESS;
			} else {
		    	return KVMessage.StatusType.DELETE_ERROR;
			}

		}	
		else {
			try {								
	        	File varTmpDir = new File(fileName);
				boolean exists = varTmpDir.exists();

	            FileWriter fileWriter = new FileWriter(fileName);
	        
	            // Always wrap FileWriter in BufferedWriter.
	            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

	            // Note that write() does not automatically
	            // append a newline character.
	            bufferedWriter.write(value+".");

	            // Always close files.
	            bufferedWriter.close();

	            if (exists) {
					return KVMessage.StatusType.PUT_UPDATE;
				} else {
					return KVMessage.StatusType.PUT_SUCCESS;
				}
			} catch (Exception putException) {
				String currentDir = System.getProperty("user.dir");
        System.out.println("Current dir using System:" +currentDir);
				//CATCH ALL EXCEPTIONS.
				return KVMessage.StatusType.PUT_ERROR;
			}

		}
		
	}

	/**
	 * Initializes and starts the client connection. 
	 * Loops until the connection is closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
			
			sendMessage(new KVMessageServer(
					"Connection to MSRG Echo server established: " 
					+ clientSocket.getLocalAddress() + " / "
					+ clientSocket.getLocalPort()));
					
			
			while(isOpen) {
				inputreads++;
				//if (inputreads == 25 ) { input.close(); input.open(); }
				try {
					if (adminPriveleges && serverDropped) {
						byte heartBeat = 4;
						output.write(heartBeat);	
						output.flush();	
					}
					byte read = (byte) input.read();
					//logger.info("Receiving BYTE:"+ read);
					//if ( read == 13) continue;
					if (  read == -1  ) {
						throw new IOException();
					}
					if (read == 0) {
						adminPriveleges =true;
						//System.out.println ("Receiving Admin Message.");
						//logger.info("Receiving Admin Message.");
						receiveAdminMessage();

					} else if (read == 2){
						System.out.println ("Receiving Server Message.");
						//logger.info("Receiving Server Message.");
						receiveServerMessage(input);
						
					} else if (read == 3 ) {
						while ( input.available() > 0 ) { read = (byte) input.read(); }
						// heartbeat message.	
						byte heartBeat = 0;
						output.write(heartBeat);	
						output.flush();		
					}
					else {//if(read == 1 ) {
						KVMessageServer reply;
						//System.out.println ("Receiving Client Message.");
						//logger.info("Receiving Client Message.");
						
						KVMessageServer latestMsg = receiveMessage(read);
						//logger.info("Received Message.");
						if ( latestMsg != null && KVServer.stopped == true) {
									KVMessage.StatusType replyStatus = KVMessage.StatusType.SERVER_STOPPED;
									reply = new KVMessageServer("nokey", "novalue", replyStatus);
									sendMessage(reply);
						} else {
							KVMessage.StatusType latestMsgStatus = latestMsg.getStatus();
							String latestMsgKey = latestMsg.getKey();
							String latestMsgValue = latestMsg.getValue();

							
							switch (latestMsgStatus) {
								case SERVER_WRITE_LOCK:
										reply = new KVMessageServer("",KVServer.metaDataString, latestMsgStatus);	
										sendMessage(reply);
										break;
								case SERVER_NOT_RESPONSIBLE:
										reply = new KVMessageServer("",KVServer.metaDataString, latestMsgStatus);
										sendMessage(reply);
									break;
								case PUT:
									putToReplicas( latestMsgKey, latestMsgValue );
									KVMessage.StatusType replyStatus = put(latestMsgKey, latestMsgValue);
									reply = new KVMessageServer(latestMsgKey, latestMsgValue, replyStatus);
									sendMessage(reply);
									break;
								case GET:
									String recValue = get(latestMsgKey);
									if (recValue == null) {
										reply = new KVMessageServer("", "", KVMessage.StatusType.GET_ERROR);
									} else {
										reply = new KVMessageServer("", recValue, KVMessage.StatusType.GET_SUCCESS);	
									}
									sendMessage(reply);
									break;
								default:
									//Nothing to do here.
									break;
							}
						}
					}
				/* connection either terminated by the client or lost due to 
				 * network problems*/	
				} catch (IOException ioe) {
					logger.info("Exception thrown.");
					//logger.error("Error! Connection lost!");
					isOpen = false;
					if (clientSocket != null) {
						input.close();
						output.close();
						clientSocket.close();
					}
					return;
				}				
			}
			
		} catch (IOException ioe) {
			logger.info("Exception thrown.");
			//logger.error("Error! Connection could not be established!", ioe);
			
		} finally {
			
			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.info("Exception thrown.");
				//logger.error("Error! Unable to tear down connection!", ioe);
			}
		}

		logger.info("Client Connection Closed.");
	}
	
	/**
	 * Method sends a TextMessage using this socket.
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream 
	 */
	public void sendMessage(KVMessageServer msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("SEND \t<" 
				+ clientSocket.getInetAddress().getHostAddress() + ":" 
				+ clientSocket.getPort() + ">: '" 
				+ msg.getMsg() +"'");
    }
	


	/**
	 * Method receives message and creates KVAdmin Object
	 *
	 */
	private void receiveAdminMessage() throws IOException {
		int i=0;
		byte read = (byte) input.read();
		byte[][] statusBytes = new byte[4][BUFFER_SIZE];
		int index = 0;
		int objCount = 0;

		String command  = new String();
		String meta		= new String();
		int cacheSize = 0;
		String replacementStrategy = new String();

		while ( read != LINE_FEED ) {

			if (index == BUFFER_SIZE ) break;

			statusBytes [objCount][index] = read;
			read = (byte) input.read();
			//logger.info ( " READING: " +  (char) read );
			if ( read == 37 ) {
				//logger.info ( "GOT %" );
				switch (objCount) {
					case 0:	
						command = new String (statusBytes[objCount]) . trim();
						//logger.info ("Command:"+command);
						break;
					case 1:	
						meta = new String (statusBytes[objCount]) . trim();
						//logger.info ("meta:"+meta);
						break;
					case 2:	
						String tmp = new String (statusBytes[objCount]) . trim();
						//logger.info ("tmp:"+tmp);
						cacheSize = Integer.parseInt(tmp);
						break;	
				}
				objCount++;
				//logger.info("objCount++" + objCount);
				read = (byte) input.read();
				index = -1;
			}
			index++;	
		}

		//logger.info("Received Message");

		replacementStrategy = new String (statusBytes[objCount]) . trim();

		while (input.available() > 0 ) {
				read = (byte) input.read();
		}
	
		//logger.info ("Command is:" +  command );
		switch(command) {
			case "init":
				KVServer.initKVServer(meta, cacheSize, replacementStrategy);
			break;
			case "start":
				KVServer.startServer();
			break;
			case "stop":
				KVServer.stopServer();
			break;
			case "shutdown":
				KVServer.shutdown();
			break;
			case "lockWrite":
				KVServer.writeLocked = true;
			break;
			case "unlockwrite":
			break;
			case "moveData":

				String [] moveDetails = replacementStrategy.split("\\s+");
				String fromServer = moveDetails[0];
				String toServer = moveDetails[1];

				logger.info ("Command is to move from " + fromServer + " to " + toServer);
				KVServer.moveData(fromServer, toServer);
 
			break;
			case "update":
				System.out.println("Command is Update");
				KVServer.update(meta);
			break;
		}
	}

	/**
	 * Method receives message and creates KVMessageServer Object
	 *
	 */
	private KVMessageServer receiveMessage(byte gotByte) throws IOException {

		byte[] putBytes =  KVMessage.StatusType.PUT.name().getBytes();

	    String key = new String();
		String value = new String();
		byte[] ctrBytes = new byte[]{10, 13};
		/*Fixed length for now.*/
		byte[] statusBytes = new byte[BUFFER_SIZE];
		int index = 0;

		//byte read = (byte) input.read();
		byte read = gotByte;		
		boolean reading = true;
		
		String []request = new String[3];
		int requestIndex=0;

		while ( read != 10 ) {
			if (index >= BUFFER_SIZE) break;
			statusBytes[index] =  read;
			if (read == 32 && requestIndex<2) {
				statusBytes[index] = '\0';
				request[requestIndex] = new String(statusBytes).trim();
				index = 0;
				statusBytes = new byte[100];
				requestIndex++; 			
			}
			read = (byte) input.read();
			index++;
		}
		read = (byte) input.read();
		//read = (byte) input.read();

		request[requestIndex] = new String(statusBytes).trim();
		//System.out.println("Request:" + request[0] + "Key:" + request[1] + "Value" + request[2] );

		//IMPROVE THIS
		KVMessage.StatusType receivedStatus = KVMessage.StatusType.PUT;
		switch (request[0]) {
			case "PUT":
				key = request[1];
				value = request[2];
				if (KVServer.writeLocked == true) {
					//logger.info("Server is locked to put");
					value = KVServer.metaDataString;
					receivedStatus = KVMessage.StatusType.SERVER_WRITE_LOCK;
				}
				else if ( serverRepsonsible(key) == false) {
					value = KVServer.metaDataString;
					receivedStatus = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
				} else {
					receivedStatus = KVMessage.StatusType.PUT; 
					//logger.info ("Will put: " + key + " " + value);
				}
				//System.out.println("Received a PUT request");
			break;
			case "GET":
				key = request[1];
				value =  "";
				if (serverRepsonsible(key) == false) {
					value = KVServer.metaDataString;
					receivedStatus = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
				} else {
					receivedStatus = KVMessage.StatusType.GET;
				}
				//System.out.println("Received a GET request");
			break;
			case "PUT_SUCCESS":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.PUT_SUCCESS;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "PUT_UPDATE":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.PUT_UPDATE;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "PUT_ERROR":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.PUT_ERROR;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "DELETE_SUCCESS":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.DELETE_SUCCESS;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "DELETE_ERROR":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.DELETE_ERROR;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "GET_SUCCESS":
				key = ""; 
				value = request[2];
				receivedStatus = KVMessage.StatusType.GET_SUCCESS;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "GET_ERROR":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.GET_ERROR;
				//System.out.println("Received PUT_SUCCESS");

			break;
		
		}

		KVMessageServer tmp = new KVMessageServer(key, value, receivedStatus);
		return tmp;
    }


    public boolean serverRepsonsible (String key) {

    	String hashkey = md5Hash (key);
    	String start = KVServer.startLocation;
    	String end = KVServer.thisServerLocation;

    	//logger.info("Checking Responsibility: Key:" + hashkey + " start: " +start  + " end:" + end);

    	if ( start.compareTo(end) < 0 ) {
	    	if ( hashkey.compareTo(start) >0 &&  hashkey.compareTo(end) <= 0) {
				//logger.info("Responsible.");
	    		return true;
	    	}
    	} else {
    		if ( hashkey.compareTo(start) >0 ||  hashkey.compareTo(end) <= 0) {
									//logger.info("Responsible.");
    			return true;
    		}
    	}
				//logger.info("IrResponsible.");
    	return false;

    }

    /* Hashing*/


	public String md5Hash(String key) {
		char[] hexArray = "0123456789ABCDEF".toCharArray();
		String plaintext = key;
		try {
			MessageDigest m = MessageDigest.getInstance("MD5");
			m.reset();
			m.update(plaintext.getBytes());
			byte[] digest = m.digest();
			char[] hexChars = new char[digest.length * 2];
		    for ( int j = 0; j < digest.length; j++ ) {
		        int v = digest[j] & 0xFF;
		        hexChars[j * 2] = hexArray[v >>> 4];
		        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		    }
		    String hashtext =  new String(hexChars);
			//String hashtext = bytesToHex(digest);//digest.toString();
			//hashtext = hashtext.toHexString();
			// Now we need to zero pad it if you actually want the full 32 chars.
			while(hashtext.length() < 32 ){
			  hashtext = "0"+hashtext;
			}

			return hashtext;
		} catch (Exception x) {
			return "";
		}
	}



	public void putToReplicas (String key, String value) {
		
		
		String myLocationInRing = KVServer.thisServerLocation;
		int locationIndex = 0;
		int ii = 0;
		
		for (String randoml: KVServer.metaData.keySet() ) {
			if ( randoml.equals(myLocationInRing) ) {
				locationIndex = ii; 
			}
			ii++;
		}
		
		int current =  ( locationIndex) % KVServer.metaData.size()  ; //new String();
		int nextOne =  ( locationIndex + 1 ) % KVServer.metaData.size()  ; //new String();
		int nextTwo =  ( locationIndex + 2 ) % KVServer.metaData.size()  ; //new String(); 
		
		String keyCurrent = KVServer.metaData.keySet().toArray(new String[KVServer.metaData.size()])[current];
		String keyNextOne = KVServer.metaData.keySet().toArray(new String[KVServer.metaData.size()])[nextOne];
		String keyNextNextOne = KVServer.metaData.keySet().toArray(new String[KVServer.metaData.size()])[nextTwo];
		
		
		String currentAddress = KVServer.metaData.get(keyCurrent) ;
 
		String addressOne = KVServer.metaData.get(keyNextOne).split(":") [0] ;
		int portOne =  Integer.parseInt(KVServer.metaData.get(keyNextOne).split(":") [1] );
		
	
		logger.info("Next address in the ring:" + addressOne);
		logger.info("port is: " + portOne);
		
		
		String addressTwo = KVServer.metaData.get(keyNextNextOne).split(":") [0] ;
		int portTwo =  Integer.parseInt(KVServer.metaData.get(keyNextNextOne).split(":") [1] );
		
		logger.info("Next address in the ring:" + addressTwo);
		logger.info("port is: " + portTwo);
		
				
		KVMessage.StatusType MsgStatusReplica1 = null;
		KVMessage.StatusType MsgStatusReplica2 = null;
		try {
			logger.info("Sending put request to the 1st replica");
			MsgStatusReplica1 = sendMSGtoreplica(addressOne, portOne, key, value, currentAddress);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			logger.info("Sending put request to the 2nd replica");
			MsgStatusReplica2 =	sendMSGtoreplica(addressTwo, portTwo, key, value, currentAddress);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		//	
		
		//	recvMSG
		
		//	sendMSG(replica1, key, value);
		//	sendMSG(replica2, key, value);
		
		//	receivereplyfrombothreplicas();
		
		
		if(MsgStatusReplica1 == KVMessage.StatusType.REPLICA_DELETE_ERROR || MsgStatusReplica1 == KVMessage.StatusType.REPLICA_PUT_ERROR){
			
			logger.info("Replica 1 failed to put or delete the key/value");
			
		}

		if(MsgStatusReplica2 == KVMessage.StatusType.REPLICA_DELETE_ERROR || MsgStatusReplica2 == KVMessage.StatusType.REPLICA_PUT_ERROR){
			
			logger.info("Replica 2 failed to put or delete the key/value");
			
		}
		//put more checks here for the returned msg
		
		



	}



	public KVMessage.StatusType sendMSGtoreplica(String replicaAddress,int  replicaPort,String key, String value, String currentAddress) 
	throws Exception{
		
		//create a new connection to the replica server. The Replica server will create a new thread for this connection 	
		logger.info("Server: Creating a connection to the replica Server");
		Socket replicaSocket = new Socket(replicaAddress, replicaPort);  
		byte serverMessageByte = 2;
		
		
		//define input and output streams for the replica_socket

		OutputStream replicaOut = replicaSocket.getOutputStream();
		InputStream replicaIn =  replicaSocket.getInputStream();
		
		
		//construct sendMessage;

		 //let the server know that the message came from another server. We do this by embedding a byte=2 for the start of the msg
		replicaOut.write(serverMessageByte);                        
		replicaOut.flush();
		
		//send the put request to the replica.
		logger.info("Server: sending put msg to replica:" + replicaAddress + ":" + replicaPort);
		
		String coordinatorIP_and_port= currentAddress;
		
		//Create another KVMessageCLient constructor which adds coordinator's IP and port as a string
		key= coordinatorIP_and_port + '/' + key;
		
		KVMessageServer msg = new KVMessageServer(key, value, KVMessage.StatusType.REPLICA_PUT);
		byte[] msgBytes = msg.getMsgBytes();
		replicaOut.write(msgBytes, 0, msgBytes.length);
		
		//Now we must wait for a reply from the server
		
		logger.info("Server: Going into receive function to get a reply from Replica server");
		KVMessageServer latestMsg = receiveServerMessage(replicaIn);
			
		KVMessage.StatusType latestMsgStatus = latestMsg.getStatus();
		
		//OutputStream replicaOut = replicaSocket.getOutputStream();
		logger.info("Server: Got a reply from Replica server Now going to return back to caller function");
		
		return latestMsgStatus;
	}




	private KVMessageServer receiveServerMessage(InputStream stream) throws IOException {

		byte[] putBytes =  KVMessage.StatusType.PUT.name().getBytes();

	    String key = new String();
		String value = new String();
		String currAddress = new String();

		KVMessageServer reply;

		byte[] ctrBytes = new byte[]{10, 13};
		/*Fixed length for now.*/
		byte[] statusBytes = new byte[BUFFER_SIZE];
		
		int index = 0;
		byte read = (byte) stream.read();
		boolean reading = true;

		logger.info("Server Inside receiveServerMEssage and waiting to receive Bytes from another server");
		
		String []request = new String[3];
		int requestIndex=0;

		while ( read != 10 ) {
			if (index >= BUFFER_SIZE) break;
			statusBytes[index] =  read;
			if (read == 32 && requestIndex<2) {
				statusBytes[index] = '\0';
				request[requestIndex] = new String(statusBytes).trim();
				index = 0;
				statusBytes = new byte[100];
				requestIndex++; 			
			}
			read = (byte) stream.read();
			index++;
		}
		
		request[requestIndex] = new String(statusBytes).trim();
		//System.out.println("Request:" + request[0] + "Key:" + request[1] + "Value" + request[2] );

		//IMPROVE THIS
		KVMessage.StatusType receivedStatus = KVMessage.StatusType.PUT;
		
		logger.info("Server: Inside receiveServerMessage() Server received the following command from another Server: " + request[0]);
	//	logger.info("Server: The 3rd argument (coordinator address) is: " + request[3]);
		
		switch (request[0]) {

		case "REPLICA_PUT":

			key = request[1];
			
			//extract the coordinator address from the key. Its embedded like this:  address + '/' + key
			String coordinator_address= new String();
			int k=0;
			
			for (String retval: key.split("/")) {
		        if(k==0){
		        	coordinator_address = retval;
		        } 
		        if(k==1){
		        	key = retval;
		        } 
		        
				k++;
		      }
			
			logger.info("The key received is: " +key);
			
			logger.info("The coordinator address received is: "+ coordinator_address);
			
			
			value = request[2];
		//	currAddress = request[3];
			//receivedStatus = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;

			logger.info("Server: Replica received the REPLICA_PUT command");
			logger.info("Server: Replica server calling the REPLICA_PUT function");
			System.out.println("Command is Replica put");
			KVMessage.StatusType receiveStatus = replica_put(key, value, coordinator_address);
			
			logger.info("Received ");
			
 			reply = new KVMessageServer("", "", receiveStatus);
 			logger.info("Server: Replica server returned the following status from the replica_put function: " + receiveStatus.toString());
 			logger.info("Server: Replica server now sending this status to the coordinator server");
 			sendMessage(reply);
			
			//Shutdown socket after this for the replica server because connection is no longer needed

		break;
		case "CHECK_HEARTBEAT":
			System.out.println("Command is check_heartbeat");
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		case "HEARTBEAT_OK":
			System.out.println("Command is HEARTBEAT_OK");
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		case "REPLICA_PUT_SUCCESS":
			System.out.println("Command is REPLICA_PUT_SUCCESS");
			key = "";
			value = "";
			receivedStatus = KVMessage.StatusType.REPLICA_PUT_SUCCESS;
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		case "REPLICA_PUT_UPDATE":
			System.out.println("Command is REPLICA_PUT_UPDATE");
			key = "";
            value = "";
            receivedStatus = KVMessage.StatusType.REPLICA_PUT_UPDATE;
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		case "REPLICA_DELETE_ERROR":
			System.out.println("Command is REPLICA_DELETE_ERROR");
			key = "";
            value = "";
            receivedStatus = KVMessage.StatusType.REPLICA_DELETE_ERROR;
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		case "REPLICA_DELETE_SUCCESS":
			System.out.println("Command is REPLICA_DELETE_SUCCESS");
			key = "";
            value = "";
            receivedStatus = KVMessage.StatusType.REPLICA_DELETE_ERROR;
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		case "REPLICA_GET_SUCCESS":
			System.out.println("Command is REPLICA_GET_SUCCESS");
			key = "";
            value = "";
            receivedStatus = KVMessage.StatusType.REPLICA_GET_SUCCESS;
			//KVMessage.StatusType receivedStatus = replica_put(key, value, coordinator_hostname);
		break;
		
		
		}
		
		logger.info("Server: leaving the receiveServerMessage function");
		KVMessageServer tmp = new KVMessageServer(key, value, receivedStatus);
		return tmp;
    }



			public KVMessage.StatusType replica_put(String key, String value, String coordinator_address) {
					String directoryName = new String(KVServer.thisServerLocation);
					directoryName = "Data/" + directoryName + "/replica-" + coordinator_address;         //the dir name should be Data/thiserverlocation/hostnameofcoordinator 
					File directory = new File(String.valueOf(directoryName));
					if (! directory.exists()){
						directory.mkdirs();
						// If you require it to make the entire directory path including parents,
						// use directory.mkdirs(); here instead.
					}

					String fileName ="Data/" + KVServer.thisServerLocation + "/replica-" + coordinator_address + "/" + key + ".txt";

					// Delete Request.
					if (value.equals("") || value == null || value.equals("null")) {

						File varTmpDir = new File(fileName);
						boolean exists = varTmpDir.delete();
						if (exists) {
							return KVMessage.StatusType.REPLICA_DELETE_SUCCESS;
						} else {
							return KVMessage.StatusType.REPLICA_DELETE_ERROR;
						}

					}	
					else {
						try {								
							File varTmpDir = new File(fileName);
							boolean exists = varTmpDir.exists();

							FileWriter fileWriter = new FileWriter(fileName);
					
							// Always wrap FileWriter in BufferedWriter.
							BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

							// Note that write() does not automatically
							// append a newline character.
							bufferedWriter.write(value);

							// Always close files.
							bufferedWriter.close();

							if (exists) {
								return KVMessage.StatusType.REPLICA_PUT_UPDATE;
							} else {
								return KVMessage.StatusType.REPLICA_PUT_SUCCESS;
							}
						} catch (Exception putException) {
							String currentDir = System.getProperty("user.dir");
					System.out.println("Current dir using System:" +currentDir);
							//CATCH ALL EXCEPTIONS.
							return KVMessage.StatusType.REPLICA_PUT_ERROR;
						}

					}
				}
	
	
/*
public String get(String key)  {
String getValue = new String();
//PUT REQUEST : Put into a data structure.
String fileName = "Data/" +KVServer.thisServerLocation + "/" + key + ".txt";
String [] Dirs= new String[5];
try {

File varTmpDir = new File(fileName);
boolean exists = varTmpDir.exists();
boolean exists1 = true;
boolean exists2 = true;
if (exists){

BufferedReader br = new BufferedReader(new FileReader(fileName));
try {
   StringBuilder sb = new StringBuilder();
   String line = br.readLine();

   while (line != null) {
       sb.append(line);
       sb.append(System.lineSeparator());
       line = br.readLine();
   }
   String everything = sb.toString();
   getValue = everything;
} finally {
   br.close();
}
getValue = getValue.trim();
return getValue;
}  //end of ifexists
else{ //else check the replica folders
logger.info("Checking Replica Folders");
File folder = new File(fileName);
File[] listOfFiles = folder.listFiles();

   for (int i = 0; i < listOfFiles.length; i++) {
   
    if (listOfFiles[i].isDirectory()) {
     
    Dirs[i]= listOfFiles[i].getName();
     }
   
   }
if(Dirs[0].contains("replica")){
String filename2 = "Data/" +KVServer.thisServerLocation + "/" + Dirs[0] + "/" + key + ".txt";   
File temp1 = new File(filename2);
exists1 = varTmpDir.exists();
if (exists1){

BufferedReader br = new BufferedReader(new FileReader(fileName));
try {
   StringBuilder sb = new StringBuilder();
   String line = br.readLine();

   while (line != null) {
       sb.append(line);
       sb.append(System.lineSeparator());
       line = br.readLine();
   }
   String everything = sb.toString();
   getValue = everything;
} finally {
   br.close();
}
getValue = getValue.trim();
return getValue;
}  //end of ifexists
}
else{
logger.info("1st REplica directory Not created Yet");
}
if(Dirs[1].contains("replica")){
String filename3 = "Data/" +KVServer.thisServerLocation + "/" + Dirs[1] + "/" + key + ".txt";    
File temp1 = new File(filename3);
exists2 = varTmpDir.exists();
if (exists2){

BufferedReader br = new BufferedReader(new FileReader(fileName));
try {
   StringBuilder sb = new StringBuilder();
   String line = br.readLine();

   while (line != null) {
       sb.append(line);
       sb.append(System.lineSeparator());
       line = br.readLine();
   }
   String everything = sb.toString();
   getValue = everything;
} finally {
   br.close();
}
getValue = getValue.trim();
return getValue;
}  //end of ifexists
}
else{
logger.info("2nd REplica directory Not created Yet");
}
logger.info("Performing Get operation: Checking all 3 folders for the key");
}
if(!exists && !exists1 && !exists2) throw new IOException();

} catch (IOException x) {
return null; 

}
return null; 
}

*/
/*
public boolean serverRepsonsibleforGet(String key) {
   
   
   
    String hashkey = md5Hash (key);
   
    String end = KVServer.thisServerLocation;
   
   
   
   
    String before =new String();
    String beforeBefore = new String();
    String beforeBeforeBefore = new String();
   
    if ( !end.equals( KVServer.metaData.firstKey()) ) {
    before = KVServer.metaData.lowerKey(end); 
    if ( !before.equals ( KVServer.metaData.firstKey() ))  {
    beforeBefore = KVServer.metaData.lowerKey(before);
    if (!beforeBefore.equals(KVServer.metaData.firstKey())) {
    beforeBeforeBefore = KVServer.metaData.lowerKey(beforeBefore);
    }
    else {
    beforeBeforeBefore = KVServer.metaData.lastKey();
    }
    } else {
    beforeBefore = KVServer.metaData.lastKey();
    beforeBeforeBefore = KVServer.metaData.lowerKey(beforeBefore);
    }
    } else {
    before = KVServer.metaData.lastKey();
    beforeBefore = KVServer.metaData.lowerKey(before);
    beforeBeforeBefore = KVServer.metaData.lowerKey(beforeBefore);
    } 
   
   
   
    String start = beforeBeforeBefore;  //assigning the start range to be the hash value of the node 2 places before on the ring with respect to the current node
   
    logger.info("Checking Responsibility for GET: Key:" + hashkey + " start: " +start  + " end:" + end);

    if ( start.compareTo(end) < 0 ) {
    if ( hashkey.compareTo(start) >0 &&  hashkey.compareTo(end) <= 0) {
    return true;
    }
    } else {
    if ( hashkey.compareTo(start) >0 ||  hashkey.compareTo(end) <= 0) {
    return true;
    }
    }
    return false;

    }
    /* Hashing*/





	
}
