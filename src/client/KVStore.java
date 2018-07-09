package client;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import logger.LogSetup;

import client.KVMessageClient;

import common.messages.KVMessage;
import client.ClientSocketListener.SocketStatus;

import java.security.*;
import java.util.*;

public class KVStore implements KVCommInterface {


	public static String thisServerLocation;
	public static String startLocation;
	public static TreeMap<String, String> metaData;
	public static String metaDataString;

	public boolean isRunning = false;
	private Logger logger = Logger.getRootLogger();


	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;

	public Set<ClientSocketListener> listeners;	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	String address;
	int port;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		listeners = new HashSet<ClientSocketListener>();
		metaData = null;
		//connect();
		//logger.info("Connection established");
	}
	
	@Override
	public void connect() 
		throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		String fullAddress = new String ( address +":" + port );
		thisServerLocation = md5Hash (fullAddress);





		clientSocket = new Socket(address, port);
		isRunning = true;

			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		KVMessageClient connectReturn = receiveMessage ();
	}

	@Override
	public void disconnect() {
		// TODO Auto-generated method stub

		if (clientSocket != null) {
			try {
				clientSocket.close();
				clientSocket = null;
			} catch (IOException e) {

			}

		}
		logger.info("connection closed!");
		
	}

	public void resetMetaData (String metaDataIn) {
		metaData = new TreeMap<String, String> ();
		int i=0;
		String previousLocation = "last";

		String[] tokens = metaDataIn.split("\\s+");

				for (i=0; i<tokens.length;i+=2) {
					//int port = Integer.parseInt(tokens[i+1]);
					String hashedlocation = new String (tokens[i]);// + ":" + tokens[i+1]);
				
					metaData.put(tokens[i] , tokens[i+1]);
					if (hashedlocation.equals(thisServerLocation)) {
						startLocation = previousLocation;
					}
					previousLocation = hashedlocation;
				}
		if (startLocation.equals("last")) {
					startLocation = previousLocation;
		}
	}

	public void connectToCorrectServer (String key) throws UnknownHostException, IOException{

			
			String hashedKey = md5Hash (key);  
			//System.out.println("Hash of key: " + hashedKey );
			String serverHash = new String();
			boolean first = true;
			String firstKey = new String();
			String firstValue = new String();

			
			//System.out.println();


			for (Map.Entry<String, String> entry : metaData.entrySet()) {
				if (first == true ) {
					firstKey = entry.getKey();
			    	firstValue = entry.getValue();
					first = false;
				}
			    String keyMeta = entry.getKey();
			    String value = entry.getValue();
			    String [] addr = value.split(":");


			    String addressIn = addr[0];
			    int portIn = Integer.parseInt(addr [1]);

			    //System.out.println("MetaData of server:      " + keyMeta + " " +value);

			    serverHash = md5Hash (value);
			    if ( hashedKey.compareTo(serverHash) <= 0) {
			    	if ( addressIn.equals(this.address) && portIn == this.port  ) {
			    		//System.out.println("In Correct Server Already... " + hashedKey + " " + serverHash );
			    		return;
			    	}

			    	//System.out.println ();
					//System.out.println ("Connecting to server ... :" + keyMeta + " " + value );

			    	disconnect ();
			    	clientSocket = new Socket(addressIn, portIn);
					isRunning = true;
					this.address = addressIn;
					this.port = portIn;

						output = clientSocket.getOutputStream();
						input = clientSocket.getInputStream();

					thisServerLocation = serverHash;
					KVMessageClient connectReturn = receiveMessage ();

					return;
			    }
			}

				//System.out.println ();
				//System.out.println ("Connecting to server ... :" + firstKey + " " + firstValue );
 				
 				String [] addr = firstValue.split(":");

			    String addressIn = addr[0];
			    int portIn = Integer.parseInt(addr [1]);

			disconnect();
			clientSocket = new Socket(addressIn, portIn);
					this.address = addressIn;
					this.port = portIn;
					isRunning = true;

						output = clientSocket.getOutputStream();
						input = clientSocket.getInputStream();

					thisServerLocation = serverHash;
					KVMessageClient connectReturn = receiveMessage ();
			return;




	}

	@Override
	public KVMessage put(String key, String value) throws Exception {

		// if (serverResponsible ( key )  )
		//System.out.println ("Trying to put key: " + md5Hash(key) +" in server: " + md5Hash(this.address+":"+this.port));
		/*
		if (metaData != null) {
		for (Map.Entry<String, String> entry : metaData.entrySet()) {
				String keyMeta = entry.getKey();
				String valueMeta = entry.getValue();
				System.out.println("MetaData of server:      " + keyMeta + " " +valueMeta);
			}
		}
		*/
		if (metaData != null) {
			connectToCorrectServer (key);
		}


		if (key.length() > 20 || value.length() > 122880) {
			throw new IOException();
		}
		//byte clientMessageByte = 1;
		//output.write(clientMessageByte);	
		//output.flush();

		KVMessageClient msg = new KVMessageClient(key, value, KVMessage.StatusType.PUT);
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes);//, 0, msgBytes.length);
		output.flush();
		boolean receivedMessage = false;
		//System.out.println("Send PUT request.");
		KVMessage ret =receiveMessage();
		//System.out.println("Got PUT reply.");
		//System.out.println("GOT PUT REPLY");
		if (ret.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK) {
			System.out.println( "Server was locked." );
			metaDataString = ret.getValue();
			resetMetaData ( metaDataString  ); 
			
			//ret = this.put(key, value);
			return ret;
		}
		else if (ret.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			metaDataString = ret.getValue();
			resetMetaData ( metaDataString  ); 
			ret = this.put(key, value);
		}
		return ret;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		//System.out.println ("Trying to get key: " + md5Hash(key));

		if (metaData != null) {
			connectToCorrectServer (key);
		}

		//byte clientMessageByte = 1;
		//output.write(clientMessageByte);	
		//output.flush();

		KVMessageClient msg = new KVMessageClient(key, "", KVMessage.StatusType.GET);
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes);//, 0, msgBytes.length);
		output.flush();
		boolean receivedMessage = false;
		KVMessage ret =receiveMessage();
		//System.out.println( " " + ret.getStatus());
		if (ret.getStatus() == KVMessage.StatusType.SERVER_WRITE_LOCK) {
			return ret;
		}
		if (ret.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
			metaDataString = ret.getValue();
			resetMetaData ( metaDataString  ); 
			ret = this.get(key);
		}
		return ret;
		
	}

	private KVMessageClient receiveMessage() throws IOException {

		byte[] putBytes =  KVMessage.StatusType.PUT.name().getBytes();

	    String key = new String();
		String value = new String();
		byte[] ctrBytes = new byte[]{10, 13};
		/*Fixed length for now.*/
		byte[] statusBytes = new byte[BUFFER_SIZE];
		
		int index = 0;
		byte read = (byte) input.read();
		boolean reading = true;
		
		String []request = new String[3];
		int requestIndex=0;
		/*
		while ( read != 10 ) {
			if (index >= BUFFER_SIZE) break;
			statusBytes[index] =  read;
			if (read == 32 && requestIndex<2) {
				statusBytes[index] = '\0';
				request[requestIndex] = new String(statusBytes).trim();
				index = 0;
				if (request[0] == "SERVER_NOT_RESPONSIBLE")
					statusBytes = new byte[BUFFER_SIZE*20];
				else
					statusBytes = new byte[BUFFER_SIZE];
				requestIndex++; 			
			}
			read = (byte) input.read();
			index++;
		}
		*/

		while ( read != 10 ) {
			if (index >= BUFFER_SIZE) break;
			statusBytes[index] =  read;
			if (read == 32 && requestIndex<2) {
				statusBytes[index] = '\0';
				request[requestIndex] = new String(statusBytes).trim();
				index = 0;
				statusBytes = new byte[BUFFER_SIZE];
				requestIndex++; 			
			}
			read = (byte) input.read();
			index++;
		}

		request[requestIndex] = new String(statusBytes).trim();
		//System.out.println("Request:" + request[0] + "Key:" + request[1] + "Value" + request[2] );

		//IMPROVE THIS
		KVMessage.StatusType receivedStatus = KVMessage.StatusType.PUT;
		switch (request[0]) {
			case "PUT":
				key = request[1];
				value = request[2];
				receivedStatus = KVMessage.StatusType.PUT;
				//System.out.println("Received a PUT request");
			break;
			case "GET":
				key = request[1];
				value =  "";
				receivedStatus = KVMessage.StatusType.GET;
				//System.out.println("Received a GET request");
			break;
			case "PUT_SUCCESS":
				key = request[1]; 
				value = request[2];
				receivedStatus = KVMessage.StatusType.PUT_SUCCESS;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "PUT_UPDATE":
				key = request[1]; 
				value = request[2];
				receivedStatus = KVMessage.StatusType.PUT_UPDATE;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "PUT_ERROR":
				key = request[1]; 
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
			case "SERVER_STOPPED":
				key = ""; 
				value = "";
				receivedStatus = KVMessage.StatusType.SERVER_STOPPED;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "SERVER_WRITE_LOCK":
				key = ""; 
				value = request[2];
				receivedStatus = KVMessage.StatusType.SERVER_WRITE_LOCK;
				//System.out.println("Received PUT_SUCCESS");

			break;
			case "SERVER_NOT_RESPONSIBLE":
				key = ""; 
				value = request[2];
				receivedStatus = KVMessage.StatusType.SERVER_NOT_RESPONSIBLE;
				//System.out.println("Received PUT_SUCCESS");

			break;
		
		}
		
		KVMessageClient tmp = new KVMessageClient(key, value, receivedStatus);
		return tmp;
		
		
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




	
}
