package app_kvEcs;

import java.io.*;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import java.security.*;
import java.util.*;

import app_kvEcs.ECSConnections;

public class ECS {

	//HashMap<String, ArrayList<String>> serverNodes;
	HashMap<String, String> servers;
	HashMap<String, String> liveserverNodes;

	//private Socket ecsSocket;
	private OutputStream output;
 	private InputStream input;

	TreeMap<String, String> metaData;
	
	TreeMap<String, Socket> ecsSockets;
	TreeMap<String, InputStream> inputs;
	TreeMap<String, OutputStream> outputs;

	/**
     * Constructor: Initializes ECS by populating serverNodes map.
	 */

	public ECS(String configFile) {
			metaData = null;
			servers = new HashMap<String, String>();
			liveserverNodes = new HashMap<String, String>();	

			String getValue = new String();
			File varTmpDir = new File(configFile);
			boolean exists = varTmpDir.exists();
			try {
				if (!exists) throw new IOException();

				BufferedReader br = new BufferedReader(new FileReader(configFile));
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

				String[] tokens = getValue.split("\\s+");

				int i=0,j=0;
				for (i=0;i<tokens.length;i+=3) {
					String serverName    = tokens[i];
					String serverAddress = tokens[i+1] + ":" + tokens[i+2];
					servers.put(serverName, serverAddress);
				}
			} catch (IOException x) {
				System.out.println("Config File does not exist!");
				System.exit(1);
			}
	}


	public void initService (int numberOfNodes, int cacheSize, String replacementStrategy) {
		
		metaData = new TreeMap<String, String> ();
		ecsSockets = new TreeMap<String, Socket> ();
		outputs = new TreeMap<String, OutputStream> ();
		inputs = new TreeMap<String, InputStream> ();


		int i=0, j=0;
		List keys = new ArrayList(servers.keySet());
		for (i = 0; i < numberOfNodes; i++) {
		    String address = servers.get( keys.get(i));
		    String hashed = md5Hash(address);
		    System.out.println (address + "-->"+ hashed);
		    metaData.put (hashed, address);
		}


				for (Map.Entry<String, String> entry : metaData.entrySet()) {
				    String key = entry.getKey();
				    String value = entry.getValue();
				    String [] addr = value.split(":");
				    System.out.println("Initializing Server...:      " + key + " " +addr[0] + "+" + addr[1]);
					// Issue SSH Calls for each of these servers... start Servers and send metadata

					Process proc;
					String script = "java -jar ms3-server.jar " + addr[1];  

					//String script = "ssh -n localhost nohup java -jar ~/Desktop/ECE419/M3/ScalableStorageService-stub/ms3-server.jar " + addr[1];// + " ERROR &";	

					Runtime run = Runtime.getRuntime();
					try {
					  	proc = run.exec(script);
					} catch (IOException e) {
					  e.printStackTrace();
					}
					
					// ADD TIMEOUT!!!!
					
					Exception x = new Exception(); 
					while (x != null) {
						x = null;
						try {
							Socket newSocket = new Socket(addr[0], Integer.parseInt(addr[1])) ;
							ecsSockets.put ( key , newSocket);
							//System.out.println("Local:" +newSocket.getLocalSocketAddress().toString()  + "   Remote:" + newSocket.getRemoteSocketAddress().toString() );
							outputs.put( key, ecsSockets.get(key).getOutputStream() );
							inputs.put( key, ecsSockets.get(key).getInputStream() );	

							sendMessage (key, "init", cacheSize, replacementStrategy);

						} catch (Exception newX) {
							x = newX;
							//System.out.println("Uknown Host.");
						}
					}

					System.out.println ("Server initialized");
				}

						ECSConnections allConns = 
			            		new ECSConnections(inputs, outputs, ecsSockets, this); //, mainCache);
						new Thread( allConns ). start();



	}

	public void start() {
		for (Map.Entry<String, String> entry : metaData.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue();
		    String [] addr = value.split(":");
		    // ...
		    System.out.println("Starting server:      " + key + " " +value);

		    sendMessage (key, "start", 10, "FIFO");
		}
	}


	public void stop() {
		for (Map.Entry<String, String> entry : metaData.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue();
		    // ...
		    System.out.println("Stoping server:      " + key + " " +value);
		    sendMessage (key, "stop", 10, "FIFO");
		}
	}
	public void shutdown() {
		for (Map.Entry<String, String> entry : metaData.entrySet()) {
		    String key = entry.getKey();
		    String value = entry.getValue();
		    // ...
		    System.out.println("Shutting down server:      " + key + " " +value);
		    sendMessage (key, "shutdown", 10, "FIFO");
			// Issue SSH Calls for each of these servers... start Servers and send metadata
		}
		metaData = null;
		metaData = new TreeMap<String, String> ();
	}

	public void add(int cacheSize, String replacementStrategy) {
			if (metaData == null) {
				System.out.println ("Must initService first before adding more servers!");
				return;
			}

			int i=0, j=0;
			String address = null;
			String hashed = null;
			List keys = new ArrayList(servers.keySet());
			if ( servers.size()   >  metaData.size()) { 
				// If servers are available
				for (i = 0; i < keys.size(); i++) {
				    address = servers.get( keys.get(i));
				    hashed = md5Hash(address);
				    if (!metaData.containsKey(hashed)) 
				    	break;
				    // do stuff here
				}
				// Reaches here if found available server
				if (address != null) {
					metaData.put (hashed, address);
					System.out.println ("Adding server ... "+ hashed + "-->"+ address);

					String [] address_tokens= address.split(":");
					String host = address_tokens[0];
					int port = Integer.parseInt(address_tokens[1]);

						//SSH

					/* Start Server */
					Process proc;
					String script = "java -jar ms3-server.jar " + port;  
					//System.out.println(script);
					Runtime run = Runtime.getRuntime();
					try {
					  	proc = run.exec(script);
					} catch (IOException e) {
					  e.printStackTrace();
					}


					Exception x = new Exception(); 
					while (x != null) {
						x = null;
						try {
							Socket newSocket = new Socket( host ,  port ) ;
							ecsSockets.put ( hashed , newSocket);
							//System.out.println("Local:" +newSocket.getLocalSocketAddress().toString()  + "   Remote:" + newSocket.getRemoteSocketAddress().toString() );
							outputs.put(hashed, ecsSockets.get(hashed).getOutputStream() );
							inputs.put( hashed, ecsSockets.get(hashed).getInputStream() );	

							sendMessage (hashed, "init", cacheSize, replacementStrategy);
							
						} catch (Exception newX) {
							x = newX;
							//System.out.println("Uknown Host.");
						}
					}
					
					double start = System.currentTimeMillis();

					double end = System.currentTimeMillis();
					double elapsed = end - start;
					while ( (end-start) < 2000 ) {
							 end = System.currentTimeMillis();
							 elapsed = end - start;
							
					}

					sendMessage (hashed, "start", cacheSize, replacementStrategy);
					System.out.println("Server initialized and started.");

				}
					
					updateMetaDataAllServers();
					double start = System.currentTimeMillis();

					double end = System.currentTimeMillis();
					double elapsed = end - start;
					while ( (end-start) < 2000 ) {
							 end = System.currentTimeMillis();
							 elapsed = end - start;
							
					}
					
					System.out.println("Moving Data for new server.");
					if ( !hashed.equals ( metaData.lastKey() ) ){
						String moveDataFromThisServer = metaData.higherKey(hashed);
						moveData ( moveDataFromThisServer , hashed ); // 1 > 2
					} else {
						String moveDataFromThisServer = metaData.firstKey();
						moveData ( moveDataFromThisServer , hashed ); // 1 > 2
					}

		
			}
			else {
				System.out.println ("All available servers are already running!");			
			}
		    // Initialize and start server. Send updated metadata to servers
	}

	public void moveData (String fromServer , String toServer) {

		//writeLock(fromServer);

		String moveCommand = "moveData";
		String moveDetails = fromServer  + " " + toServer;

		sendMessage (fromServer, moveCommand, 10, moveDetails );

		//writeunlock(fromServer)

	}

	public void remove() {
		if (metaData != null) {
			if (metaData.size() > 1) {

				String serverToRemove = metaData.firstKey();
				metaData.remove (metaData.firstKey() );
				updateMetaDataAllServers();
				//sendMessage (serverToRemove, "lockWrite" , 10 , "FIFO" );	
				moveData ( serverToRemove, metaData.firstKey()	);		
				//sendMessage ( metaData.firstKey(), "shutdown" , 0, "FIFO" ) ;
				// ... Do lockwrite stuff here.				
				//metaData.remove ( metaData.firstKey() );
				
			} else {
				shutdown();			
			}
		}
	}


	/* Send Message */
	public void sendMessage (String serverHash, String command, int cacheSize, String extraData ) {
		try {
		byte adminMessageByte = 0;
		outputs.get(serverHash).write(adminMessageByte);	
		outputs.get(serverHash).flush();
		KVAdminMessage newmsg;
		
		newmsg = new KVAdminMessage(command, metaData, cacheSize, extraData);
		//printServerStarts ( newmsg.getMeta() );

		byte[] msgBytes = newmsg.getMsgBytes();
		outputs.get(serverHash).write(msgBytes, 0, msgBytes.length);
		outputs.get(serverHash).flush();

		} catch (Exception x) {
			System.out.println ("Failed to send message to server!");
			String address = metaData.get(serverHash);
			String [] address_tokens= address.split(":");
					String host = address_tokens[0];
					int port = Integer.parseInt(address_tokens[1]);
							try {
							Socket newSocket = new Socket( host ,  port ) ;
							ecsSockets.put ( serverHash , newSocket);
							//System.out.println("Local:" +newSocket.getLocalSocketAddress().toString()  + "   Remote:" + newSocket.getRemoteSocketAddress().toString() );
							outputs.put(serverHash, ecsSockets.get(serverHash).getOutputStream() );
							inputs.put( serverHash, ecsSockets.get(serverHash).getInputStream() );	
							} catch (Exception x2) {
								 return ;
							}
			sendMessage( serverHash , command, cacheSize, extraData);
		}

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



	public void onServerFail (String failedServer) {

		String addr = metaData.get(failedServer);
		String killThisOne = new String();
		for (String servName: servers.keySet() ) {
			if ( addr.equals (servers.get(servName))) {
				killThisOne = servName;				
				//servers.remove( servName);
				break;	
			}	
		}


		servers.remove(killThisOne);
		inputs.remove(failedServer);
		outputs.remove(failedServer);
		ecsSockets.remove(failedServer);
		metaData.remove(failedServer);
		updateMetaDataAllServers();
								
		ECSConnections allConns = 
			            		new ECSConnections(inputs, outputs, ecsSockets, this); //, mainCache);
						new Thread( allConns ). start();
		//ECSConnections.update(inputs, outputs, ecsSockets, this);

		
		// reallocate data
		add(10, "LIFO");

	}

	public void updateMetaDataAllServers () {
	/*Update the metaData of all the servers.*/
		for (Map.Entry<String, String> entry : metaData.entrySet()) {
			String key = entry.getKey();
			String value = entry.getValue();
			sendMessage (key, "update", 0, "FIFO");
		}
	}







}
