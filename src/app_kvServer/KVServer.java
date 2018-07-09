package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;


import java.io.*;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import app_kvServer.Memory;
import app_kvServer.CacheValue;

import app_kvServer.KVServerHeart;



import java.util.*;
import java.security.*;

public class KVServer extends Thread  {

	protected static String thisServerLocation;
	protected static String startLocation;
	protected static String metaDataString;
	protected static String lockMetaData;

	protected static TreeMap<String, String> metaData;
	protected static int cacheSize;
	protected static String replacementStrategy;
	protected static boolean stopped;

	private static Logger logger = Logger.getRootLogger();

	
	private int port;
    private static ServerSocket serverSocket;
    private static boolean running;
	protected static boolean writeLocked;

	protected static Memory mainMemory;

    //public Cache mainCache = null;
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		this.start(); 
		

	}
	public KVServer(int port) {
		this.port = port;

		//mainCache = new Cache (cacheSize, strategy);
		thisServerLocation = new String ("127.0.0.1:" + port);
		thisServerLocation = md5Hash (thisServerLocation);
		writeLocked = false;
		this.start(); 
		

	}

	public static void update ( String metaDataIn) {
				
		metaData = new TreeMap<String, String> ();
		metaDataString = metaDataIn;
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
	



		logger.info("Updated MetaData"); 
		i=0;
		for (Map.Entry<String, String> entry : metaData.entrySet()) {
					// UPDATE METADATA

					String key = entry.getKey();
					String value = entry.getValue();
							logger.info("MetaData:" + i + " :" + key + " " + value); i++;
		}

	}


	public static void initKVServer( String metaDataIn, int cacheSizeIn, String replacementStrategyIn) {

		logger.info("Initializing :" + thisServerLocation);;
		metaData = new TreeMap<String, String> ();
		metaDataString = metaDataIn;
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
		// METADATA ALREADY INITIALIZED
		replacementStrategy = replacementStrategyIn;
		cacheSize = cacheSizeIn;
		stopped = true;

		mainMemory = new Memory(cacheSize, replacementStrategy, thisServerLocation);



			String heartConnAddr = metaData.get( startLocation).trim();
			String[] tokens2 = heartConnAddr.split(":");


					//if (totalConns == 1) {
					
						logger.info("Initializing heart!");
						KVServerHeart heartConnection = 
			            		new KVServerHeart(tokens2[0] , Integer.parseInt(tokens2[1])); //, mainCache);
						//new Thread( heartConnection ). start();

						logger.info("Done initializing heart!");
					
					//}



	}

	public static void startServer() {
		//RunnableClass rc = new RunnableClass();
		//Thread t = new Thread(mainMemory);
		//t.start();
		//mainMemory.run();
		stopped = false;
	}

	public static void stopServer() {
		stopped = true;
	}


	private static OutputStream outputSend;
	private static InputStream inputSend; 
	public static void moveData(String fromServer, String toServer){

		logger.info ("Moving data " + fromServer + " -> " + toServer);
		 
		String[] addrPort = metaData.get(toServer).split(":");
		 

		Map<String, String> data  = new TreeMap<String, String> ();

		//
				

		File folder = new File("Data/" + fromServer);
		File[] listOfFiles = folder.listFiles();
		String getValue = new String();
		
		for (int i = 0; i < listOfFiles.length; i++) {
		  if (listOfFiles[i].isFile()) {
		    //System.out.println("File " + listOfFiles[i].getName());
				try {
							String fileName = "Data/" +fromServer + "/" + listOfFiles[i].getName() ;
							File varTmpDir = new File(fileName);
						 if (!varTmpDir.isDirectory())  {

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
									String keyFile = listOfFiles[i].getName().split(".txt") [0];
									String keyHash = md5Hash ( keyFile );
					
								/*if ( keyHash.compareTo ( toServer ) > 0 && keyHash.compareTo(metaData.lastKey()) < 0 ) {
								} else {
								varTmpDir.delete();
								}*/
								if ( keyHash.compareTo ( toServer ) > 0 && keyHash.compareTo(metaData.lastKey()) < 0 ) {
						
								} else {
										logger.info ("Recording data to move : " + listOfFiles[i].getName() + " " + getValue );
										data.put ( listOfFiles[i].getName().split(".txt") [0], getValue );
										varTmpDir.delete();
								}

								getValue = getValue.trim();
						}
		
				//return getValue;	

			}	catch (IOException x) {

				//return null;	
			}
					//String keyFile = listOfFiles[i].getName().split(".txt") [0];
					//String keyHash = md5Hash ( keyFile );
					

					

		  }
		}

		//
		//String fileName = "Data/" + fromServer + "/" + key + ".txt";
		
		logger.info("Lets move now!");
		Set<String> allKeys  = data.keySet();
		 
		 
		String address = addrPort[0];
		int port = Integer.parseInt( addrPort [1] );

		try {
		Socket sendDataSocket = new Socket ( address, port);
		outputSend = sendDataSocket.getOutputStream();
		inputSend = sendDataSocket.getInputStream();

		for(String key: allKeys){
			KVMessageServer  newMsg = new KVMessageServer ( key, data.get(key) , KVMessageServer.StatusType.PUT ); 
			byte[] msgBytes = newMsg.getMsgBytes();
			outputSend.write(msgBytes);//, 0, msgBytes.length);
			outputSend.flush();
			//sendMessageMove ( newMsg );
		}
		 
		 
		 
		} catch (Exception x) {
		System.out.println ("Unknown host");
		}
			logger.info ("Moved Data");
	}


	public void sendMessageMove(KVMessageServer msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		outputSend.write(msgBytes, 0, msgBytes.length);
		outputSend.flush();
		/*logger.info("SEND \t<" 
		+ clientSocket.getInetAddress().getHostAddress() + ":" 
		+ clientSocket.getPort() + ">: '" 
		+ msg.getMsg() +"'");*/
    }


public static int totalConns;


	 /**
     * Initializes and starts the server. 
     * Loops until the the server should be closed.
     */
    public void run() {
		totalConns = 0;
    	running = initializeServer();
        if(serverSocket != null) {
	        while(isRunning()){
					totalConns ++;
					/*Establish a connection client*/
					try  {
						logger.info("Trying to get a client.");
			            Socket client = serverSocket.accept(); 			            
						KVClientConnection connection = 
			            		new KVClientConnection(client); //, mainCache);
			            new Thread(connection).start(); 
							logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());              
	                } catch (IOException e)  {
	                	logger.info("Failed to connect to client.");
	                		
	                	}
					} 
	        }
	        logger.info(this.port+ " shutting down...");
	}
    
    
    private boolean isRunning() {
        return this.running;
    }

    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public static void shutdown(){
        running = false;
			try {
				serverSocket.close();
			} catch (IOException e)  {
			}
			System.exit(1);
    }

    private boolean initializeServer() {

			try {
            serverSocket = new ServerSocket(port, 500);
					logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
			} catch (IOException e)  {
			}
            return true;

    }	
	

	public static String md5Hash(String key) {
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



    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {	

    	
    	try {
    		new LogSetup("logs/server.log", Level.ALL);
    	if (args.length == 1) {
    		int port = Integer.parseInt(args[0]);
    		metaData = new TreeMap<String, String> ();
    		KVServer mainServer = new KVServer(port);
    		stopped = true;

    	}
    	} catch (Exception x) {
    		System.out.println("Failed to start server!");
    		System.exit(1);
    	} 
    	/*
			try {
				new LogSetup("logs/server.log", Level.ALL);
				if (args.length == 3) {
				System.out.println ("Server starting...");
					int port = Integer.parseInt(args[0]);
					int cacheSize = Integer.parseInt(args[1]);
					String strategy = args[2];

					KVServer mainServer =  new KVServer(port, cacheSize, strategy);//.start();
				}
			}catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
			} catch (NumberFormatException nfe) {
				System.out.println("Error! Invalid argument <port>! Not a number!");
				System.out.println("Usage: Server <port>!");
				System.exit(1);
			}
		
		*/
	
    }


   


}
