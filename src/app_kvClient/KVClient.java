package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVStore;

import logger.LogSetup;

import client.ClientSocketListener;

import common.messages.KVMessage;

public class KVClient implements ClientSocketListener{

		//public enum SocketStatus{CONNECTED, DISCONNECTED, CONNECTION_LOST};
	

		private static Logger logger = Logger.getRootLogger();
		private boolean stop = false;
		private static final String PROMPT = "EchoClient> ";
		private BufferedReader stdin;

		private String serverAddress;
		private int serverPort;

		private KVStore client = null;

	private void connect(String address, int port)
		throws UnknownHostException, IOException {
		client = new KVStore (address, port);
		client.connect();
		client.listeners.add(this);
		System.out.println("Connection to server established!: /" + address
			+ " /" + port);
		//client.start();
		logger.info("Connection established");

	}
	private void disconnect() {
		//try {
			client.disconnect();
			client = null;
			System.out.println("Disconnected!");
			//client.start();
			logger.info("Connection Disconnected");
		///} catch (IOException e) {
		//	System.out.println("Unable to disconnect!");
		///client.start();
			//logger.info("Unable to disconnect");
		//}

	}

	@Override
	public void handleNewMessage(KVMessage msg) {
		if(!stop) {
			if (msg.getStatus().toString().equals("GET_SUCCESS")) {
				System.out.println(msg.getValue());
			}
			else {
				System.out.println(msg.getStatus().toString());
			}
			//System.out.print(PROMPT);
		}
	}
	
	@Override
	public void handleStatus(SocketStatus status) {
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}
		
	}
	/*HANDLE COMMAND*/
	private void handleCommand(String cmdLine) throws IOException {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			if (client != null && client.isRunning ) {
				disconnect();
			}
			System.out.println(PROMPT + "Application exit!");
		
		}/*Connect*/
		 else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);

				} catch(NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		}
		/*PUT*/
		
		else if (tokens[0].equals("put")) {
	
			if (client != null && client.isRunning ) {
				String key = tokens[1];
				StringBuilder value = new StringBuilder();
					for(int i = 2; i < tokens.length; i++) {
						value.append(tokens[i]);
						if (i != tokens.length -1 ) {
							value.append(" ");
						}
					}	
				try {
					if (key.length() > 20 || value.length() > 122880) {
						throw new IOException();
					}
					KVMessage retVale = client.put (key, value.toString());
					handleNewMessage(retVale);	
				} catch (Exception e) {
					printError("Unable to put!");

				}
			} else {
				printError("Not connected!");
				throw new IOException();
			}
				
		}
		/*GET*/		
		else if (tokens[0].equals("get")) {
	
			if (client != null && client.isRunning ) {
				String key = tokens[1];
				
				try {
					KVMessage retVale = client.get(key);
					handleNewMessage(retVale);
				}catch (Exception e) {
					printError("Unable to put!");

				}	
			} else {
				printError("Not connected!");		
			}
				
		}
		/*XX*/
		else if(tokens[0].equals("disconnect")) {
			if (client != null && client.isRunning ) {
				disconnect();
			} else {
				printError("Not connected!");
			}

			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}


		public void run() {

				try {
					new LogSetup("logs/client.log", Level.OFF);		
				} catch (IOException e) {
						System.out.println("Error! Unable to initialize logger!");
						e.printStackTrace();
						System.exit(1);
				}
				
				while(!stop) {

					stdin = new BufferedReader(new InputStreamReader(System.in));
					System.out.print(PROMPT);
					
					try {
						
						String cmdLine = stdin.readLine();
								double start = System.currentTimeMillis();
						if (cmdLine != null ){
						try {
							int exceptions = 0;
							if (cmdLine.equals("Y")) {
								int i= 0;
								for (i=0;i<100;i++) {
									String value = "asdfasdfasdfasdf asdfaesd end";
									String newKey = new String ("yuxasdf" + i);
									try  {
			 						KVMessage retVale = client.put (newKey, value);
			 						} catch (Exception x) {
			 							//System.out.println("EX!");
			 							exceptions++;
			 						}
								}
							} else {
										if (cmdLine.equals("a")) {
													char a = 'a'; int i = 0;for (i=0;i<30;i++) {
										try  {
						 						KVMessage retVale = client.put ( "" + a++, "lolz");
						 						} catch (Exception x) {
						 							//System.out.println("EX!");
						 							exceptions++;
						 						}
										}	}
							}

							double end = System.currentTimeMillis();

								//System.out.println ("Time: " + start + " End:" + end + " total:   " + (end -start));
						//	System.out.println("Done. Exceptions Found: " + exceptions  +".");
							
							this.handleCommand(cmdLine);
								
						} catch (IOException e) {
							printError("Could not handle command.");
						}
						}
					} catch (IOException e) {
						stop = true;
						printError("CLI does not respond - Application terminated ");
					}
				}
		}	

		private void printError(String error){
			System.out.println(PROMPT + "Error! " +  error);
		}

		private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("send <text message>");
		sb.append("\t\t sends a text message to the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}
	
	private void printPossibleLogLevels() {
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}


		/**
		 * Main entry point for the echo server application. 
		 * @param args contains the port number at args[0].
		 */
		public static void main(String[] args) {
			//try {
			/*	String a = "aaaaaaaa";
				String b = "fdfsdfsa";
				String c = "0asfasdf";
				int aa = a.compareTo(b); 
				int bb = a.compareTo(c);
				System.out.println (":::>> " + aa + " " + bb);*/
				
				KVClient application = new KVClient();
				application.run();
			/*} /*catch (IOException e) {
				System.out.println("Error! Unable to initialize logger!");
				e.printStackTrace();
				System.exit(1);
			}*/
		}

}
