package app_kvEcs;

import java.io.*;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class ECSClient {

	private ECS ecsInstance;
	private static String configFile;
	private BufferedReader stdin;
	//ECSClient client;

	private static final String PROMPT = "ECS Admin> ";

	private boolean stop;

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECS HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("initService <number of nodes> <cache size> <replacement strategy>");
		sb.append("\t Initializes servers \n");
		sb.append(PROMPT).append("start");
		sb.append("\t\t\t\t\t\t\t\t Starts Initialized servers \n");
		sb.append(PROMPT).append("stop");
		sb.append("\t\t\t\t\t\t\t\t\t Stops all servers \n");
		sb.append(PROMPT).append("shutdown");
		sb.append("\t\t\t\t\t\t\t\t Shutdowns all servers \n");
		sb.append(PROMPT).append("add <cache size> <replacement strategy>");
		sb.append("\t\t\t\t Adds server \n");
		sb.append(PROMPT).append("remove");
		sb.append("\t\t\t\t\t\t\t\t Removes a random server. \n");
		sb.append("\n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");
		
		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

		private void printError(String error){
			System.out.println(PROMPT + "Error! " +  error);
		}

	private void handleCommand(String cmdLine) throws IOException {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			System.out.println(PROMPT + "Application exit!");
		}
		else if (tokens[0].equals("initService")) {
			if (tokens.length == 4 ) {
				ecsInstance.initService(Integer.parseInt(tokens[1]), Integer.parseInt(tokens[2]), tokens[3]);
			} else {
				printError("Invalid number of parameters!");
			}
		}
		else if (tokens[0].equals("start")) {
			ecsInstance.start();
		}
		else if (tokens[0].equals("stop")) {
			ecsInstance.stop();

		}
		else if (tokens[0].equals("shutdown")) {
			ecsInstance.shutdown();
		}
		else if (tokens[0].equals("add")) {
			if (tokens.length == 3) {
				ecsInstance.add(Integer.parseInt(tokens[1]), tokens[2]);
			} else {
				printError("Invalid number of parameters!");
			}
		}
		else if (tokens[0].equals("remove")) {
				ecsInstance.remove();
		}
		else{
			printError("Invalid Command.");
		}
	}

	public void run() {
		ecsInstance = new ECS(configFile);
		stop = false;
		printHelp();
		//ecsInstance.initService(5,10,"LIFO");
		while (!stop) {

			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			
			try {
				
				String cmdLine = stdin.readLine();
				try {
					this.handleCommand(cmdLine);
				} catch (IOException e) {
					printError("Could not handle command.");
				}
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}

		}
	}

	public static void main(String[] args) {
		System.out.println("Starting ECS.");
		try {
			if (args.length != 1) throw new NumberFormatException();
			configFile = args[0];

			ECSClient client = new ECSClient();
			client.run();
			
		} catch (NumberFormatException nfe) {
				System.out.println("Format Error! Usage: ECS.jar <config_file>!");
				System.exit(1);
		}
	}

}
