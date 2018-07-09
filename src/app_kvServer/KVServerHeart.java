package app_kvServer;

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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;




public class KVServerHeart implements Runnable {

	String address;
	int port;
		private static Logger logger = Logger.getRootLogger();

	private Socket clientSocket;
	private OutputStream output;
 	private InputStream input;

	public boolean isRunning;
	/*public KVServerHeart(Socket clientSocket) { //, Cache mainCache) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		inputreads=0;		
	}*/
	

	public KVServerHeart(String address, int port) {
		

		try {
			new LogSetup("logs/server.log", Level.ALL);
			logger.info ("Starting heart");
			this.address = address;
			this.port = port;
					isRunning = true;
		
		} catch (Exception x) {
			logger.info ("EXCEPTION:HEARTBEAT!");
		}
	}	

	@Override
	public void run() {
		while (isRunning) {
			try {
					clientSocket = new Socket(address, port);
	

					output = clientSocket.getOutputStream();
					input = clientSocket.getInputStream();



				logger.info("Sun is shining.");
							byte heartBeat = 3;
							output.write(heartBeat);	
							output.flush();

				while (input.available() > 0 ) {
					byte inByte = (byte) input.read();
				}

						input.close();
						output.close();
						clientSocket.close();

					double start = System.currentTimeMillis();

					double end = System.currentTimeMillis();
					double elapsed = end - start;
					while ( (end-start) < 20000 ) {
							 end = System.currentTimeMillis();
							 elapsed = end - start;
							
					}
	
			} catch (Exception x) {
				logger.info ("EXCEPTION:HEARTBEAT!");
				KVClientConnection.serverDropped = true;
			}
		}
	}
}
