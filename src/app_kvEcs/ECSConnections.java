package app_kvEcs;

import java.io.*;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import java.security.*;
import java.util.*;

public class ECSConnections implements Runnable {

	public  boolean isRunning;
	public  TreeMap<String, InputStream> inputsE;
	public  TreeMap<String, OutputStream> outputsE;
	public  TreeMap<String, Socket> ecsSocketsE;
	public  ECS ecsInstanceE;

	public	ECSConnections (TreeMap<String, InputStream>  inputs, TreeMap<String, OutputStream>  outputs, TreeMap<String, Socket> ecsSockets, ECS ecsInstance) {
		isRunning = true;
		inputsE = inputs;
		outputsE = outputs;
		ecsSocketsE = ecsSockets;
		ecsInstanceE = ecsInstance;
	}	




	@Override
	public void run() {
		
		while (isRunning) {
			String failedServer = new String();		
	//System.out.println("hi, hi!");
				Set<String> inputsSet = inputsE.keySet();

				for (String x: inputsSet) {
					failedServer = x;
					try {
					InputStream thisStream = inputsE.get(x);
					OutputStream outputS = outputsE.get(x);
					
							byte heartBeat = 3;
							outputS.write(heartBeat);	
							outputS.flush();

					while (thisStream.available() > 0) {
								byte reading = (byte) thisStream.read();
								if (reading == -1) {//System.out.println("SomeoneDied");
									throw new Exception();
								}
								if ( reading == 4) {}//System.out.println("SomeoneDied++");}
					}			
						if (ecsSocketsE.get(x).isClosed()) {
									
						}				 else {

 							//System.out.println(".");
							//System.out.println("" + ecsSocketsE.get(x).getRemoteSocketAddress());
						}					
					} catch (Exception exp) {
						System.out.println ("Exception thrown while reading connection streams.");
						ecsInstanceE.onServerFail( failedServer);

						isRunning = false;
						return;
						
					}
				}


					double start = System.currentTimeMillis();

					double end = System.currentTimeMillis();
					double elapsed = end - start;
					while ( (end-start) < 5000 ) {
							 end = System.currentTimeMillis();
							 elapsed = end - start;
							
					}


		}		
	}


}
