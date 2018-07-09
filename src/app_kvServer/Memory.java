package app_kvServer;

//import java.nio.file;
import java.io.*;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.LinkedHashMap;
import java.util.Map;

import app_kvServer.CacheValue;
import java.util.concurrent.locks.ReentrantLock;

/**
*This class represents a main Memory which would also have a cache that would be initialzed with the configuration based on the parameters passed in
* 
*/


public class Memory implements Runnable{

	private boolean running;
	//Structure for main memory
	// <|Key = MD5 Hash||Value = CacheValue|>
	private TreeMap<String, CacheValue> mainMemory;//
	//private TreeMap<String, CacheValue> keyMemory;
					//|Key, CacheValue|
	private String hashCode;//

	//cache initialization parameters
	private int cacheSize;//
	private int cacheOccupancy;//
	private String strategy;//
	private HashMap<String, CacheValue> cache;//

	//structures to implement eviction strategies
	private LinkedList<CacheValue> fifoList;//
	private LinkedList<CacheValue> lfuList;//
	private LinkedList<CacheValue> lruList;//

	//structure containing dirty cache entries
	private ArrayList<CacheValue> dirtyEntries;//

	//locks for fine grained lock
	private TreeMap<String, ReentrantLock> lockMap;//
	private ReentrantLock putLock;
	//private ReentrantLock evictLock();


	/**
	* This a constructor for the memory object associated with the server. A structure is initialized to represent the cache as well
	* @param size: the size of the cache needed to be initialized
	* @param strategy: Replacement strategy for the Eviction of Cache blocks
	* @param hashCode: The MD5 Hash Code associated with the server 
	*/
	public Memory(int size, String strategy, String hashCode){
		//initialize the cache
		this.cacheSize = size;//
		this.cacheOccupancy = 0;//
		this.strategy = strategy;//
		this.hashCode = hashCode;//
		this.cache = new HashMap<String, CacheValue>(this.cacheSize);//

		//structure containing dirty cache entries
		this.dirtyEntries = new ArrayList<CacheValue>();//

		this.mainMemory = new TreeMap<String, CacheValue>();//
									//|hashcode, cacheValue|
		//locks for fine grained locking
		this.lockMap = new TreeMap<String, ReentrantLock>();//
		this.putLock = new ReentrantLock();

		this.lruList = null;//
		this.lfuList = null;//
		this.fifoList = null;//

		this.running = true;

		if(this.strategy == "LRU"){this.lruList = new LinkedList<CacheValue>();}
		else if(this.strategy == "LFU"){this.lfuList = new LinkedList<CacheValue>();}
		else{this.fifoList = new LinkedList<CacheValue>();}
	}

/*****************************************************************Caching Mechanisms****************************************************************/
	public boolean put(String key, String value, String hashValue){

		//create an entry for the map
		CacheValue cacheEntry = new CacheValue(key, value, 1, hashValue);

		if(!lockMap.containsKey(hashValue)){
			putLock.lock();
			try{
				ReentrantLock newLock = new ReentrantLock(true);
				newLock.lock();
				try{	
					lockMap.put(hashValue, newLock);
					this.mainMemory.put(hashValue,cacheEntry);
					this.cache.put(hashValue, cacheEntry);
					//this.cacheOccupancy++;
					this.dirtyEntries.add(cacheEntry);
					updateEvictionLists(cacheEntry);
					
				}finally{newLock.unlock();}
				
			}finally{putLock.unlock();}
		}
		else{
			ReentrantLock hashLock = lockMap.get(hashValue);
			hashLock.lock();
			try{
				//check if the cache has this entry
				if(this.cache.containsKey(hashValue)){
					cacheEntry.timesUsed += (cache.get(hashValue)).timesUsed;
				}
				
				if(this.cacheSize == this.cacheOccupancy){
					if(!this.cache.containsKey(hashValue)){
						evictEntry();		
					}
				}

				this.mainMemory.put(hashValue,cacheEntry);
				this.cache.put(hashValue,cacheEntry);
				//this.cacheOccupancy++;
				this.dirtyEntries.add(cacheEntry);
				updateEvictionLists(cacheEntry);

			}finally{hashLock.unlock();}//release the lock on hashvalue
		}
		return true;
	}


	public String get(String key, String hashValue){

		if(!lockMap.containsKey(hashValue)){return null;}
		else{

			
			ReentrantLock hashLock = lockMap.get(hashValue);
			CacheValue cacheEntry = null;
			//CacheValue cacheEntry;
			hashLock.lock();
			try{
				
				//check cache
				if(this.cache.containsKey(hashValue)){

					cacheEntry = this.cache.get(hashValue);
					cacheEntry.timesUsed++;
					updateEvictionLists(cacheEntry);
				//then check memory & fetch from disk always
				}else {
					if(this.onDisk(key)){

						if(this.cacheSize == this.cacheOccupancy){
							evictEntry();
						}

						cacheEntry = fetchFromDisk(key, hashValue);
						cacheEntry.timesUsed++;
						this.cache.put(cacheEntry.hashValue, cacheEntry);
						//this.cacheOccupancy++;
						updateEvictionLists(cacheEntry);
					}
				}
			}finally{hashLock.unlock();}

			return cacheEntry.value;
		}
	}

	public boolean delete(String key, String hashValue){

		if(!lockMap.containsKey(hashValue)){return false;}
		else{
			ReentrantLock hashLock = lockMap.get(hashValue);
			hashLock.lock();
			try{
				this.lockMap.remove(hashValue);
				CacheValue kvPair = this.cache.get(hashValue);
				this.cache.remove(hashValue);
				//this.cacheOccupancy--;
				this.mainMemory.remove(hashValue);
				this.lfuList.remove(kvPair);
				this.lruList.remove(kvPair);
				this.fifoList.remove(kvPair);

				String fileName = "Data/" + key + ".txt";
				//create file
				File tmpFile = new File(fileName);

				if(onDisk(key)){
					tmpFile.delete();
				}
			}finally{
				hashLock.unlock();
				hashLock = null;
			}
			return hashLock == null;
		}
	}

	private void updateEvictionLists(CacheValue cacheEntry){

		CacheValue temp = this.cache.get(cacheEntry.hashValue);

		if(this.strategy == "FIFO"){
			fifoList.remove(temp);
			fifoList.add(0, cacheEntry);	

		}else if(this.strategy == "LFU"){
			lfuList.remove(temp);
			lfuList.add(cacheEntry);
			Collections.sort(lfuList);
		}else{
			lruList.remove(temp);
			lruList.add(0, cacheEntry);
		}

		return;
	}	

	private boolean evictEntry(){

		Collections.sort(this.lfuList);

		//if(CacheTable.containsKey(key)){
		CacheValue kvPair;
		ReentrantLock hashLock;

		if(this.strategy == "LRU"){
		//evict entry at the end of the list
			kvPair = this.lruList.removeLast();
		}else if(this.strategy == "LFU"){
			kvPair = this.lfuList.removeFirst();
		}else{//FIFO
			kvPair = this.fifoList.removeLast();
		}
			
		hashLock = this.lockMap.get(kvPair.hashValue);
		hashLock.lock();
		try{
			this.cache.remove(kvPair.key);
		//	this.cacheOccupancy--;	
			kvPair.timesUsed = 0;
			this.mainMemory.put(kvPair.hashValue, kvPair);
			this.dirtyEntries.add(kvPair);
		}finally{
			hashLock.unlock();

		}
		//writeToDisk(kvPair.key, kvPair);
		return true;
	}
/********************************************************************DISK OPERATIONS*********************************************************************/
	/**
	* Checks whether a given key has been placed on the disk
	*/
	//checks whether a key exists on disk 
	private boolean onDisk(String key){
		String fileName = "Data/" + key + ".txt";
		File tmpFile = new File(fileName);
		return tmpFile.exists();
	}

	

		//fetches key value pair from the disk into the cache
	//returns null if not on disk
	private CacheValue fetchFromDisk(String key, String hashValue){

		if(!onDisk(key)){
			return null;
		}else{
			//record file name
			String fileName = "Data/" + key + ".txt";
				//create file
			try{
				File tmpFile = new File(fileName);
				//make a buffered reader to read the file
				
				BufferedReader br = new BufferedReader(new FileReader(fileName));

				try{
					//make a string builder to get the value
					StringBuilder sb = new StringBuilder();
					try {
						String line = br.readLine();
						while (line != null) {
							sb.append(line);
							sb.append(System.lineSeparator());
							line = br.readLine();
						}

						String value = sb.toString();
						CacheValue entry = new CacheValue(key,value,0, hashValue);
						return entry;

					} 
					catch (IOException e) {
						System.out.println("Function fetchFromDisk can't do br.readline()");
					}
				}
				finally{
					try {
						br.close();
					}catch(IOException iox){
						System.out.println("can't close buffer");
					}
 				}
			}catch(FileNotFoundException ffe){
				System.out.println("Function fetchFromDisk tried to open a file that doesn't exist");
			}	
		}
		return null;
	}
	
	private boolean writeToDisk(String key, CacheValue entry){

		//record file name
		String fileName = "Data/" + key + ".txt";
		//create file
		File tmpFile = new File(fileName);

		if(onDisk(key)){
			tmpFile.delete();
		}

		File varTmpDir = new File(fileName);
		try{
			FileWriter fileWriter = new FileWriter(fileName);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			bufferedWriter.write(entry.value);
			bufferedWriter.close();
			return true;
		}catch(IOException e){
			System.out.println("Write to Disk Failed");
		}
		return false;
	}

	public TreeMap<String, CacheValue> returnData(String range){

		String tempKey = range;
		CacheValue temp;
		TreeMap<String, CacheValue> obj = new TreeMap<String, CacheValue>();
		while(mainMemory.lowerKey(tempKey) != null){

			
			ReentrantLock hashLock = lockMap.get(tempKey);
			hashLock.lock();
			try{
				temp = mainMemory.get(tempKey);
				obj.put(tempKey, temp);
			}finally{
				hashLock.unlock();
			}
			tempKey = mainMemory.lowerKey(tempKey);
			
		}

		return obj;
	}

	public boolean removeData(String range){

		String tempKey = range;
		CacheValue temp;
		//TreeMap<String, CacheValue> removedData;
		while(mainMemory.lowerKey(tempKey) != null){
			
			temp = mainMemory.get(tempKey);
			delete(tempKey, temp.hashValue);

			tempKey = mainMemory.lowerKey(tempKey);
		}

		return true;
	}

	public void run(){
		ReentrantLock hashLock;
		this.cacheOccupancy = this.cache.size();
		while(this.running){
			for(CacheValue entry: this.dirtyEntries){
				if(this.lockMap.containsKey(entry.hashValue)){
					hashLock = lockMap.get(entry.hashValue);
					hashLock.lock();
					try{	
						this.dirtyEntries.remove(entry);
						entry.dirtyBit = 0;

						if(entry.value == this.mainMemory.get(entry.hashValue).value){
							this.mainMemory.put(entry.hashValue, entry);
							if(this.cache.containsKey(entry.hashValue)){this.cache.put(entry.hashValue, entry);}	
							writeToDisk(entry.key, entry);
						}
						
					}finally{
						hashLock.unlock();
					}
					
				}
			}
		}

		this.cacheOccupancy = this.cache.size();
	}

	public void stop(){
		this.running = false;
		ReentrantLock hashLock;
		this.cacheOccupancy = this.cache.size();
		while(this.running){
			for(CacheValue entry: this.dirtyEntries){
				if(this.lockMap.containsKey(entry.hashValue)){
					hashLock = lockMap.get(entry.hashValue);
					hashLock.lock();
					try{	
						this.dirtyEntries.remove(entry);
						entry.dirtyBit = 0;

						if(entry.value == this.mainMemory.get(entry.hashValue).value){
							this.mainMemory.put(entry.hashValue, entry);
							if(this.cache.containsKey(entry.hashValue)){this.cache.put(entry.hashValue, entry);}	
							writeToDisk(entry.key, entry);
						}
						
					}finally{
						hashLock.unlock();
					}
					
				}
			}
		}
		this.cacheOccupancy = this.cache.size();
	}
}
