package app_kvServer;

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
import java.util.concurrent.locks.ReentrantLock;

public class CacheValue implements Comparable<CacheValue>{

	public String key;
	//value
	public String value;
	//needed for times used in LFU
	public int timesUsed;
	//needed to see if it needs to be written back upon eviction
	public int dirtyBit;

	public String hashValue;

	//lock for fine grained locking
	//public Lock pairLock;

	public CacheValue(String key, String value, int dirtyBit, String hashValue){
		this.key = key;
		this.value = value;
		this.dirtyBit = dirtyBit;
		this.hashValue = hashValue;
		this.timesUsed = 1;
	}

	public int compareTo(CacheValue entry){

		if(this.timesUsed == entry.timesUsed){
			return 0;
		}else{
			return this.timesUsed<entry.timesUsed ? -1 : 1;
		}
	}
}
