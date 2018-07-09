package client;

import java.io.Serializable;

import common.messages.KVMessage;
/**
 * Represents a simple text message, which is intended to be received and sent 
 * by the server.
 */
public class KVMessageClient implements KVMessage {

	private static final long serialVersionUID = 5549512212003782618L;
	private String msg;
	private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	


	public StatusType statusType;
	private String key;
	private String value;


	public StatusType getStatus() {
		return statusType;
	}

	public String getKey() {
		return key;	
	}
	public String getValue() {
		return value;
	}

    /**
     * Constructs a TextMessage object with a given array of bytes that 
     * forms the message.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public KVMessageClient(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes);
	}
	
	/**
     * Constructs a TextMessage object with a given String that
     * forms the message. 
     * 
     * @param msg the String that forms the message.
     */
	public KVMessageClient(String key, String value, StatusType st) {
			this.statusType = st;
			this.key = key;
			this.value = value;
			this.msgBytes = toByteArray(key, value, st);
	}

	public KVMessageClient(String msg) {
			this.msg = msg;
			this.msgBytes = toByteArray("",msg, StatusType.PUT);
	}


	/**
	 * Returns the content of this TextMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMsg() {
		if (this.statusType == StatusType.PUT_SUCCESS) {
			return "PUT_SUCESS";
		}
		if (this.statusType == StatusType.PUT_UPDATE) {
			return "PUT_UPDATE";
		}
		if (this.statusType == StatusType.PUT_ERROR) {
			return "PUT_ERROR";
		}
		if (this.statusType == StatusType.DELETE_SUCCESS) {
			return "DELETE_SUCCESS";
		}
		if (this.statusType == StatusType.DELETE_ERROR) {
			return "DELETE_ERROR";
		}
		
		if (this.statusType == StatusType.GET_ERROR) {
			return "GET_ERROR";
		}
		//return msg;
		return value;
	}

	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
	public byte[] getMsgBytes() {
		return msgBytes;
	}
	
	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	/*private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}*/
	private byte[] toByteArray(String key, String value, StatusType st){

		byte[] st_bytes = st.name().getBytes();
		byte[] bytes = key.getBytes();
		byte[] v_bytes = value.getBytes();
		byte[] ctrBytes1 = new byte[]{32};
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};

		byte[] tmp = new byte[ st_bytes.length + ctrBytes1.length + bytes.length + ctrBytes1.length + v_bytes.length + ctrBytes.length];

		System.arraycopy(st_bytes,0,tmp,0         ,st_bytes.length);
		System.arraycopy(ctrBytes1,0,tmp,st_bytes.length         ,ctrBytes1.length);
		System.arraycopy(bytes,0   ,tmp, st_bytes.length + ctrBytes1.length         ,bytes.length);
		System.arraycopy(ctrBytes1,0,tmp,st_bytes.length + ctrBytes1.length + bytes.length         ,ctrBytes1.length);
		System.arraycopy(v_bytes ,0,tmp, st_bytes.length + ctrBytes1.length + bytes.length + ctrBytes1.length        ,v_bytes.length);
		System.arraycopy(ctrBytes,0,tmp, st_bytes.length + ctrBytes1.length + bytes.length + ctrBytes1.length + v_bytes.length       ,ctrBytes.length);

		/*KVMessage ByteArray formed of the format |StatusBytes|CtrBytes|MSGBytes|CtrBytes|*/
		return tmp;

		/*
		byte[] bytes = s.getBytes();
		byte[] bytesST = (st.name().getBytes());

		byte[] combinedbytes = new byte[bytes.length + bytesST.length];

		System.arraycopy(bytes,0,combinedbytes,0         ,bytes.length);
		System.arraycopy(bytesST,0,combinedbytes,bytes.length,bytesST.length);

		
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[combinedbytes.length + ctrBytes.length];
		
		System.arraycopy(combinedbytes, 0, tmp, 0, combinedbytes.length);
		System.arraycopy(ctrBytes, 0, tmp, combinedbytes.length, ctrBytes.length);
		*/
				
	}
	
}
