package common.messages;

public interface KVMessage {
	
    public enum StatusType {
    	GET,             		/* Get - request */
    	GET_ERROR,       		/* requested tuple (i.e. value) not found */
    	GET_SUCCESS,     		/* requested tuple (i.e. value) found */
    	PUT,               		/* Put - request */
    	PUT_SUCCESS,     		/* Put - request successful, tuple inserted */
    	PUT_UPDATE,      		/* Put - request successful, i.e., value updated */
    	PUT_ERROR,       		/* Put - request not successful */
    	DELETE_SUCCESS,  		/* Delete - request successful */
    	DELETE_ERROR,     		/* Delete - request successful */
    	SERVER_STOPPED,         /* Server is stopped, no requests are processed */
    	SERVER_WRITE_LOCK,      /* Server locked for out, only get possible */
    	SERVER_NOT_RESPONSIBLE,  /* Request not successful, server not responsible for key */
		REPLICA_PUT, /*Send update key/value to replica with Replica put*/
		REPLICA_PUT_SUCCESS,       /*Replica replies to coordinator with replica_put_success upon successfully inserting key/value*/
		REPLICA_PUT_UPDATE, /*Replica replies to coordinator with replica_put_update_success upon successfully updating key/value*/
		REPLICA_DELETE_SUCCESS, /*Replica replies to coordinator with replica_DELETE_SUCCESS upon successfully deleting key/value*/
		REPLICA_DELETE_ERROR,
		REPLICA_PUT_ERROR,       /*Replica replies to coordinator with replica_put_error if put was unsuccessful*/
		REPLICA_GET_SUCCESS, /*If replica gets a read request and it covers that hash range then we return the value with this Status Type*/
		CHECK_HEARTBEAT,
		HEARTBEAT_OK

}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();
	
}


