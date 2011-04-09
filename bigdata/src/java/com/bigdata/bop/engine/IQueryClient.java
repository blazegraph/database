package com.bigdata.bop.engine;

import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.PipelineOp;

/**
 * Interface for a client executing queries (the query controller).
 */
public interface IQueryClient extends IQueryPeer {

	/**
	 * Return the set of queries which are running as of the moment when the
	 * request was processed. Queries reported in the returned array may
	 * terminate at any time.
	 */
	UUID[] getRunningQueries() throws RemoteException;
	
    /**
     * Return the query.
     * 
     * @param queryId
     *            The query identifier.
     * @return The query.
     * 
     * @throws IllegalArgumentException
     *             if there is no such query.
     */
    PipelineOp getQuery(UUID queryId) throws RemoteException;

    /**
     * Notify the client that execution has started for some query, operator,
     * node, and index partition.
     */
    void startOp(StartOpMessage msg)
            throws RemoteException;

    /**
     * Notify the client that execution has halted for some query, operator,
     * node, shard, and source binding set chunk(s). If execution halted
     * abnormally, then the cause is sent as well.
     */
    void haltOp(HaltOpMessage msg) throws RemoteException;
    
}
