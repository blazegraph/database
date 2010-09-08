package com.bigdata.bop.engine;

import java.rmi.Remote;
import java.rmi.RemoteException;

import com.bigdata.bop.BOp;

/**
 * Interface for a client executing queries (the query controller).
 */
public interface IQueryClient extends IQueryPeer {

    /*
     * @todo Could return a data structure which encapsulates the query results
     * and could allow multiple results from a query, e.g., one per step in a
     * program.
     */
    
//    /**
//     * Evaluate a query which materializes elements, such as an
//     * {@link IPredicate}.
//     * 
//     * @param queryId
//     *            The unique identifier for the query.
//     * @param timestamp
//     *            The timestamp or transaction against which the query will run.
//     * @param query
//     *            The query to evaluate.
//     * @param source
//     *            The initial binding sets to get the query going (this is
//     *            typically an iterator visiting a single empty binding set).
//     * 
//     * @return An iterator visiting the elements materialized by the query.
//     * 
//     * @throws Exception
//     */
//    public IChunkedIterator<?> eval(long queryId, long timestamp, BOp query)
//            throws Exception;

//    /**
//     * Evaluate a query which visits {@link IBindingSet}s, such as a join.
//     * 
//     * @param queryId
//     *            The unique identifier for the query.
//     * @param timestamp
//     *            The timestamp or transaction against which the query will run.
//     * @param query
//     *            The query to evaluate.
//     * @param source
//     *            The initial binding sets to get the query going (this is
//     *            typically an iterator visiting a single empty binding set).
//     * 
//     * @return An iterator visiting {@link IBindingSet}s which result from
//     *         evaluating the query.
//     * 
//     * @throws Exception
//     */
//    public IChunkedIterator<IBindingSet> eval(long queryId, long timestamp,
//            BOp query, IAsynchronousIterator<IBindingSet[]> source)
//            throws Exception;

    /**
     * Return the query.
     * 
     * @param queryId
     *            The query identifier.
     * @return The query.
     * 
     * @throws RemoteException
     */
    public BOp getQuery(long queryId) throws RemoteException;

    /**
     * Notify the client that execution has started for some query, operator,
     * node, and index partition.
     */
    public void startOp(StartOpMessage msg)
            throws RemoteException;

    /**
     * Notify the client that execution has halted for some query, operator,
     * node, shard, and source binding set chunk(s). If execution halted
     * abnormally, then the cause is sent as well.
     */
    public void haltOp(HaltOpMessage msg) throws RemoteException;
    
//    /**
//     * Notify the query controller that a chunk of intermediate results is
//     * available for the query.
//     * 
//     * @param queryId
//     *            The query identifier.
//     */
//    public void addChunk(long queryId) throws RemoteException;
//
//    /**
//     * Notify the query controller that a chunk of intermediate results was
//     * taken for processing by the query.
//     * 
//     * @param queryId
//     *            The query identifier.
//     */
//    public void takeChunk(long queryId) throws RemoteException;
    
}
