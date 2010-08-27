package com.bigdata.bop.engine;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IPredicate;
import com.bigdata.relation.accesspath.IAsynchronousIterator;
import com.bigdata.striterator.IChunkedIterator;

/**
 * Interface for a client executing queries.
 */
public interface IQueryClient extends IQueryPeer, Remote {

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
     * Notify the client that execution has started for some query,
     * operator, node, and index partition.
     * 
     * @param queryId
     * @param opId
     * @param serviceId
     * @param partitionId
     */
    public void startOp(long queryId, int opId, UUID serviceId,
            int partitionId) throws RemoteException;

    /**
     * Notify the client that execution has halted for some query, operator,
     * node and index partition. If execution halted abnormally, then the
     * cause is sent as well.
     * 
     * @param queryId
     * @param opId
     * @param serviceId
     * @param partitionId
     * @param cause
     *            <code>null</code> unless execution halted abnormally.
     */
    public void haltOp(long queryId, int opId, UUID serviceId,
            int partitionId, Throwable cause) throws RemoteException;

}