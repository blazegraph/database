package com.bigdata.bop.engine;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.BOp;

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
     * Notify the client that execution has started for some query, operator,
     * node, and index partition.
     * 
     * @param queryId
     *            The query identifier.
     * @param opId
     *            The operator identifier.
     * @param partitionId
     *            The index partition identifier.
     * @param serviceId
     *            The node on which the operator will execute.
     * @param nchunks
     *            The #of chunks which form the input to that operator (for the
     *            atomic termination condition decision).
     */
    public void startOp(long queryId, int opId, int partitionId, UUID serviceId, final int nchunks)
            throws RemoteException;

    /**
     * Notify the client that execution has halted for some query, operator,
     * node and index partition. If execution halted abnormally, then the cause
     * is sent as well.
     * 
     * @param queryId
     *            The query identifier.
     * @param opId
     *            The operator whose execution phase has terminated for a
     *            specific index partition and input chunk.
     * @param partitionId
     *            The index partition against which the operator was executed.
     * @param serviceId
     *            The node which executed the operator.
     * @param cause
     *            <code>null</code> unless execution halted abnormally.
     * @param nchunks
     *            The #of chunks which were output by the operator (for the
     *            atomic termination decision). This is ONE (1) for scale-up.
     *            For scale-out, this is one per index partition over which the
     *            intermediate results were mapped.
     * @param taskStats
     *            The statistics for the execution of that bop on that shard and
     *            service.
     */
    public void haltOp(long queryId, int opId, int partitionId, UUID serviceId,
            Throwable cause, int nchunks, BOpStats taskStats)
            throws RemoteException;

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
