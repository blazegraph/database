package com.bigdata.bop.engine;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.UUID;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.service.IService;

/**
 * Interface for a node participating in the exchange of NIO buffers to
 * support query execution.
 */
public interface IQueryPeer extends Remote {

    /**
     * The {@link UUID} of the service within which the {@link IQueryPeer} is
     * running.
     * 
     * @see IService#getServiceUUID()
     */
    UUID getServiceUUID() throws RemoteException;

    /**
     * Declare a query to a peer. This message is sent to the peer before any
     * other message for that query and declares the query and the query
     * controller with which the peer must communicate during query evaluation.
     * 
     * @param queryDecl
     *            The query declaration.
     * 
     * @throws UnsupportedOperationException
     *             unless running in scale-out.
     */
    void declareQuery(IQueryDecl queryDecl) throws RemoteException;

    /**
     * Notify a service that a buffer having data for some {@link BOp} in some
     * running query is available. The receiver may request the data when they
     * are ready. If the query is cancelled, then the sender will drop the
     * buffer.
     * 
     * @param msg
     *            The message.
     * 
     * @throws UnsupportedOperationException
     *             unless running in scale-out.
     */
    void bufferReady(IChunkMessage<IBindingSet> msg) throws RemoteException;

}
