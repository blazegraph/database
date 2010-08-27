package com.bigdata.bop.engine;

import java.net.InetSocketAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;

import com.bigdata.bop.BOp;

/**
 * Interface for a node participating in the exchange of NIO buffers to
 * support query execution.
 */
public interface IQueryPeer extends Remote {

    /**
     * Notify a service that a buffer having data for some {@link BOp} in
     * some running query is available. The receiver should may request the
     * buffer when they are ready. If the query is cancelled, then the
     * sender will drop the buffer.
     * 
     * @param clientProxy
     *            proxy used to communicate with the client running the
     *            query.
     * @param serviceAddr
     *            address which may be used to demand the data.
     * @param queryId
     *            the unique query identifier.
     * @param bopId
     *            the identifier for the target {@link BOp}.
     * 
     * @return <code>true</code> unless the receiver knows that the query
     *         has already been cancelled.
     * 
     * @todo nodes should hold open connections for buffer transfers to
     *       avoid overhead with establishing those connections and problems
     *       with immediate reconnect under heavy load.
     */
    void bufferReady(IQueryClient clientProxy, InetSocketAddress serviceAddr,
            long queryId, int bopId) throws RemoteException;

}