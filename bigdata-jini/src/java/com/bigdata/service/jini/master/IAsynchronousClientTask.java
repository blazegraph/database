/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jul 13, 2009
 */

package com.bigdata.service.jini.master;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Interface for remote {@link Callable}s which process chunks of resources as
 * assigned by the {@link MappedTaskMaster}. This interface extends
 * {@link Remote} since its methods are invoked using RMI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <U>
 *            The generic type of the {@link Callable}'s aggregate result.
 * @param <V>
 *            The generic type of the resources in the chunk.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IAsynchronousClientTask<U, V> extends Remote {

    // Not a remote method.
//    /**
//     * This is the main thread for the client. It should setup operations and
//     * then block. The master will interrupt the main thread using
//     * {@link Future#cancel(boolean)} once (a) all pending work is done; or (b)
//     * the job is interrupted.
//     * 
//     * {@inheritDoc}
//     */
//    public U call() throws Exception;

    /**
     * Accept a chunk of resources for processing. The client may block while it
     * is accepting the chunk, but SHOULD NOT process the resources within this
     * method. For each resource processed, the client MUST notify the master
     * regarding the success or error for that resource. Those notifications are
     * processed asynchronously.
     * 
     * @param chunk
     *            The chunk of resources to be processed. Each resource is
     *            normally a file path, URL, etc.
     * 
     * @throws RemoteException
     *             if there is an RMI problem.
     * @throws InterruptedException
     *             if the client thread accepting the request is interrupted.
     */
    public void accept(V[] chunk) throws RemoteException, InterruptedException;

    /**
     * Signal that no new chunks will be tasked to client. The client should
     * flush its asynchronous index write buffers. The main thread should
     * normally remain blocked in {@link Callable#call()} until the master
     * interrupts the client using {@link Future#cancel(boolean)}. However, if
     * all work is complete, then the client MAY interrupt the main thread.
     * 
     * @throws RemoteException
     *             if there is an RMI problem.
     * @throws InterruptedException
     *             if the client thread accepting the request is interrupted.
     */
    public void close() throws RemoteException, InterruptedException;
    
    /**
     * Return a proxy for the future of this task (this allows us to have both
     * the proxy for the task and the proxy for its {@link Future}). The
     * {@link Future} is required in order to interrupt the client tasks when
     * the {@link MappedTaskMaster} terminates.
     * 
     * @throws IllegalStateException
     *             if the future has not been set.
     */
    public Future<U> getFuture() throws RemoteException;

}
