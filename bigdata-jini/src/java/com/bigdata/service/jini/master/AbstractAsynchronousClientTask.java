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
 * Created on Jul 10, 2009
 */

package com.bigdata.service.jini.master;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.bigdata.service.FederationCallable;
import com.bigdata.service.jini.JiniFederation;

/**
 * Abstract base class for {@link IAsynchronousClientTask}.
 * 
 * @param <U>
 *            The generic type of the {@link Callable}'s value.
 * @param <V>
 *            The generic type of the resource identifier.
 * @param <L>
 *            The generic type of the client locator.
 */
public abstract class AbstractAsynchronousClientTask<U, V, L> extends
        FederationCallable<U> implements IAsynchronousClientTask<U, V>
//        INotifyOutcome<V, L> 
{

    private final INotifyOutcome<V, L> proxy;

    private transient Future<U> future;

    /**
     * 
     * @param proxy
     *            The proxy for the
     */
    protected AbstractAsynchronousClientTask(final INotifyOutcome<V, L> proxy) {

        if (proxy == null)
            throw new IllegalArgumentException();

        this.proxy = proxy;
        
    }

    /**
     * Return the proxy for the remote object which is notified as asynchronous
     * operations succeed or fail.
     */
    public INotifyOutcome<V, L> getNotifyProxy() {

        return proxy;
        
    }
    
    synchronized public Future<U> getFuture() {
        
        if (future == null)
            throw new IllegalStateException();
        
        return ((JiniFederation<?>) getFederation()).getProxy(future);
        
    }

    /**
     * Set the {@link Future}.
     * 
     * @param future
     *            The future for this callable.
     * 
     * @throws IllegalArgumentException
     *             if the <i>future</i> is <code>null</code>.
     * @throws IllegalStateException
     *             if the future has been set to a different value.
     */
    synchronized public final void setFuture(final Future<U> future) {
        
        if (future == null)
            throw new IllegalArgumentException();

        if (this.future != null && this.future != future)
            throw new IllegalStateException();

        this.future = future;

    }

//    /**
//     * Invokes the corresponding method on the proxy.
//     */
//    public void success(final V resource,final L locator) throws RemoteException {
//
//        proxy.success(resource, locator);
//
//    }
//
//    /**
//     * Invokes the corresponding method on the proxy.
//     */
//    public void error(final V resource, final L locator, final Throwable cause)
//            throws RemoteException {
//
//        proxy.error(resource, locator, cause);
//
//    }

//    /**
//     * Open an input stream reading from the resource identifier. This method is
//     * designed for pluggable abstractions for resolving the resource identifier
//     * to its data.
//     * 
//     * @param resource
//     *            The resource identifier.
//     */
//    abstract protected InputStream openResource(V resource) throws IOException;

}
