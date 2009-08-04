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

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface used by client tasks to notify the master about the outcome of each
 * resource tasked to that client. The interface extends {@link Remote} since it
 * is invoked from the client tasks against a proxy for the master.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <V>
 *            The generic type of the resource identifier.
 * @param <L>
 *            The generic type of the client locator.
 *            
 * @see IAsynchronousClientTask
 */
public interface INotifyOutcome<V, L> extends Remote {

    /**
     * Invoked each time a client tasks completes processing for some resource.
     * 
     * @param resource
     *            The resource identifier.
     * @param locator
     *            The client locator.
     */
    public void success(final V resource, final L locator)
            throws RemoteException;

    /**
     * Invoked each time a client tasks encounters an error when processing some
     * resource.
     * 
     * @param resource
     *            The resource identifier.
     * @param locator
     *            The client locator.
     * @param t
     *            The exception.
     */
    public void error(final V resource, final L locator, final Throwable t)
            throws RemoteException;

}
