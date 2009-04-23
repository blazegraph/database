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
 * Created on Apr 23, 2009
 */

package com.bigdata.service;

import java.io.IOException;
import java.rmi.Remote;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

/**
 * Interface for running procedures on a remote service. In general,
 * applications SHOULD restrict their discovery of {@link IRemoteExecutor} to
 * {@link IClientService}s. Applications SHOULD NOT run arbitrary
 * {@link Callable}s against an {@link IDataService} or an
 * {@link IMetadataService} unless it is necessary to access the index objects
 * locally on the service (index partition moves, scale-out joins, and rule
 * execution all do this).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IRemoteExecutor extends Remote {

    /**
     * Submit a {@link Callable} and return its {@link Future}. The
     * {@link Callable} will execute on the
     * {@link IBigdataFederation#getExecutorService()}.
     * 
     * @return The {@link Future} for that task.
     * 
     * @throws RejectedExecutionException
     *             if the task can not be accepted for execution.
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @todo change API to <T> Future<T> submit(Callable<T> proc). This will
     *       break existing code but reflects the correct use of generics.
     */
    public Future<? extends Object> submit(Callable<? extends Object> proc)
            throws IOException;

}
