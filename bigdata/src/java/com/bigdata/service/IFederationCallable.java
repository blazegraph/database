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

import java.io.Serializable;
import java.util.concurrent.Callable;

/**
 * Interface for {@link Callable}s which require access to the
 * {@link IBigdataFederation} when running on an {@link IRemoteExecutor}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IFederationCallable extends Serializable {

    /**
     * Invoked before the task is executed to provide a reference to the
     * {@link IBigdataFederation} for the service on which the task is
     * executing.
     * 
     * @param fed
     *            The federation.
     * 
     * @throws IllegalArgumentException
     *             if the argument is <code>null</code>
     * @throws IllegalStateException
     *             if {@link #setFederation(IBigdataFederation)} has already
     *             been invoked.
     */
    void setFederation(IBigdataFederation fed);

    /**
     * Return the {@link IBigdataFederation} reference.
     * 
     * @return The federation and never <code>null</code>.
     * 
     * @throws IllegalStateException
     *             if {@link #setFederation(IBigdataFederation)} has not been
     *             invoked.
     */
    IBigdataFederation getFederation();

}
