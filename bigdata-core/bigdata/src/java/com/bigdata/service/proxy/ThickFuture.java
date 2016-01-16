/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jan 4, 2012
 */

package com.bigdata.service.proxy;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A thick {@link Future}. This is used to communicate the state of a
 * {@link Future} to a remote client. The constructor blocks until the
 * {@link Future} is done and then makes a record of the {@link Future}'s state.
 * That state is serializable as the object returned by the {@link Future} is
 * serializable.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ThickFuture<V> implements Future<V>, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    private final V outcome;
    private final boolean cancelled;
    private final ExecutionException cause;

    /**
     * Awaits the {@link Future} and then makes a serializable record of the
     * {@link Future}'s state.
     */
    public ThickFuture(final Future<V> f) {
        V outcome = null;
        boolean cancelled = false;
        ExecutionException cause = null;
        try {
            outcome = f.get();
        } catch (InterruptedException e) {
            cancelled = true;
        } catch (ExecutionException e) {
            cause = e;
        }
        this.outcome = outcome;
        this.cancelled = cancelled;
        this.cause = cause;
    }
    
    /**
     * {@inheritDoc}
     * <p>
     * The future can not be cancelled as it is already done.
     */
    @Override
    final public boolean cancel(boolean mayInterruptIfRunning) {
        // Return [false] since the Future is already done.
        return false;
    }

    @Override
    final public boolean isCancelled() {
        return cancelled;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Always returns <code>true</code>.
     */
    @Override
    final public boolean isDone() {
        // Always true.
        return true;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@link Future} is already done, so this method does not block.
     */
    final public V get() throws InterruptedException, ExecutionException {
        if (cancelled)
            throw new InterruptedException();
        if (cause != null)
            throw cause;
        return outcome;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The {@link Future} is already done, so this method does not block.
     */
    @Override
    final public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return get();
    }

}
