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
package com.bigdata.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.apache.log4j.Logger;

/**
 * Helper task for monitoring the results of otherwise unwatched tasks.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 */
public class MonitoredFutureTask<T> extends FutureTask<T> {

    private static final Logger log = Logger
            .getLogger(MonitoredFutureTask.class);
    
    public MonitoredFutureTask(final Callable<T> callable) {
        super(callable);
    }

    public MonitoredFutureTask(final Runnable runnable, final T result) {
        super(runnable, result);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Hooked to notice when the task is done.
     */
    @Override
    public void run() {
        try {
            super.run();
        } finally {
            doCheck();
        }
    }

    private void doCheck() {
        if (super.isDone()) {
            try {
                super.get();
            } catch (InterruptedException e) {
                log.warn("Interrupted: " + e);
            } catch (ExecutionException e) {
                log.error(e, e);
            }
        }
    }

}
