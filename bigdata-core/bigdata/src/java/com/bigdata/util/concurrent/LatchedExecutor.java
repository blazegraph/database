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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

/**
 * A fly weight helper class that runs tasks either sequentially or with limited
 * parallelism against some thread pool. Deadlock can arise when limited
 * parallelism is applied if there are dependencies among the tasks. Limited
 * parallelism is enforced using a counting {@link Semaphore}. New tasks can
 * start iff the latch is non-zero. The maximum parallelism is the minimum of
 * the value specified to the constructor and the potential parallelism of the
 * delegate service.
 * <p>
 * Note: The pattern for running tasks on this service is generally to
 * {@link #execute(Runnable)} a {@link Runnable} and to make that
 * {@link Runnable} a {@link FutureTask} if you want to await the {@link Future}
 * of a {@link Runnable} or {@link Callable} or otherwise manage its execution.
 * <p>
 * Note: This class can NOT be trivially wrapped as an {@link ExecutorService}
 * since the resulting delegation pattern for submit() winds up invoking
 * execute() on the delegate {@link ExecutorService} rather than on this class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @todo write unit tests.
 */
public class LatchedExecutor implements Executor {

    private static final transient Logger log = Logger
            .getLogger(LatchedExecutor.class);
    
    /**
     * The delegate executor. 
     */
    private final Executor executor;
    
    /**
     * This is used to limit the concurrency with which tasks submitted to this
     * class may execute on the delegate {@link #executor}.
     */
    private final Semaphore semaphore;
    
    /**
     * A thread-safe blocking queue of pending tasks.
     * 
     * @todo The capacity of this queue does not of necessity need to be
     *       unbounded.
     */
    private final BlockingQueue<Runnable> queue = new LinkedBlockingDeque<Runnable>(/*unbounded*/);

    private final int nparallel;
    
    /**
     * Return the maximum parallelism allowed by this {@link Executor}.
     */
    public int getNParallel() {
    	
    	return nparallel;
    	
    }
    
    public LatchedExecutor(final Executor executor, final int nparallel) {

        if (executor == null)
            throw new IllegalArgumentException();

        if (nparallel < 1)
            throw new IllegalArgumentException();

        this.executor = executor;

        this.nparallel = nparallel;
        
        this.semaphore = new Semaphore(nparallel);

    }

    @Override
    public void execute(final Runnable r) {
        if (!queue.offer(new Runnable() {
            /*
             * Wrap the Runnable in a class that will start the next Runnable
             * from the queue when it completes.
             */
           @Override
            public void run() {
                try {
                    r.run();
                } finally {
                    scheduleNext();
                }
            }
        })) {
            // The queue is full.
            throw new RejectedExecutionException();
        }
        if (semaphore.tryAcquire()) {
            // We were able to obtain a permit, so start another task.
            scheduleNext();
        }
    }

    /**
     * Schedule the next task if one is available (non-blocking).
     * <p>
     * Pre-condition: The caller has a permit.
     */
	private void scheduleNext() {
		while (true) {
			Runnable next = null;
			if ((next = queue.poll()) != null) {
				try {
					executor.execute(next);
					return;
				} catch (RejectedExecutionException ex) {
					// log error and poll the queue again.
					log.error(ex, ex);
					continue;
				}
			} else {
				semaphore.release();

				// check for a missed enqueue
                if (!queue.isEmpty() && semaphore.tryAcquire()) {
                    continue;
                }

                return;
			}
		}
	}
}
