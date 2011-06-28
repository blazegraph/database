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
 * Created on Oct 14, 2008
 */

package com.bigdata.util.concurrent;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for managing the execution of a set of {@link Callable} tasks on
 * an {@link ExecutorService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <T>
 *            The generic type of the values returned by the {@link Callable}
 *            tasks.
 */
public class ExecutionHelper<T> {

    private final ExecutorService service;
    private final long timeout;
    private final TimeUnit unit;

    /**
     * Tasks will execute without timeout.
     * 
     * @param service
     *            The service on which the tasks will be executed.
     */
    public ExecutionHelper(final ExecutorService service) {

        this(service, Long.MAX_VALUE, TimeUnit.MILLISECONDS);

    }

    /**
     * Tasks will execute with the specified timeout.
     * 
     * @param service
     *            The service on which the tasks will be executed.
     * @param timeout
     *            The timeout for the tasks when submitted.
     * @param unit
     *            The unit in which that timeout is expressed.
     */
    public ExecutionHelper(final ExecutorService service, final long timeout,
            final TimeUnit unit) {

        if (service == null) {

            throw new IllegalArgumentException();
            
        }
        
        if(timeout <= 0L) {
            
            throw new IllegalArgumentException();
            
        }
        
        if(unit == null) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.service = service;
       
        this.timeout = timeout;
        
        this.unit = unit;
        
    }
    
    /**
     * Submit subquery tasks, wait until they are done, and verify that all
     * tasks were executed without error. A variety of steps are taken to
     * normalize handling interrupts, canceled execution due to timeout, and
     * rejected execution when the {@link ExecutorService} has been shutdown.
     * 
     * @param tasks
     *            The tasks.
     * 
     * @throws InterruptedException
     *             Tasks that are interrupted and tasks that are canceled due
     *             to timeout result in a logged warning and a thrown
     *             {@link InterruptedException}. Likewise, if a
     *             {@link RejectedExecutionException} is thrown and the service
     *             is shutdown, then that exception is converted into an
     *             {@link InterruptedException}.
     * @throws RejectedExecutionException
     *             If a {@link RejectedExecutionException} is thrown and the
     *             executor service has not been shutdown.
     * @throws ExecutionExceptions
     *             If one or more tasks fails. The causes thrown by the various
     *             tasks are available as an aid to debugging.
     */
    public void submitTasks(final List<Callable<T>> tasks)
            throws InterruptedException, ExecutionExceptions {
        
        if (tasks.isEmpty()) {
            
            // No tasks.
            return;
            
        }
        
        boolean interrupted = false;
        List<ExecutionException> causes = null;
        final List<Future<T>> futures;
        try {

            // submit tasks and await completion of those tasks.
            futures = service.invokeAll(tasks, timeout, unit);
            
            for(Future<T> f : futures) {
                
                // verify that no task failed.
                try {
                    f.get();
                } catch(InterruptedException ex) {
                    interrupted = true;
                } catch(CancellationException ex) {
                    interrupted = true;
                } catch(ExecutionException ex) {
                    if (causes == null) {
                        causes = new LinkedList<ExecutionException>();
                    }
                    causes.add(ex);
                }
                
            }
            
        } catch (InterruptedException ex) {

            /*
             * The task writing on the buffer was interrupted. For query, this
             * is how we eagerly terminate rule evaluation, e.g., when a
             * high-level iterator is closed without fully materializing the
             * solutions that are (or are being) computed. A LIMIT clause on a
             * rule can have this effect.
             */
            
            interrupted = true;
            
        } catch(RejectedExecutionException ex) {
            
            if (service.isShutdown()) {

                /*
                 * Asynchronous shutdown of the executor service.
                 * 
                 * Note: When normal shutdown of the service is requested it is
                 * common that the main thread will be in a state in which it
                 * attempts to schedule more task(s). This results in a
                 * RejectedExecutionException. We treat this just like an
                 * interrupt since the join can not progress due to the shutdown
                 * of the executor service.
                 */

                interrupted = true;
                
            } else {

                throw ex;
                
            }
            
        }

        if(interrupted) {

            throw new InterruptedException("Terminated by interrupt");
            
        }
        
        if (causes != null) {

            throw new ExecutionExceptions(causes);

        }

    }
    
}
