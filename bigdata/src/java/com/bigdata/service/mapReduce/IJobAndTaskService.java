/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Sep 23, 2007
 */

package com.bigdata.service.mapReduce;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;
import java.util.concurrent.Future;


/**
 * A interface for a service using a job and task model. The service may have
 * many clients, and each client may start jobs and execute tasks within running
 * jobs. A job must be started before tasks may be submitted. Tasks run with
 * some unknown concurrency. The service MUST be sent a heartbeat for each job
 * to keep tasks running in that job. The heartbeat returns the #of completed
 * tasks for that job. The client can drain the completed tasks for a job,
 * transferring information about their {@link Outcome} to itself. A pending or
 * running task may be cancelled, in which case it will show up as completed
 * (with {@link Status#Cancelled}). If the service does not receive several
 * heatbeats in a row for a job, then it will cancel the job and all tasks
 * running in that job. When the client is done executing tasks for a job, it
 * should cancel the job. When a job is cancelled, all state for that job is
 * thrown away.
 * <p>
 * Note: This interface extends {@link Remote} since it may be used as a remote
 * service. Per the contract of the {@link Remote} interface, methods on this
 * interface are declared to throw {@link IOException} since they may be invoked
 * by RMI.
 * 
 * @param M
 *            The type for the job metadata parameter in
 *            {@link #startJob(UUID, Object)}.
 * @param T
 *            The type for the task in {@link #submit(UUID, Object)}.
 * 
 * @todo add a status message returning a String containing a service specific
 *       status summary.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IJobAndTaskService<M extends IJobMetadata,T extends ITask> extends Remote {

    /**
     * The unique identifier for this service.
     * 
     * @return The unique service identifier.
     */
    public UUID getServiceUUID() throws IOException;

    /**
     * Declare a job. The service will now accept tasks for the job. The client
     * MUST send {@link #heartbeat(UUID)} messages or the service will
     * {@link #cancelJob(UUID) cancel} the job.
     * 
     * @param jobMetadata
     *            The service specific job metadata.
     */
    public void startJob(M jobMetadata) throws IOException;

    /**
     * Terminate a job. The service will no longer accept tasks for this job.
     * Any running tasks will be terminated. This method SHOULD be invoked
     * whether a job is terminated normally or aborted. It will be invoked by
     * the service in any case if the service no longer recieves
     * {@link #heartbeat(UUID)} messages for this job.
     * 
     * @param job
     *            The job identifier.
     * 
     * @exception IllegalStateException
     *                if the job is not known to the service.
     */
    public void cancelJob(UUID job) throws IOException;

    /**
     * The service will {@link #cancelJob(UUID)} the job if it does not continue
     * to receive heartbeat for that job. The service timeout for a job is on
     * the order of a few seconds, but clients are encouraged to send heartbeats
     * ever 100ms.
     * <p>
     * Note: Clients may also use this message to monitor the progression of
     * tasks for the specified job.
     * 
     * @param job
     *            The job identifier.
     * 
     * @return The #of completed tasks for that job.
     */
    public int heartbeat(UUID job) throws IOException;
    
    /**
     * Drains the {@link Outcome}s for all completed tasks. The {@link Outcome}
     * for those tasks is no longer available from the service.
     * 
     * @param job
     *            The job identifier.
     * 
     * @return The task {@link Outcome}s.
     */
    public Outcome[] drain(UUID job) throws IOException;
    
    /**
     * Submit a task to be executed as part of a job (asynchronous). The task
     * will be placed into a queue and will begin executing once there is an
     * available worker. When the tasks completes with any {@link Status}, it
     * will be reported by {@link #heartbeat(UUID)} and its {@link Outcome} will
     * be made available by {@link #drain(UUID)}.
     * 
     * @param job
     *            The job identifier.
     * @param task
     *            The task to be executed.
     * @param timeout
     *            When non-zero, the timeout in milliseconds for this task once
     *            it begins to execute on the service (this controls the actual
     *            maximum run time of the task rather than the time since the
     *            task was submitted or since the job was started).
     */
    public void submit(UUID job, T task, long timeout) throws IOException;

    /**
     * Cancel a task. If the task is queued it will not be run. It is is
     * running, it will be cancelled. If the task has already completed the
     * service will ignore this message.
     * 
     * @param job
     *            The job identifier.
     * @param task
     *            The task identifier.
     * 
     * @return false if the task could not be cancelled (per
     *         {@link Future#cancel(boolean)}).
     */
    public boolean cancel(UUID job, UUID task) throws IOException;

}
