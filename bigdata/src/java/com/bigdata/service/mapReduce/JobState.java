package com.bigdata.service.mapReduce;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import com.bigdata.service.IBigdataClient;
import com.bigdata.service.mapReduce.AbstractJobAndTaskService.AbstractTaskWorker;

/**
 * Job state that is relevant to this service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JobState<M> {

    /**
     * The job identifier.
     */
    final UUID uuid;

    /**
     * The job metadata.
     */
    final M metadata;

    /**
     * Used to access the federation from which inputs will be read/written.
     * 
     * @todo break into an input federation and an output federation so that
     * you can run a map/reduce across federations.
     */
    final IBigdataClient client;

    /**
     * The job start time.
     */
    final long begin = System.currentTimeMillis();

    /**
     * The time at which the last heartbeat was received for this job.
     */
    long heartbeat = begin;

    /**
     * The #of tasks started for this job on this service.
     */
    long nstarted = 0L;

    /**
     * The #of tasks ended that have produced an {@link Outcome} for this
     * job on this service.
     */
    long nended = 0L;

    /**
     * The running tasks for this job. The key is the task UUID. The value
     * is the {@link Future} for that task.
     */
    Map<UUID, Future<Object>> futures = new ConcurrentHashMap<UUID, Future<Object>>();

    /**
     * The {@link Outcome}s for the completed tasks. {@link Outcome}s are
     * placed into this queue by the {@link AbstractTaskWorker}.
     */
    BlockingQueue<Outcome> outcomes = new LinkedBlockingQueue<Outcome>(/*no capacity limit*/);

    /**
     * The job identifier.
     */
    public UUID getUUID() {

        return uuid;

    }

    public JobState(UUID uuid, M metadata, IBigdataClient client) {

        if (uuid == null)
            throw new IllegalArgumentException();

        if (metadata == null)
            throw new IllegalArgumentException();

        if (client == null)
            throw new IllegalArgumentException();

        this.uuid = uuid;

        this.metadata = metadata;

        this.client = client;

    }

    /**
     * Cancel all running tasks for this job.
     * <p>
     * Note: The job MUST be cancelled first since otherwise tasks could
     * continue to be queued while this method is running.
     * <p>
     * Note: {@link Outcome}s are discarded when the job is cancelled.
     */
    public void cancelAll() {

        int n = 0;

        Iterator<Future<Object>> itr = futures.values().iterator();

        while (itr.hasNext()) {

            Future<Object> future;

            try {

                future = itr.next();

            } catch (NoSuchElementException ex) {

                MapService.log.info("Exhausted by concurrent completion.");

                break;

            }

            future.cancel(true/*may interrupt if running*/);

            n++;

            try {

                itr.remove();

            } catch (NoSuchElementException ex) {

                MapService.log.info("Task already gone.");

            }

        }

        MapService.log.info("Cancelled " + n + " tasks for job=" + uuid);

        // discard the outcomes when the job is cancelled.
        outcomes.clear();

    }

    /**
     * Cancel the task if it is running.
     * 
     * @param task
     *            The task identifier.
     * 
     * @return true if the job was cancelled.
     */
    public boolean cancel(UUID task) {

        Future<Object> future = futures.remove(task);

        if (future != null && future.cancel(true/*may interrupt if running*/)) {

            MapService.log.info("Cancelled task: job=" + uuid + ", task="
                    + task);

            // Note: This is done by the AbstractTaskWorker.
            //                outcomes.add(new Outcome(task,Status.Cancelled,null));

            return true;

        } else {

            MapService.log.info("Could not cancel task - not running? : job="
                    + uuid + ", task=" + task);

            return false;

        }

    }

    /**
     * A human readable summary of the {@link JobState}.
     */
    public String status() {

        // #of active tasks at this instant (can change asynchronously).
        final int nactive = futures.size();

        // #of outcomes at this instant (can change asynchronously).
        final int noutcomes = outcomes.size();

        return "job=" + uuid + ", nactive=" + nactive + ", noutcomes="
                + noutcomes + ", nstarted=" + nstarted + ", nended=" + nended;

    }

}
