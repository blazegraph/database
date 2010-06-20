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

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.btree.AsynchronousIndexWriteConfiguration;
import com.bigdata.btree.BigdataMap;
import com.bigdata.btree.BigdataSet;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.relation.accesspath.BlockingBuffer;
import com.bigdata.service.IMetadataService;
import com.bigdata.service.jini.JiniFederation;

/**
 * Extends the {@link TaskMaster} to assign chunks of resources for processing
 * to the client tasks.
 * <p>
 * The master scans a source such as the file system, a distributed file system,
 * or a scale-out index to identify the resources to be processed. The resources
 * are placed onto a hash-partitioned buffer, which is drained by the various
 * clients. If a client succeeds, then the resources have been handled
 * successfully and a new set of resources is assigned to that client. If a
 * client fails, the resources are reassigned to a different client. If all
 * clients fail, the job is aborted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <S>
 *            The generic type of the {@link JobState}.
 * @param <T>
 *            The generic type of the client task.
 * @param <U>
 *            The generic type of the value returned by the client task.
 * @param <V>
 *            The generic type of the resources to be tasked.
 */
public abstract class MappedTaskMaster<//
S extends MappedTaskMaster.JobState,//
T extends AbstractAsynchronousClientTask<U, V, L>, //
L extends ClientLocator, //
U, //
V extends Serializable//
>//
        extends TaskMaster<S, T, U> {

    /**
     * {@link Configuration} options for the {@link MappedTaskMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions extends TaskMaster.ConfigurationOptions {

        /**
         * Option specifies the {@link AsynchronousIndexWriteConfiguration} used
         * to provision the buffer on which the master queues the resources to
         * be processed by the client tasks.
         * <p>
         * Note: The configuration values for the indices are stored in the
         * {@link IndexMetadata} on the {@link IMetadataService}. The easiest
         * way to tweak things is just to update the {@link IndexMetadata}
         * objects on the {@link IMetadataService}.
         * <p>
         * Note: Clients using the asynchronous index write API are MUCH more
         * efficient if they can maintain a steady workload.
         */
        String RESOURCE_BUFFER_CONFIG = "resourceBufferConfig";
        
        /**
         * Option specifies the {@link IResourceScannerFactory} used to
         * instantiate the scanner which will select the resources to be
         * processed by the job.
         */
        String RESOURCE_SCANNER_FACTORY = "resourceScannerFactory";

        /**
         * The initial capacity of the pending {@link Map} for the job -or-
         * {@link Integer#MAX_VALUE} to use a {@link BigdataMap}.
         * <p>
         * The pending set contains all resources for which there is an
         * outstanding asynchronous work request. There is a pending set for the
         * {@link ResourceBufferTask} (per job) and the
         * {@link ResourceBufferSubtask} (per client).
         */
        String PENDING_SET_MASTER_INITIAL_CAPACITY = "pendingSetMasterInitialCapacity";

        int DEFAULT_PENDING_SET_MASTER_INITIAL_CAPACITY = 16;

        /**
         * The initial capacity of the pending {@link Set} for each client -or-
         * {@link Integer#MAX_VALUE} to use a {@link BigdataSet}.
         */
        String PENDING_SET_SUBTASK_INITIAL_CAPACITY = "pendingSetSubtaskInitialCapacity";
   
        int DEFAULT_PENDING_SET_SUBTASK_INITIAL_CAPACITY = 16;

        /**
         * The hash function used to assign resources to client tasks.
         */
        String CLIENT_HASH_FUNCTION = "clientHashFunction";
        
        /**
         * When <code>true</code>, the source files identified by the scanner
         * will be deleted if they are successfully processed.
         */
        String DELETE_AFTER = "deleteAfter";

    }
    
    /**
     * The job description for an {@link MappedTaskMaster}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class JobState extends TaskMaster.JobState {
    
        /**
         * 
         */
        private static final long serialVersionUID = 1395850823170993889L;

        /**
         * Configuration for the buffer used to pass resources to the client
         * tasks.
         * 
         * @see ConfigurationOptions#RESOURCE_BUFFER_CONFIG
         */
        protected final AsynchronousIndexWriteConfiguration conf;

        /**
         * Factory for the scanner selecting the resources to be processed.
         * 
         * @see ConfigurationOptions#RESOURCE_SCANNER_FACTORY
         */
        protected final IResourceScannerFactory<?> scannerFactory;

        /**
         * @see ConfigurationOptions#PENDING_SET_MASTER_INITIAL_CAPACITY
         */
        public final int pendingSetMasterInitialCapacity;

        /**
         * @see ConfigurationOptions#PENDING_SET_SUBTASK_INITIAL_CAPACITY
         */
        public final int pendingSetSubtaskInitialCapacity;

        /**
         * @see ConfigurationOptions#CLIENT_HASH_FUNCTION
         */
        public final IHashFunction clientHashFunction;
        
        /**
         * When <code>true</code>, the each data file will be deleted once
         * its data has been loaded into the {@link ITripleStore}.
         * 
         * @see 
         */
        public final boolean deleteAfter;
                
        @Override
        protected void toString(final StringBuilder sb) {

            super.toString(sb);

            sb.append(", " + ConfigurationOptions.RESOURCE_BUFFER_CONFIG + "="
                    + conf);

            sb.append(", " + ConfigurationOptions.RESOURCE_SCANNER_FACTORY
                    + "=" + scannerFactory);

            sb.append(", "
                    + ConfigurationOptions.PENDING_SET_MASTER_INITIAL_CAPACITY
                    + "=" + pendingSetMasterInitialCapacity);

            sb.append(", "
                    + ConfigurationOptions.PENDING_SET_SUBTASK_INITIAL_CAPACITY
                    + "=" + pendingSetSubtaskInitialCapacity);

            sb.append(", " + ConfigurationOptions.DELETE_AFTER + "="
                    + deleteAfter);

        }

        /**
         * {@inheritDoc}
         */
        public JobState(final String component, final Configuration config)
                throws ConfigurationException {

            super(component, config);
            
            /*
             * Note: the chunk size here should be selected in part as a
             * function of the effort involved in processing each resource
             * assigned to a given client. Together with the input queue on the
             * client, that will determine how many outstanding resource
             * requests are scheduled for processing, how far the scanner is
             * running in advance of the task execution, and how much workload
             * is assigned to a client in advance of observing the rate at which
             * the client is consuming its workload.
             */
            final int masterChunkSize = 10000;
            final int sinkChunkSize = 1000;
            conf = (AsynchronousIndexWriteConfiguration) config.getEntry(component,
                    ConfigurationOptions.RESOURCE_BUFFER_CONFIG, AsynchronousIndexWriteConfiguration.class,
                    new AsynchronousIndexWriteConfiguration(//
                            100, // masterQueueCapacity,
                            masterChunkSize, //
                            TimeUnit.SECONDS.toNanos(5),// masterChunkTimeoutNanos
                            Long.valueOf(IndexMetadata.Options.DEFAULT_SINK_IDLE_TIMEOUT_NANOS).longValue(),//
                            Long.valueOf(IndexMetadata.Options.DEFAULT_SINK_POLL_TIMEOUT_NANOS).longValue(),//
                            100, // sinkQueueCapacity
                            sinkChunkSize,//
                            TimeUnit.SECONDS.toNanos(20)// sinkChunkTimeoutNanos
                    ));

            scannerFactory = (IResourceScannerFactory) config.getEntry(
                    component, ConfigurationOptions.RESOURCE_SCANNER_FACTORY,
                    IResourceScannerFactory.class);

            pendingSetMasterInitialCapacity = (Integer) config
                    .getEntry(
                            component,
                            ConfigurationOptions.PENDING_SET_MASTER_INITIAL_CAPACITY,
                            Integer.TYPE,
                            ConfigurationOptions.DEFAULT_PENDING_SET_MASTER_INITIAL_CAPACITY);

            pendingSetSubtaskInitialCapacity = (Integer) config
                    .getEntry(
                            component,
                            ConfigurationOptions.PENDING_SET_SUBTASK_INITIAL_CAPACITY,
                            Integer.TYPE,
                            ConfigurationOptions.DEFAULT_PENDING_SET_SUBTASK_INITIAL_CAPACITY);

            clientHashFunction = (IHashFunction) config.getEntry(component,
                    ConfigurationOptions.CLIENT_HASH_FUNCTION,
                    IHashFunction.class, new DefaultHashFunction());

            deleteAfter = (Boolean) config.getEntry(
                    component,
                    ConfigurationOptions.DELETE_AFTER, Boolean.TYPE);

        }

    }

    /**
     * {@inheritDoc}
     */
    public MappedTaskMaster(JiniFederation<?> fed) throws ConfigurationException {

        super(fed);

    }

    /**
     * Runs the scanner, handing off resources to clients for processing. The
     * clients should run until they are interrupted by the master. When the
     * scanner is done, the resource buffer is closed. The master will continue
     * to run until the pendingSet is empty. Once the master buffer is exhausted
     * (closed and drained), the sinks will flush their last chunks and then
     * {@link IAsynchronousClientTask#close()} the clientTask. This prevents
     * workload starvation during the shutdown protocol.
     */
    @Override
    protected void runJob() throws Exception {
        
        /*
         * Allocate and start processes for buffer used to assign resources to
         * the client tasks.
         */
        final BlockingBuffer<V[]> resourceBuffer = newResourceBuffer();

        try {

            // instantiate scanner backed by the resource buffer.
            final AbstractResourceScanner<?> scanner = getJobState().scannerFactory
                    .newScanner((BlockingBuffer) resourceBuffer);

            // start scanner.
            final Future<Long> scannerFuture = fed.getExecutorService().submit(
                    scanner);

            System.out.println("Master running : " + scanner);

            // await scanner future.
            final Long acceptCount = scannerFuture.get();

            System.out.println("Master accepted " + acceptCount
                    + " resources for processing.");

            // close the buffer - no more resources will be queued.
            resourceBuffer.close();

            // await the completion of the work for the queued resources.
            resourceBuffer.getFuture().get();

        } catch (Throwable t) {

            // interrupt buffer.
            resourceBuffer.abort(t);

            // rethrow exception.
            throw new RuntimeException(t);
            
        }
        
    }

    /**
     * Allocate and start processing for the buffer used to hand off resources
     * for processing to the clients.
     * 
     * @todo Specializing the behavior of the {@link MappedTaskMaster} requires
     *       overriding the behavior of the {@link ResourceBufferTask}. It
     *       should be parameterized if possible with interfaces for interesting
     *       things, however its termination logic is tricky and overrides
     *       should not mess with that.
     */
    public BlockingBuffer<V[]> newResourceBuffer() {

        final AsynchronousIndexWriteConfiguration conf = getJobState().conf;

        final ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics> stats //
            = new ResourceBufferStatistics<ClientLocator, ResourceBufferSubtaskStatistics>(
                getFederation());
        
        final BlockingBuffer<V[]> resourceBuffer = new BlockingBuffer<V[]>(
                new LinkedBlockingDeque<V[]>(conf.getMasterQueueCapacity()),//
                conf.getMasterChunkSize(),//
                conf.getMasterChunkTimeoutNanos(),// 
                TimeUnit.NANOSECONDS,//
                false// NOT ordered data.
        );

        final ResourceBufferTask.M<V> task = new ResourceBufferTask.M<V>(
                this, //
                conf.getSinkIdleTimeoutNanos(),//
                conf.getSinkPollTimeoutNanos(),//
                conf.getSinkQueueCapacity(), //
                conf.getSinkChunkSize(), //
                conf.getSinkChunkTimeoutNanos(),//
                stats,//
                resourceBuffer//
        ) {
          
            @Override
            public void didSucceed(final V resource) {
                super.didSucceed(resource);
                if (getJobState().deleteAfter) {
                    if (resource instanceof File) {
                        final File file = (File) resource;
                        if (!file.delete() && file.exists()) {
                            log.warn("Could not delete file: " + resource);
                        }
                    }
                }
            }

            /*
             * Overridden to log the error here rather than on the
             * AbstractPendingSetMaster's log.
             */
            @Override
            public void didFail(final V resource,final Throwable t) {
                log.error(resource,t);
//                super.didFail(resource,t);
            }

        };

        final Future<? extends ResourceBufferStatistics> future = getFederation()
                .getExecutorService().submit(task);

        resourceBuffer.setFuture(future);

        /*
         * Attach to the counters reported by the client to the LBS.
         */
        attachPerformanceCounters(stats.getCounters());

        return task.getBuffer();

    }

    /**
     * Unsupported -- see {@link #newClientTask(INotifyOutcome, int)}.
     * 
     * @throws UnsupportedOperationException
     *             always.
     */
    protected T newClientTask(final int clientNum) {
        throw new UnsupportedOperationException();
    }

    /**
     * Factory for new client tasks.
     * 
     * @param notifyProxy
     *            The proxy for the object to which the client must deliver
     *            notice of success or failure for each processed resource.
     * @param locator
     *            The locator for the client on which the task will be executed.
     * 
     * @return The client task.
     */
    abstract protected T newClientTask(INotifyOutcome<V, L> notifyProxy,
            final L locator);

}
