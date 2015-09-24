/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on Feb 3, 2012
 */

package com.bigdata.service.jini.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.channels.ClosedByInterruptException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import net.jini.config.ConfigurationException;

import org.apache.log4j.Logger;

import com.bigdata.config.Configuration;
import com.bigdata.counters.CAT;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.util.DaemonThreadFactory;
import com.bigdata.util.InnerCause;

/**
 * A utility class designed to measure the message passing rate among the
 * services in the {@link IBigdataFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class MessageTest {

    protected static final Logger log = Logger.getLogger(MessageTest.class);
    
    /**
     * The component name for this class (for use with the
     * {@link ConfigurationOptions}).
     */
    public static final String COMPONENT = MessageTest.class.getName(); 

    /**
     * {@link Configuration} options for this class.
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface ConfigurationOptions {

        /**
         * How long to wait in milliseconds for service discovery.
         */
        String DISCOVERY_DELAY = "discoveryDelay";
   
        long DEFAULT_DISCOVERY_DELAY = 5000; // ms
        
        /**
         * The duration of the test in milliseconds.
         */
        String DURATION = "duration";

        long DEFAULT_DURATION = 5000L; // ms
        
        /**
         * The #of messages which each service will send.
         */
        String MESSAGES = "messages";

        long DEFAULT_MESSAGES = 10000L;
        
        /**
         * The #of messages which each service will send in parallel (the size
         * of the per-service thread pool used to send out messages and await
         * responses) (non-negative integer).
         */
        String PARALLEL = "parallel";
        
        int DEFAULT_PARALLEL = 50;

        /**
         * The size (in bytes) of the additional payload data sent with the RMI
         * request (non-negative integer).
         */
        String PAYLOAD = "payload";
        
        int DEFAULT_PAYLOAD = 0;
        
    }
    
    /**
     * Run a message traffic test on the federation.
     * <p>
     * <strong>Jini MUST be running</strong>
     * <p>
     * <strong>You MUST specify a sufficiently lax security policy</strong>,
     * e.g., using <code>-Djava.security.policy=policy.all</code>, where
     * <code>policy.all</code> is the name of a policy file.
     * 
     * @param args
     *            The name of the configuration file for the jini client that
     *            will be used to connect to the federation.
     * 
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws IOException
     * @throws TimeoutException
     * @throws ConfigurationException 
     */
    static public void main(final String[] args) throws InterruptedException,
            ExecutionException, IOException, TimeoutException,
            ConfigurationException {

        if (args.length == 0) {

            System.err.println("usage: <client-config-file>");

            System.exit(1);

        }

        /*
         * Connect to an existing jini federation.
         */
        final JiniClient<?> client = JiniClient.newInstance(args);

        final JiniFederation<?> fed = client.connect();

        final long discoveryDelay = (Long) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.DISCOVERY_DELAY,
                        Long.TYPE, ConfigurationOptions.DEFAULT_DISCOVERY_DELAY);

        final long duration = (Long) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.DURATION, Long.class,
                        ConfigurationOptions.DEFAULT_DURATION);

        final long messages = (Long) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.MESSAGES, Long.class,
                        ConfigurationOptions.DEFAULT_MESSAGES);

        final int parallel = (Integer) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.PARALLEL, Integer.class,
                        ConfigurationOptions.DEFAULT_PARALLEL);
        
        final int payload = (Integer) fed
                .getClient()
                .getConfiguration()
                .getEntry(COMPONENT, ConfigurationOptions.PAYLOAD, Integer.class,
                        ConfigurationOptions.DEFAULT_PAYLOAD);

        try {

            /*
             * Wait until we have the metadata service
             */
            if (log.isInfoEnabled())
                log.info("Waiting up to " + discoveryDelay
                        + "ms for metadata service discovery.");

            /*
             * Wait up to the timeout. We need at least 2 data services for the
             * test.
             */
            fed.awaitServices(//
                    2,// minDataServices
                    discoveryDelay// timeout(ms)
            );

            // Get UUIDs for the discovered data services.
            final UUID[] uuids = fed.getDataServiceUUIDs(0/*maxCount*/);

            // Resolve proxies for the discovered data services.
            final IDataService[] dataServices = new IDataService[uuids.length];

            for (int i = 0; i < uuids.length; i++) {

                dataServices[i] = fed.getDataService(uuids[i]);
                
            }
 
            final List<Callable<Long>> tasks = new ArrayList<Callable<Long>>(
                    dataServices.length);

            for (int i = 0; i < dataServices.length; i++) {

                tasks.add(new MessageTask(duration, messages, parallel,
                        payload, dataServices));

            }
            
            final long begin = System.currentTimeMillis();
            
            final Collection<Future<Long>> futures = fed.getExecutorService()
                    .invokeAll(tasks, duration, TimeUnit.MILLISECONDS);

            long nmessages = 0L;

            int failedServices = 0;

            for (Future<Long> f : futures) {

                try {

                    nmessages += f.get();

                } catch (ExecutionException ex) {

                    failedServices++;

                    log.error("Failure: " + ex, ex);
                    
                }

            }

            final long elapsed = System.currentTimeMillis() - begin;

            System.out.println("#services=" + dataServices.length
                    + ", elapsedMillis=" + elapsed + ", #messagesOk="
                    + nmessages + ", #failedServices=" + failedServices
                    + ", rate="
                    + (elapsed == 0 ? 0 : nmessages / (double) elapsed));

        } finally {

            client.disconnect(false/* immediateShutdown */);

        }
        
    }

    /**
     * A task which is sumitted to a service. The service then exchanges simple
     * messages at the maximum rate for the specified duration with all of the
     * other services. The task reports the #of messages which were successfully
     * exchanged. Any errors will terminate the task.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class MessageTask implements Callable<Long> {

        private final long duration;

        private final long messages;
        
        private final int parallel;
        
        private final int payload;
        
        private final IDataService[] dataServices;

        public MessageTask(final long duration, final long messages,
                final int parallel, final int payload,
                final IDataService[] dataServices) {

            if (duration <= 0)
                throw new IllegalArgumentException();

            if (messages <= 0)
                throw new IllegalArgumentException();

            if (parallel <= 0)
                throw new IllegalArgumentException();

            if (payload < 0)
                throw new IllegalArgumentException();

            if (dataServices == null)
                throw new IllegalArgumentException();

            if (dataServices.length < 2)
                throw new IllegalArgumentException();

            for (int i = 0; i < dataServices.length; i++) {
                
                if (dataServices[i] == null)
                    throw new IllegalArgumentException();
            
            }

            this.duration = duration;

            this.messages = messages;
            
            this.parallel = parallel;
            
            this.payload = payload;
            
            this.dataServices = dataServices;

        }
       
        /**
         * Pumps tasks into a fixed thread pool until done or interrupted.
         */
        @Override
        public Long call() throws Exception {

            final ExecutorService service = Executors.newFixedThreadPool(
                    parallel, new DaemonThreadFactory("MessageTest"));

            try {

                final long begin = System.currentTimeMillis();

                final CAT nsent = new CAT();
                final CAT ndone = new CAT();
                final CAT nerrors = new CAT();
                final AtomicBoolean running = new AtomicBoolean(true);

                long elapsed;

                while (running.get()
                        && (elapsed = (System.currentTimeMillis() - begin)) < duration
                        && nsent.estimate_get() < messages) {

                    for (int i = 0; i < dataServices.length; i++) {

                        final IDataService ds = dataServices[i];

                        service.execute(new Runnable() {
                            public void run() {
                                try {
                                    final Future<?> f = ds.submit(new NOpTask(
                                            payload));
                                    nsent.increment();
                                    f.get();
                                    ndone.increment();
                                } catch (RemoteException e) {
                                    if (InnerCause.isInnerCause(e,
                                            RejectedExecutionException.class)
                                            || InnerCause.isInnerCause(e,
                                                    InterruptedException.class)
                                            || InnerCause
                                                    .isInnerCause(
                                                            e,
                                                            ClosedByInterruptException.class)) {
                                        if (running
                                                .compareAndSet(
                                                        false/* expect */, true/* update */)) {
                                            log.warn("Interrupted - will halt.");
                                        }
                                    } else {
                                        nerrors.increment();
                                        log.error(e, e);
                                    }
                                } catch (InterruptedException e) {
                                    if (running
                                            .compareAndSet(false/* expect */,
                                                    true/* update */)) {
                                        log.warn("Interrupted - will halt.");
                                    }
                                } catch (ExecutionException e) {
                                    if (InnerCause.isInnerCause(e,
                                            RejectedExecutionException.class)
                                            || InnerCause.isInnerCause(e,
                                                    InterruptedException.class)
                                            || InnerCause
                                                    .isInnerCause(
                                                            e,
                                                            ClosedByInterruptException.class)) {
                                        if (running
                                                .compareAndSet(
                                                        false/* expect */, true/* update */)) {
                                            log.warn("Interrupted - will halt.");
                                        }
                                    } else {
                                        nerrors.increment();
                                        log.error(e, e);
                                    }
                                }
                            }
                        });

                    }

                }

                return ndone.get();

            } finally {

                service.shutdownNow();

            }

        } // call()

    } // class MessageTask

    /**
     * A task which does nothing. This serves as a simple message for the test.
     * The payload size is variable and is specified to the constructor.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class NOpTask implements Callable<Void>, Externalizable {

        private int payload;
        
        /**
         * De-serialization constructor.
         */
        public NOpTask() {
            
        }
        
        public NOpTask(final int payload) {

            if (payload < 0)
                throw new IllegalArgumentException();
            
            this.payload = payload;
            
        }
        
        
        @Override
        public Void call() throws Exception {
            // NOP
            return null;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeInt(payload);
            
            for (int i = 0; i < payload; i++) {
            
                out.writeByte(i);
                
            }

        }

        @Override
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {
        
            payload = in.readInt();
            
            if (payload < 0)
                throw new IOException();
            
            for (int i = 0; i < payload; i++) {
            
                in.readByte();
                
            }

        }

    }

}
