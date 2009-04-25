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
 * Created on Oct 15, 2008
 */

package com.bigdata.service.jini;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.bigdata.journal.Journal.Options;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.jini.util.JiniServicesHelper;
import com.bigdata.test.ExperimentDriver;
import com.bigdata.test.ExperimentDriver.IComparisonTest;
import com.bigdata.test.ExperimentDriver.Result;
import com.bigdata.util.InnerCause;
import com.bigdata.util.NV;

/**
 * A harness for performance tests for a jini-federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PerformanceTest //extends ProxyTestCase<AbstractScaleOutFederation>
        implements IComparisonTest {

    protected static final Logger log = Logger.getLogger(PerformanceTest.class);
    
    public PerformanceTest() {

        super();
        
    }

    public PerformanceTest(String name) {
        
//        super(name);
        
    }
    
    /**
     * Options understood by this stress test.
     * <p>
     * Note: The best performance was observed for all values of the payload
     * size (0,100,1000,10000) when the period was 3ms. That yielded between 280
     * and 330 operations per second. Performance falls off if the period is
     * increased. If the period is decreased, then there is a substantial risk
     * that the JVM will exhaust its resources. Since we do not directly control
     * the inter-submit period in normal applications, this only gives us an
     * upper bound for throughput. (Note that all services resided within the
     * same JVM for these results, so it is possible that RMI was cheating and
     * hence that (de-)serialization costs were not paid - the JVM was JRockit,
     * so that is perhaps even more likely.)
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    interface TestOptions extends Options {
        
        /**
         * A task will be submitted every period ms.
         */
        String PERIOD = "test.period";
        
        /**
         * The timeout for the test (ms).
         */
        String TIMEOUT = "test.timeout";
        
        /**
         * The #of data bytes carried by the {@link WorkloadTask}.
         */
        String PAYLOAD_SIZE = "test.nbytes";

    }
    
    /**
     * Run the stress test configured in the code.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        final Properties properties = new Properties();

        properties.setProperty(TestOptions.PERIOD,"1");
        properties.setProperty(TestOptions.TIMEOUT,"5000");
        properties.setProperty(TestOptions.PAYLOAD_SIZE,"100");

        final PerformanceTest test = new PerformanceTest("PerformanceTest");
        
         test.setUpComparisonTest(properties);
//        test.setUp();
        
        try {

            System.err.println("Running test");
            
            test.doComparisonTest(properties);
        
        } finally {

            try {
                
                test.tearDownComparisonTest();
//                test.tearDown();
                
            } catch(Throwable t) {

                log.warn("Tear down problem: "+t, t);
                
            }
            
        }

    }
    
    /**
     * Experiment generation utility class.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class GenerateExperiment extends ExperimentDriver {
        
        /**
         * Generates an XML file that can be run by {@link ExperimentDriver}.
         * 
         * @param args
         */
        public static void main(String[] args) throws Exception {
            
            // this is the test to be run.
            String className = PerformanceTest.class.getName();
            
            Map<String,String> defaultProperties = new HashMap<String,String>();

            /* 
             * Set defaults for each condition.
             */
            
            defaultProperties.put(TestOptions.TIMEOUT,"5000");

            List<Condition>conditions = new ArrayList<Condition>();
            
            conditions.add(new Condition(defaultProperties));

            conditions = apply(conditions, new NV[] {
                    new NV(TestOptions.PERIOD, "1"),
                    new NV(TestOptions.PERIOD, "2"),
                    new NV(TestOptions.PERIOD, "3"),
                    new NV(TestOptions.PERIOD, "4"),
                    new NV(TestOptions.PERIOD, "5"),
                    });
            
            conditions = apply(conditions, new NV[] {
                    new NV(TestOptions.PAYLOAD_SIZE, "0"),
                    new NV(TestOptions.PAYLOAD_SIZE, "100"),
                    new NV(TestOptions.PAYLOAD_SIZE, "1000"),
                    new NV(TestOptions.PAYLOAD_SIZE, "10000"),
                    });
            
//            // co-vary the core pool size and the queue capacity.
//            conditions = apply(conditions, new NV[][] {
//                    new NV[] {
//                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
//                                    "500"),
//                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
//                                    "500"),
//                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
//                                    "500") },
//                    new NV[] {
//                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
//                                    "1000"),
//                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
//                                    "1000"),
//                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
//                                    "1000") },
//                    new NV[] {
//                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
//                                    "1500"),
//                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
//                                    "1500"),
//                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
//                                    "1500") },
//                    new NV[] {
//                            new NV(TestOptions.WRITE_SERVICE_CORE_POOL_SIZE,
//                                    "2000"),
//                            new NV(TestOptions.WRITE_SERVICE_MAXIMUM_POOL_SIZE,
//                                    "2000"),
//                            new NV(TestOptions.WRITE_SERVICE_QUEUE_CAPACITY,
//                                    "2000") }
//                    });
            
//            conditions = apply(conditions, new NV[] {
//                    new NV(TestOptions.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "true"),
//                    new NV(TestOptions.WRITE_SERVICE_PRESTART_ALL_CORE_THREADS, "false"),
//                    });
            
//            conditions = apply(conditions, new NV[][] {
////                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Transient.toString())},
////                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Direct.toString())},
//                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Disk.toString())},
////                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Disk.toString()),
////                                    new NV(TestOptions.FORCE_ON_COMMIT,ForceEnum.No.toString())},
////                    new NV[]{new NV(TestOptions.BUFFER_MODE, BufferMode.Mapped.toString())},
//                    });
            
            Experiment exp = new Experiment(className,defaultProperties,conditions);

            // copy the output into a file and then you can run it later.
            System.err.println(exp.toXML());

        }
        
    }

    /**
     * A performance test for the #of RMI calls that may be issued as a function
     * of the thread count and and payload size. The tasks will be run on the
     * {@link IBigdataFederation} for the target data service (they do not
     * address indices, but are simply executed and return their Future). The
     * tasks themselves are NOPs. The tasks will return a proxy for their Future
     * when run using the Jini framework.
     * <p>
     * Note: You can control the parallelism with with the client executes the
     * tasks using
     * {@link IBigdataClient.Options#CLIENT_MAX_PARALLEL_TASKS_PER_REQUEST}.
     * This is safer than placing a limit directly on the thread pool using
     * {@link IBigdataClient.Options#CLIENT_THREAD_POOL_SIZE}.
     */
    public Result doComparisonTest(Properties properties) throws Exception {

        /*
         * Note: This is fixed at zero since any delay here decreases the
         * apparent throughput.
         */
        final long initialDelay = 0L;
        
//        final long initialDelay = Long.parseLong(properties
//                .getProperty(TestOptions.INITIAL_DELAY));

        final long period = Long.parseLong(properties
                .getProperty(TestOptions.PERIOD));

        final long timeout = Long.parseLong(properties
                .getProperty(TestOptions.TIMEOUT));

        final int payloadSize = Integer.parseInt(properties
                .getProperty(TestOptions.PAYLOAD_SIZE));

        // we are using two data services - wait for both of them to be
        // discovered.
        final int minDataServices = 2; // @todo param
        final UUID[] dataServiceUUIDs = fed.awaitServices(minDataServices,
                10000/* ms */);

        System.err
                .println("Have " + dataServiceUUIDs.length + " data services");

        final Random r = new Random();

        final byte[] data = new byte[payloadSize];
        r.nextBytes(data);

        /*
         * Note: We run both the task that consumes the futures and the
         * scheduled task that submits the workload tasks on this service so
         * we need at least two threads in the thread pool.
         */
        final ScheduledExecutorService submitService = new ScheduledThreadPoolExecutor(
                2/*size*/);

        ScheduledFuture submitFuture = null;

        // used to collect futures.
        final LinkedBlockingQueue<Future> futuresQueue = new LinkedBlockingQueue<Future>();
        
        try {

            final AtomicInteger nsubmiterrs = new AtomicInteger();
            final AtomicInteger nsubmitted = new AtomicInteger();
            final AtomicInteger ncomplete = new AtomicInteger();
            final AtomicInteger nerror = new AtomicInteger();
            final AtomicInteger nok = new AtomicInteger();

            /*
             * Submit task that will consume the futures of the workload tasks
             * that we run on the data services.
             */
            final Future consumeFuture = submitService.submit(new Runnable() {

                public void run() {

                    try {

                        while (!Thread.interrupted()) {

                            final Future f = futuresQueue.take();

                            ncomplete.incrementAndGet();

                            try {

                                f.get();

                                nok.incrementAndGet();
                                
//                                System.out.print("d"); // Ok

                            } catch (ExecutionException ex) {

                                nerror.incrementAndGet();

                            }

                        }

                    } catch (InterruptedException ex) {

                        // done.

                    }

                }

            });

            /*
             * Submit scheduled task that will submit the tasks to be run on the
             * various data services at a scheduled rate.
             */

            final long begin = System.currentTimeMillis();

            submitFuture = submitService.scheduleAtFixedRate(new Runnable() {

                public void run() {

                    final long elapsed = System.currentTimeMillis() - begin;

//                    System.err.println("elapsed="+elapsed);
                    
                    if (elapsed >= timeout) {

                        System.err.println("timeout: elapsed="+elapsed);
                        
                        // interrupt the task consuming the workload futures.
                        consumeFuture.cancel(true/* mayInterruptIfRunning */);

                        // stop submitting new tasks.
                        throw new RuntimeException(new InterruptedException("Done"));

                    }

                    final UUID serviceUUID = dataServiceUUIDs[r
                            .nextInt(dataServiceUUIDs.length)];

                    final IDataService service = fed
                            .getDataService(serviceUUID);

//                    System.out.print("s"); // submit to client's service.

                    fed.getExecutorService().submit(new Runnable() {

                        public void run() {

                            try {

                                futuresQueue.add(service
                                        .submit(new WorkloadTask(data)));

                                nsubmitted.incrementAndGet();

//                                System.out.print("S"); // submit to data service.

                            } catch (Throwable t) {

                                nsubmiterrs.incrementAndGet();

                                log.warn(t);

                            }
                        }
                    });

                }

            }, initialDelay, period, TimeUnit.MILLISECONDS);

            try {
                // wait for the scheduled submit task to terminate.
                System.err.println("Waiting: timeout="+timeout+"ms");
                submitFuture.get();
            } catch (ExecutionException ex) {
                if (InnerCause.isInnerCause(ex, InterruptedException.class)) {
                    System.err.println("Test was interrupted");
                } else {
                    log.error(ex);
                    throw ex;
                }
            }
            
            /*
             * the actual run time.
             */
            final long elapsed = System.currentTimeMillis() - begin;

            System.err.println("Done: elapsed=" + elapsed);
            
            // #of tasks per second.
            final double tasksPerSecond = ncomplete.get() * 1000d / elapsed;

            Result result = new Result();

            result.put("nbytes", ""+payloadSize);
            
            result.put("largestPoolSize", ""+((ThreadPoolExecutor)fed.getExecutorService()).getLargestPoolSize());
            result.put("nsubmiterrs", "" + nsubmiterrs);
            result.put("nsubmitted", "" + nsubmitted);
            result.put("ncomplete", "" + ncomplete);
            result.put("nerror", "" + nerror);
            result.put("nok", "" + nok);
            result.put("elapsed", "" + elapsed);
            result.put("tasks/sec", "" + tasksPerSecond);

            System.err.println(result.toString(true/* newline */));

            return result;

        } finally {

            // terminate the task
            if (submitFuture != null) {

                submitFuture.cancel(true/* mayInterruptIfRunning */);

            }

            // shutdown the submit service.
            submitService.shutdownNow();

        }

    }
    
    /**
     * Task submitted to the remote {@link DataService}s.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class WorkloadTask implements Callable<Object>, Serializable {

        /**
         * 
         */
        private static final long serialVersionUID = -8127096462411567648L;

        private final byte[] data;
        
        public WorkloadTask(byte[] data) {

            this.data = data;
            
        }

        public Object call() throws Exception {

//            System.err.print("r"); // run.
            
            return null;

        }
        
    }
    
    private final String path = "src/resources/config/standalone/";
    private JiniServicesHelper services;
    private JiniFederation fed; 

    public void setUpComparisonTest(Properties properties) throws Exception {

        services = new JiniServicesHelper(path);
        
        try {
        
            services.start();

            System.err.println("Services are up.");
            
            fed = services.client.connect();

            System.err.println("Client is connected.");

        } catch (Throwable t) {
            
            log.error("Could not start: " + t);
            
            services.destroy();
            
            throw new RuntimeException("Could not start", t);
                        
        }
        
    }

    /**
     * Shutdown the federation.
     * <p>
     * This is an immediate shutdown and destroy. It does not wait for all tasks
     * to complete. You should therefore ignore the messy warnings and errors
     * generated after destroy() is called!
     */
    public void tearDownComparisonTest() throws Exception {

        System.err.println("Will destroy services");
        
        try {

            services.destroy();
            
        } catch (Throwable t) {
            
            log.error(t);
            
        }

    }
    
}
