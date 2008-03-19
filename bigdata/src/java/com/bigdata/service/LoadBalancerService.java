package com.bigdata.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
import com.bigdata.service.DataService.StatusTask;
import com.bigdata.service.mapred.IMapService;
import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * The {@link LoadBalancerService} collects a variety of performance counters
 * from hosts and services, identifies over- and under- utilized hosts and
 * services based on the collected data and reports those to {@link DataService}s
 * so that they can auto-balance, and acts as a clearning house for WARN and
 * URGENT alerts for hosts and services.
 * <p>
 * While the {@link LoadBalancerService} MAY observe service start/stop events,
 * it does NOT get directly informed of actions that change the load
 * distribution, such as index partition moves or reading from a failover
 * service. Instead, {@link DataService}s determine whether or not they are
 * overloaded and, if so, query the {@link LoadBalancerService} for the identity
 * of under-utilized services. If under-utilized {@link DataService}s are
 * reported by the {@link LoadBalancerService} then the {@link DataService} will
 * self-identify index partitions to be shed and move them onto the identified
 * under-utilized {@link DataService}s. The {@link LoadBalancerService} learns
 * of these actions solely through their effect on host and service load as
 * self- reported by various services.
 * 
 * @todo All clients ({@link IBigdataClient}, {@link DataService},
 *       {@link IMapService}, etc) should issue WARN and URGENT notices. The
 *       client-side rules for those alerts should be configurable / pluggable /
 *       declarative. It would be great if the WARN and URGENT notices were able
 *       to carry some information about the nature of the emergency.
 * 
 * FIXME Compute {@link ServiceScore}s over the aggregated data. There could be
 * a score for the last minute, hour, and day or the last minute, five minutes,
 * and ten minutes. For starters, we can just run some hand-coded rules.
 * Consider special transition states for new hosts and services.
 * 
 * @todo should this service (in its Jini realization) track the discovered data
 *       services or base its decisions solely on reporting by the data
 *       services? note that services will have to
 *       {@link #notify(String, String))} as the first thing they do so that
 *       they quickly become available for assignment unless we are also doing
 *       service discovery.
 * 
 * @todo logging on this {@link ILoadBalancerService#log} can provide a single
 *       point for administrators to configure email or other alerts.
 * 
 * @see IRequiredHostCounters, A core set of variables to support
 *      decision-making.
 * 
 * @todo The decision-making logic itself should be pluggable so that people can
 *       reply on the data that they have for their platform(s) that seems to
 *       best support decision-making and can apply rules for their platforms,
 *       environment, and applications which provide the best overall QOS.
 * 
 * @see http://www.google.com/search?hl=en&q=load+balancing+jini
 * 
 * @todo SNMP
 *       <p>
 *       http://en.wikipedia.org/wiki/Simple_Network_Management_Protocol
 *       <p>
 *       http://www.snmp4j.org/
 *       <p>
 *       http://sourceforge.net/projects/joesnmp/ (used by jboss)
 *       <p>
 *       http://net-snmp.sourceforge.net/ (Un*x and Windows SNMP support).
 * 
 * @todo Send {@link Runtime#availableProcessors()},
 *       {@link Runtime#maxMemory()} as part of the per-process counters.
 * 
 * @todo performance testing links: Grinder, etc.
 *       <p>
 *       http://www.opensourcetesting.org/performance.php
 * 
 * @todo Consider an SNMP adaptor for the clients so that they can report to
 *       SNMP aware applications. In this context we could report both the
 *       original counters (as averaged) and the massaged metrics on which we
 *       plan to make decisions.
 */
abstract public class LoadBalancerService implements ILoadBalancerService,
        IServiceShutdown {

    /**
     * Lock is used to control access to data structures that are not
     * thread-safe.
     */
    final protected ReentrantLock lock = new ReentrantLock();

    /**
     * Used to await a service join when there are no services.
     */
    final protected Condition joined = lock.newCondition();

    /**
     * @todo get rid of hosts that are no longer active. e.g., we no longer
     *       receive {@link #notify(String, byte[])} events from the host and
     *       the host can not be pinged.
     */
    protected Map<String/*hostname*/,HostScore> hosts = new ConcurrentHashMap<String,HostScore>();

    /**
     * The set of known services and some metadata about those services.
     */
    protected Map<UUID/* serviceUUID */, ServiceScore> services = new ConcurrentHashMap<UUID, ServiceScore>();

    /**
     * Scores for the services in ascending order (least utilized to most
     * utilized).
     * <p>
     * This array is initially <code>null</code> and gets updated periodically
     * by the {@link UpdateTask}. The methods that report service utilization
     * and under-utilized services are all based on the data in this array.
     * Since services can leave at any time, that logic MUST also test for
     * existence of the service in {@link #services} before assuming that the
     * service is still live.
     * 
     * @todo do we need a HostScore also?
     */
    protected ServiceScore[] scores = null;
    
    /**
     * Aggregated performance counters for all hosts and services. The services
     * are all listed under their host, e.g., <code>hostname/serviceUUID</code>
     */
    protected CounterSet counters = new CounterSet();

    /**
     * Per-host metadata.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * @todo do we need per-host scoring?  I expect so.
     */
    protected class HostScore {

        public final String hostname;

        public HostScore(String hostname) {
            
            assert hostname != null;
            
            assert hostname.length() > 0;
            
            this.hostname = hostname;
            
        }
        
    }
    
    /**
     * Per-service metadata and a score for that service which gets updated
     * periodically by the {@link UpdateTask}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ServiceScore  {
        
        public final String hostname;
        
        public final UUID serviceUUID;
        public double rawScore = 0d;
        public double score = 0d;
        public int rank = -1;
        public double drank = 0d;
        
        public ServiceScore(UUID serviceUUID, String hostname) {
            
            assert hostname != null;
            
            assert hostname.length() > 0;
            
            this.hostname = hostname;
         
            assert serviceUUID != null;
            
            this.serviceUUID = serviceUUID;
            
        }
        
    }

    /**
     * Return the canonical hostname in the context of a RMI request. This is
     * used to determine the host associated with a service in
     * {@link #join(UUID)}, {@link #notify(String, byte[])}, etc. If the
     * request is not remote then return the canonical hostname for this host.
     */
    abstract protected String getClientHostname();
    
    /**
     * Runs a periodic {@link UpdateTask}.
     */
    final protected ScheduledExecutorService updateService;
    
    /**
     * Options understood by the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
        /**
         * The delay between scheduled invocations of the {@link StatusTask}.
         * 
         * @see #DEFAULT_UPDATE_DELAY
         */
        public final static String UPDATE_DELAY = "statusDelay";
        
        /**
         * The default {@link #UPDATE_DELAY}.
         */
        public final static String DEFAULT_UPDATE_DELAY = ""+(60*1000);
    
    }

    /**
     * 
     * @param properties
     */
    public LoadBalancerService(Properties properties) {
        
        // setup scheduled runnable for periodic status messages.
        {

            final long initialDelay = 100;
            
            final long delay = Long.parseLong(properties.getProperty(
                    Options.UPDATE_DELAY,
                    Options.DEFAULT_UPDATE_DELAY));

            log.info(Options.UPDATE_DELAY + "=" + delay);
            
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            updateService = Executors
            .newSingleThreadScheduledExecutor(DaemonThreadFactory
                    .defaultThreadFactory());
            
            updateService.scheduleWithFixedDelay(new UpdateTask(), initialDelay,
                    delay, unit);

        }
        
    }
    
    public void shutdown() {

        log.info("begin");
        
        updateService.shutdown();

        log.info("done");

    }

    public void shutdownNow() {

        log.info("begin");
        
        updateService.shutdownNow();


    }

    /**
     * Updates the set of under-utilized hosts and services based on an
     * examination of aggregated performance counters.
     * 
     * @todo this should run pluggable logic.
     * 
     * @todo if a client does not
     *       {@link ILoadBalancerService#notify(String, byte[])} for 120 seconds
     *       then presume dead?
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class UpdateTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(UpdateTask.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
                .toInt();

        public UpdateTask() {
        }
        
        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {

            try {

                // FIXME update set of under-utilized hosts and services.
                
            } catch (Throwable t) {

                log.warn("Problem in update task?", t);

            }

        }

    }

    /**
     * Notify the {@link LoadBalancerService} that a new service is available.
     * <p>
     * Note: You CAN NOT use this method to handle a Jini service join since you
     * will not have access to the hostname. The hostname is only available when
     * the service itself uses {@link #notify(String, byte[])} for the first
     * time.
     * 
     * @param serviceUUID
     * 
     * @todo Jini is not restricted to host-based protocols, but may be there is
     *       a way to get the hostname from the service item?  If you assume that
     *       it is a TCP connection?
     */
    protected void join(UUID serviceUUID,String hostname) {
        
        log.info("serviceUUID="+serviceUUID+", hostname="+hostname);
        
        lock.lock();
        
        try {

            if(services.containsKey(serviceUUID)) {
             
                log.warn("Already joined: serviceUUID="+serviceUUID+", hostname="+hostname);
                
                return;
                
            }

            log.info("New service joined: serviceUUID="+serviceUUID+", hostname="+hostname);
            
            /*
             * Create history in counters - path is /host/serviceUUID.
             */
            counters.makePath(hostname+ICounterSet.pathSeparator+serviceUUID);
            
            /*
             * Add to set of known services.
             */
            services.put(serviceUUID, new ServiceScore(serviceUUID,hostname));

            joined.signal();
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Notify the {@link LoadBalancerService} that a service is no longer available.
     * 
     * @param serviceUUID
     */
    protected void leave(UUID serviceUUID) {
        
        log.info("serviceUUID="+serviceUUID);
        
        lock.lock();
        
        try {

            final ServiceScore info = services.get(serviceUUID);

            if (info == null) {

                log.warn("No such service? serviceUUID=" + serviceUUID);

                return;
                
            }
            
            // @todo remove history from counters - path is /host/serviceUUID
//            root.deletePath(path);
            
            services.remove(serviceUUID);
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    final private IInstrumentFactory instrumentFactory = new IInstrumentFactory() {

        public IInstrument newInstance(Class type) {

            if(type==null) throw new IllegalArgumentException();
            
            if (type == Double.class || type == Float.class) {

                return new HistoryInstrument<Double>(new Double[] {});
                
            } else if (type == Long.class || type == Integer.class) {
                
                return new HistoryInstrument<Long>(new Long[] {});
                
            } else if( type == String.class ) {
                
                return new Instrument<String>() {
                    public void sample() {}
                };
                
            } else {
                
                throw new UnsupportedOperationException("type: "+type);
                
            }
            
        }
      


    };
    
    public void notify(String msg, byte[] data) {

        setupLoggingContext();
        
        try {
        
            log.info(msg);

            try {
                
                // read into our local history.
                counters.readXML(new ByteArrayInputStream(data),
                        instrumentFactory, null/* filter */);
                
            } catch (Exception e) {

                log.warn(e.getMessage(), e);
                
                throw new RuntimeException(e);
                
            }
        
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    public void warn(String msg) {
        
        setupLoggingContext();

        try {

            log.warn(msg);

        } finally {

            clearLoggingContext();

        }
        
    }

    public void urgent(String msg) {

        try {

            log.error(msg);

        } finally {
            
            clearLoggingContext();
            
        }
                
    }
    
    public UUID getUnderUtilizedDataService() throws IOException, TimeoutException, InterruptedException {
        
        return getUnderUtilizedDataServices(1, 1, null/* exclude */)[0];
        
    }

    public UUID[] getUnderUtilizedDataServices(int minCount, int maxCount,
            UUID exclude) throws IOException, TimeoutException, InterruptedException {

        try {
            
            if (minCount < 0)
                throw new IllegalArgumentException();

            if (maxCount < 0)
                throw new IllegalArgumentException();

            lock.lock();

            // timeout in milliseconds.
            final long timeout = 3 * 1000;

            long begin = System.currentTimeMillis();

            try {

                if (scores == null && minCount > 0) {

                    // Scores are not available immediately.

                    while (true) {

                        final long elapsed = System.currentTimeMillis() - begin;

                        if (elapsed > timeout)
                            throw new TimeoutException();

                        // all services that we know about right now.
                        final UUID[] knownServiceUUIDs = services.keySet()
                                .toArray(new UUID[] {});

                        if (knownServiceUUIDs.length == 0) {

                            // await a join.
                            joined.await(100, TimeUnit.MILLISECONDS);

                            continue;

                        }

                        /*
                         * Scan and see if we have anything left after verifying
                         * that the service is still on our "live" list and
                         * after excluding this the option [exclude] service.
                         */

                        int nok = 0;

                        for (int i = 0; i < knownServiceUUIDs.length; i++) {

                            if (exclude != null
                                    && exclude.equals(knownServiceUUIDs[i])) {

                                knownServiceUUIDs[i] = null;

                                continue;

                            }

                            if (!services.containsKey(knownServiceUUIDs[i])) {

                                knownServiceUUIDs[i] = null;

                                continue;

                            }

                            nok++;

                        }

                        if (nok <= 0) {

                            // await a join.
                            joined.await(100, TimeUnit.MILLISECONDS);

                            continue;

                        }

                        /*
                         * Now that we have at least one UUID, populate the
                         * return array.
                         * 
                         * Note: We return at least minCount UUIDs, even if we
                         * have to return the same UUID in each slot.
                         */

                        final UUID[] uuids = new UUID[Math.max(minCount, nok)];

                        int n = 0, i = 0;

                        while (n < nok) {

                            UUID tmp = knownServiceUUIDs[i++
                                    % knownServiceUUIDs.length];

                            if (tmp == null)
                                continue;

                            uuids[n++] = tmp;

                        }

                        return uuids;

                    }

                }

                // FIXME Handle the case when we have the scores on hand.
                throw new UnsupportedOperationException();

            } finally {

                lock.unlock();

            }

        } finally {

            clearLoggingContext();

        }
        
    }

    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * This implementation add the "serviceUUID" parameter to the {@link MDC}.
     * The serviceUUID is, in general, assigned asynchronously by the service
     * registrar. Once the serviceUUID becomes available it will be added to the
     * {@link MDC}. This datum can be injected into log messages using
     * %X{serviceUUID} in your log4j pattern layout.
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceUUID", serviceUUID.toString());

        } catch(Throwable t) {
            /*
             * Ignore.
             */
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceUUID");
        
    }

//  /**
//  * Return the UUID of an under utilized data service.
//  * 
//  * this is just an arbitrary instance and does not consider
//  *       utilization.
//  */
// public UUID getUnderUtilizedDataService() throws IOException {
//
//     ServiceItem item = server.dataServiceLookupCache.lookup(null);
//
//     log.info(item.toString());
//
//     return JiniUtil.serviceID2UUID(item.serviceID);
//     
// }
//
// /**
//  * this does not pay attention to utilization and there is no
//  *       guarentee that it is reporting the available data services in a
//  *       round-robin fashion.  This method probably needs to be moved to
//  *       a service that is specialized in load balancing the hosts and
//  *       services in the federation.
//  */
// public UUID[] getUnderUtilizedDataServices(int limit, final UUID exclude) throws IOException {
//
//     ServiceItemFilter filter = exclude != null ? new ServiceItemFilter() {
//
//         public boolean check(ServiceItem arg0) {
//
//             if (exclude != null
//                     && JiniUtil.serviceID2UUID(arg0.serviceID).equals(
//                             exclude)) {
//
//                 return false;
//
//             }
//             
//             return true;
//
//         }
//
//     }
//             : null;
//     
//     ServiceItem[] items = server.dataServiceLookupCache.lookup(filter,
//             limit);
//
//     final List<UUID> a = new ArrayList<UUID>(limit);
//     
//     for(int i=0; i<items.length; i++) {
//         
//         UUID uuid = JiniUtil.serviceID2UUID(items[i].serviceID);
//         
//         a.add(uuid);
//         
//     }
//     
//     return a.toArray(new UUID[a.size()]);
//     
// }

}
