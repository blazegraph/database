package com.bigdata.service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IInstrument;
import com.bigdata.counters.Instrument;
import com.bigdata.counters.AbstractStatisticsCollector.IHostCounters;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
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
 * self- reported by various services. *
 * <p>
 * Note: utilization should be defined in terms of transient system resources :
 * CPU, IO (DISK and NET), RAM. DISK exhaustion on the otherhand is the basis
 * for WARN or URGENT alerts since it can lead to immediate failure of all
 * services on the same host.
 * <p>
 * Note: When new services are made available, either on new hosts or on the
 * existing hardware, and service utilization discrepancies should become
 * rapidly apparent (within a few minutes). Once we have collected performance
 * counters for the new hosts / services, a subsequent overflow event(s) on
 * existing {@link DataService}(s) will cause index partition moves to be
 * nominated targetting the new hosts and services. The amount of time that it
 * takes to re-balance the load on the services will depend in part on the write
 * rate, since writes drive overflow events and index partition splits, both of
 * which lead to pre-conditions for index partition moves.
 * <p>
 * Note: If a host is suffering high IOWAIT then it is probably "hot for read"
 * (writes are heavily buffered and purely sequential and therefore unlikely to
 * cause high IOWAIT where as reads are typically random on journals even
 * through a key range scan is sequential on index segments). Therefore a "hot
 * for read" condition should be addressed by increasing the replication count
 * for those service(s) which are being swamped by read requests on a host
 * suffering from high IOWAIT.
 * 
 * @todo try writing move tests using a mock version of this service.
 * 
 * @todo we could significantly accelerate overflow events when new hardware is
 *       made available by setting the forceOverflow flag on the highly utilized
 *       data services. we probably don't want to do this for all highly
 *       utilized services at once since that could cause a lot of perturbation.
 * 
 * @todo Work out high-level alerting for resource exhaustion and failure to
 *       maintain QOS on individual machines, indices, and across the
 *       federation.
 * 
 * @todo All clients ({@link IBigdataClient}, {@link DataService},
 *       {@link IMapService}, etc) should issue WARN and URGENT notices. The
 *       client-side rules for those alerts should be configurable / pluggable /
 *       declarative. It would be great if the WARN and URGENT notices were able
 *       to carry some information about the nature of the emergency.
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
 * @todo refactor so that we can use the same basic infrastructure for load
 *       balancing of other service classes as well, e.g., map/reduce services.
 * 
 * @see IRequiredHostCounters, A core set of variables to support
 *      decision-making.
 * 
 * @todo ping hosts?
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
     * Service join timeout in milliseconds - used when we need to wait for a
     * service to join before we can recommend an under-utilized service.
     * 
     * @todo config
     */
    final long timeout = 3 * 1000;

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
    protected AtomicReference<ServiceScore[]> scores = new AtomicReference<ServiceScore[]>(null);
    
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
    protected class ServiceScore implements Comparable<ServiceScore> {
        
        public final String hostname;
        
        public final UUID serviceUUID;
        
        /** The raw score computed for that service. */
        public final double rawScore;
        /** The normalized score computed for that service. */
        public double score;
        /** The rank in [0:#scored].  This is an index into the Scores[]. */
        public int rank = -1;
        /** The normalized double precision rank in [0.0:1.0]. */
        public double drank = -1d;
        
        /**
         * Constructor variant used when you do not have performance counters
         * for the service and could not compute its rawScore.
         * 
         * @param hostname
         * @param serviceUUID
         */
        public ServiceScore(String hostname, UUID serviceUUID) {

            this(hostname, serviceUUID, 0d);

        }

        /**
         * Constructor variant used when you have computed the rawStore.
         * 
         * @param hostname
         * @param serviceUUID
         * @param rawScore
         */
        public ServiceScore(String hostname, UUID serviceUUID, double rawScore) {
            
            if (hostname == null)
                throw new IllegalArgumentException();
            
            if (serviceUUID == null)
                throw new IllegalArgumentException();
            
            this.hostname = hostname;
         
            this.serviceUUID = serviceUUID;
            
            this.rawScore = rawScore;
            
        }
        
        public String toString() {
            
            return "ServiceScore{hostname=" + hostname + ", serviceUUID="
                    + serviceUUID + ", rawScore=" + rawScore + ", score="
                    + score + ", rank=" + rank + ", drank=" + drank + "}";
            
        }
        
        /**
         * Places elements into order by ascending {@link #rawScore}. The
         * {@link #serviceUUID} is used to break any ties.
         */
        public int compareTo(ServiceScore arg0) {
            
            if(rawScore < arg0.rawScore) {
                
                return -1;
                
            } else if(rawScore> arg0.rawScore ) {
                
                return 1;
                
            }
            
            return serviceUUID.compareTo(arg0.serviceUUID);
            
        }

    }

    /**
     * Return the canonical hostname of the client in the context of a RMI
     * request. This is used to determine the host associated with a service in
     * {@link #notify(String, byte[])}, etc. If the request is not remote then
     * return the canonical hostname for this host.
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
         * The delay between scheduled invocations of the {@link UpdateTask}.
         * <p>
         * Note: the {@link AbstractStatisticsCollector} implementations SHOULD
         * sample at one minute intervals by default and clients SHOULD report
         * the collected performance counters at approproximately one minute
         * intervals. The update rate can be no more frequent than the reporting
         * rate, but could be 2-5x slower, especially if we use WARN and URGENT
         * events to immediately re-score services.
         * 
         * @see #DEFAULT_UPDATE_DELAY
         * 
         * @see AbstractStatisticsCollector.Options#INTERVAL
         */
        public final static String UPDATE_DELAY = "statusDelay";
        
        /**
         * The default {@link #UPDATE_DELAY}.
         */
        public final static String DEFAULT_UPDATE_DELAY = ""+(60*1000);
    
    }

    /**
     * 
     * @param properties See {@link Options}
     */
    public LoadBalancerService(Properties properties) {
        
        // setup scheduled runnable for periodic updates of the service scores.
        {

            final long delay = Long.parseLong(properties.getProperty(
                    Options.UPDATE_DELAY,
                    Options.DEFAULT_UPDATE_DELAY));

            log.info(Options.UPDATE_DELAY + "=" + delay);

            /*
             * Wait a bit longer for the first update task since service may be
             * starting up as well and we need to have the performance counter
             * data on hand before we can do anything.
             */
            final long initialDelay = delay * 2;
            
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

        log.info("done");

    }

    /**
     * Computes and updates the {@link ServiceScore}s based on an examination
     * of aggregated performance counters.
     * 
     * @todo There could be a score for the last minute, hour, and day or the
     *       last minute, five minutes, and ten minutes.
     * 
     * @todo For starters, we can just run some hand-coded rules. Consider
     *       special transition states for new hosts and services.
     * 
     * @todo The scoring logic should be pluggable so that people can reply on
     *       the data that they have for their platform(s) that seems to best
     *       support decision-making and can apply rules for their platforms,
     *       environment, and applications which provide the best overall QOS.
     * 
     * @todo The logic to choose the under- and over-utilized services based on
     *       the services scores should be configurable (this is different from
     *       the logic to compute those scores).
     * 
     * @todo if a client does not
     *       {@link ILoadBalancerService#notify(String, byte[])} for 120 seconds
     *       then presume dead? this requires that we compute the age of the
     *       last reported counter value. e.g., do a counter scan for the
     *       service and report the largest value for lastModified() on any
     *       counter for that service.
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

                if(services.isEmpty()) {
                    
                    log.warn("No active services");
                    
                    LoadBalancerService.this.scores.set( null );
                    
                    return;
                    
                }
                
                /*
                 * Update scores for the active services.
                 */

                final Vector<ServiceScore> scores = new Vector<ServiceScore>();
                
                // For each host
                final Iterator<ICounterSet> itrh = counters.counterSetIterator();
                
                while(itrh.hasNext()) {
                    
                    final CounterSet hostCounterSet = (CounterSet) itrh.next();
                    
                    // Note: name on hostCounterSet is the fully qualified hostname.
                    final String hostname = hostCounterSet.getName();
                    
                    final CounterSet servicesCounterSet = (CounterSet)hostCounterSet.getPath("service");
                    
                    if(servicesCounterSet==null) {
                        
                        log.warn("No services? hostname="+hostname);
                        
                        continue;
                        
                    }
                    
                    // For each service.
                    final Iterator<ICounterSet> itrs = hostCounterSet.counterSetIterator();
                    
                    while(itrs.hasNext()) {

                        final CounterSet serviceCounterSet = (CounterSet) itrh.next();

                        // Note: name on serviceCounterSet is the serviceUUID.
                        final UUID serviceUUID = UUID.fromString(serviceCounterSet.getName());
                        
                        if(!services.containsKey(serviceUUID)) {
                            
                            // Note: path includes the hostname.
                            log.info("Service is not active: "+serviceCounterSet.getPath());
                            
                            continue;
                            
                        }

                        /*
                         * Compute the score for that service.
                         */
                        ServiceScore score;
                        try {

                            score = computeScore(hostname, serviceUUID,
                                    hostCounterSet, serviceCounterSet);

                        } catch (Exception ex) {

                            log.error("Problem computing service score: "
                                    + serviceCounterSet.getPath(), ex);

                            /*
                             * Keep the old score if we were not able to compute
                             * a new score.
                             * 
                             * Note: if the returned value is null then the
                             * service asynchronously was removed from the set
                             * of active services.
                             */
                            score = services.get(serviceUUID);
                            
                            if (score == null) {
                                
                                log.info("Service leave during update task: "
                                        + serviceCounterSet.getPath());
                                
                                continue;
                                
                            }

                        }

                        /*
                         * Add to collection of scores.
                         */
                        scores.add(score);

                    }

                }

                if (scores.isEmpty()) {

                    log.warn("No performance counters for services, but "
                            + services.size() + " active services");
                    
                    LoadBalancerService.this.scores.set( null );
                    
                    return;
                    
                }
                
                // scores as an array.
                final ServiceScore[] a = scores.toArray(new ServiceScore[] {});

                // sort scores into ascending order (least utilized to most
                // utilized).
                Arrays.sort(a);

                /*
                 * compute normalized score, rank, and drank.
                 */
                for (int i = 0; i < a.length; i++) {
                    
                    final ServiceScore score = a[i];
                    
                    score.rank = i;
                    
                    score.drank = ((double)i)/a.length;
                   
                    // update score in global map.
                    services.put(score.serviceUUID, score);

                    log.info(score.toString()); //@todo debug?

                }
                
                log.info("The most active index was: " + a[a.length - 1]);

                log.info("The least active index was: " + a[0]);
                
                // Atomic replace of the old scores.
                LoadBalancerService.this.scores.set( a );

                log.info("Updated scores for "+a.length+" services");
                
            } catch (Throwable t) {

                log.warn("Problem in update task?", t);

            }

        }

        /**
         * Compute the score for a service.
         * 
         * @param hostname
         *            The fully qualified hostname.
         * @param serviceUUID
         *            The service {@link UUID}.
         * @param hostCounterSet
         *            The performance counters for that host.
         * @param serviceCounterSet
         *            The performance counters for that service.
         * @return
         */
        protected ServiceScore computeScore(String hostname, UUID serviceUUID,
                ICounterSet hostCounterSet, ICounterSet serviceCounterSet) {

            /*
             * FIXME compute the score!
             * 
             * Note: utilization is defined in terms of transient system
             * resources : CPU, IO (DISK and NET), RAM.
             * 
             * @todo compute per-host scores first so that we can just reuse
             * those scores here. the host scores could also carry a flag if
             * there is a critical resource issue, e.g., running out of disk,
             * with that host. The other critical issue is swapping, which will
             * suck down performance for the whole host and where the correct
             * response is to alert an admin to shutdown one or more processes
             * on that host (whoops! if you don't have failover - you need to
             * configure the data services to use less RAM in that case or halt
             * some non-federation processes). All other resources fail soft, by
             * causing the response time to increase.
             * 
             * Note: DISK exhaustion is the basis for WARN or URGENT alerts
             * since it can lead to immediate failure of all services on the
             * same host. Still, we need to consider DISK exhaustion if only to
             * mark those services so that they do not get marked as
             * under-utilized.
             * 
             * A host with enough CPU/RAM/IO/DISK can support more than one data
             * service thereby using more than 2G of RAM (a limit on 32 bit
             * hosts). In this case it is important that we can move index
             * partitions between those data services or the additional
             * resources will not be fully exploited. We do this by looking at
             * not just host utilization but also at process utilization, where
             * the process is a data service.
             */

            double percentDiskFreeSpace = getCurrentValue(hostCounterSet,
                    IHostCounters.LogicalDisk_PercentFreeSpace, .5d);
            
            double percentProcessorTime = getLastHourAverageValue(
                    hostCounterSet, IHostCounters.CPU_PercentProcessorTime, .5d);

            double percentIOWait= getLastHourAverageValue(
                    hostCounterSet, IHostCounters.CPU_PercentIOWait, .1d);

            double rawScore = percentProcessorTime;
            
            final ServiceScore score = new ServiceScore(hostname, serviceUUID,
                    rawScore);
            
            return score;
            
        }

        protected double getCurrentValue(ICounterSet counterSet, String path,
                double defaultValue) {

            assert counterSet != null;
            assert path != null;

            ICounter c = (ICounter) counterSet.getPath(path);

            if (c == null)
                return defaultValue;

            try {

                double val = (Double) c.getValue();

                return val;

            } catch (Exception ex) {

                log.warn("Could not read double value: counterSet="
                        + counterSet.getPath() + ", counter=" + path);

                return defaultValue;

            }

        }
        
        // @todo variant that gives the last N minutes in HistoryInstrument.
        protected double getLastHourAverageValue(ICounterSet counterSet, String path,
                double defaultValue) {

            assert counterSet != null;
            assert path != null;

            ICounter c = (ICounter) counterSet.getPath(path);

            if (c == null)
                return defaultValue;

            try {

                HistoryInstrument inst = (HistoryInstrument)c.getInstrument();
                
                double val = (Double) inst.minutes.getAverage();

                return val;

            } catch (Exception ex) {

                log.warn("Could not read double value: counterSet="
                        + counterSet.getPath() + ", counter=" + path);

                return defaultValue;

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
     *       a way to get the hostname from the service item? If you assume that
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
            services.put(serviceUUID, new ServiceScore(hostname, serviceUUID));

            joined.signal();
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Notify the {@link LoadBalancerService} that a service is no longer
     * available.
     * 
     * @param serviceUUID
     */
    protected void leave(UUID serviceUUID) {

        log.info("serviceUUID=" + serviceUUID);

        lock.lock();

        try {

            final ServiceScore info = services.get(serviceUUID);

            if (info == null) {

                log.warn("No such service? serviceUUID=" + serviceUUID);

                return;
                
            }
            
            /*
             * @todo remove history from counters - path is /host/serviceUUID?
             * Consider scheduling removal after a few hours or just sweeping
             * periodically for services with no updates in the last N hours so
             * that people have access to post-mortem data. For the same reason,
             * we should probably snapshot the data prior to the leave
             * (especially if there are WARN or URGENT events for the service)
             * and perhaps periodically snapshot all of the counter data onto
             * rolling log files.
             */
//            root.deletePath(path);
            
            services.remove(serviceUUID);
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Used to read {@link CounterSet} XML.
     */
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
    
    /**
     * Note: {@link #getClientHostname()} is the fully qualified hostname of the
     * client.
     */
    public void notify(String msg, UUID serviceUUID, byte[] data) {

        setupLoggingContext();
        
        try {
        
            log.info(msg+" : serviceUUID="+serviceUUID);

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

    public void warn(String msg, UUID serviceUUID) {
        
        setupLoggingContext();

        try {

            log.warn(msg+" : serviceUUID="+serviceUUID);

        } finally {

            clearLoggingContext();

        }
        
    }

    public void urgent(String msg, UUID serviceUUID) {

        setupLoggingContext();

        try {

            log.error(msg+" : serviceUUID="+serviceUUID);

        } finally {
            
            clearLoggingContext();
            
        }
                
    }

    public boolean isHighlyUtilizedDataService(UUID serviceUUID) throws IOException
    {
    
        setupLoggingContext();

        try {

            final ServiceScore[] scores = this.scores.get();

            // No scores yet?
            if (scores == null) {

                log.info("No scores yet");

                return false;

            }

            final ServiceScore score = services.get(serviceUUID);

            if (score == null) {

                log.info("Service is not scored: " + serviceUUID);

                return false;

            }

            return isHighlyUtilizedDataService(score,scores);
            
        } finally {

            clearLoggingContext();

        }
        
    }

    public boolean isUnderUtilizedDataService(UUID serviceUUID) throws IOException {

        setupLoggingContext();
        
        try {

            final ServiceScore[] scores = this.scores.get();

            // No scores yet?
            if (scores == null) {

                log.info("No scores yet");

                return false;

            }

            final ServiceScore score = services.get(serviceUUID);

            if (score == null) {

                log.info("Service is not scored: " + serviceUUID);

                return false;

            }
          
            return isUnderUtilizedDataService(score,scores);
            
        } finally {
            
            clearLoggingContext();
            
        }
        
    }

    protected boolean isHighlyUtilizedDataService(ServiceScore score,ServiceScore[] scores) {

        assert score != null;
        assert scores != null;
        
        boolean highlyUtilized = false;

        if (score.drank > .8) {

            // top 20% is considered to be highly utilized.

            highlyUtilized = true;

        } else if (score.rank == scores.length - 1) {

            // top rank is considered to be highly utilized.

            highlyUtilized = true;

        }

        log.info("highlyUtilized=" + highlyUtilized + " : " + score);

        return highlyUtilized;

    }
    
    protected boolean isUnderUtilizedDataService(ServiceScore score,ServiceScore[] scores) {
        
        assert score != null;
        assert scores != null;
        
        boolean underUtilized = false;

        if (score.drank < .2) {

            // bottom 20% is considered to be under-utilized.

            underUtilized = true;

        } else if (score.rank == 0) {

            // bottom rank is considered to be under-utilized.

            underUtilized = true;

        }

        log.info("underUtilized=" + underUtilized + " : " + score);

        return underUtilized;
        
    }
    
    public UUID getUnderUtilizedDataService() throws IOException, TimeoutException, InterruptedException {
        
        return getUnderUtilizedDataServices(1, 1, null/* exclude */)[0];
        
    }

    public UUID[] getUnderUtilizedDataServices(int minCount, int maxCount,
            UUID exclude) throws IOException, TimeoutException, InterruptedException {

        setupLoggingContext();
        
        if (minCount < 0)
            throw new IllegalArgumentException();

        if (maxCount < 0)
            throw new IllegalArgumentException();

        try {
            
            lock.lock();

            final ServiceScore[] scores = this.scores.get();

            try {

                if (scores == null) {
                    
                    if (minCount == 0) {

                        log.info("No scores, minCount is zero - will return null.");
                        
                        return null;
                        
                    }

                    /*
                     * Scores are not available immediately. This will await a
                     * non-excluded service join and then return the
                     * "under-utilized" services without reference to computed
                     * service scores. This path is always used when the load
                     * balancer first starts up since it will not have scores
                     * for at least one pass of the UpdateTask.
                     */

                    return getUnderUtilizedDataServicesWithoutScores(minCount,
                            maxCount, exclude);

                }

                /*
                 * Handle the case when we have the scores on hand.
                 * 
                 * Note: if[minCount] is non-zero, then [knownGood] is set to a
                 * service that is (a) not excluded and (b) active. This is the
                 * fallback service that we will recommend if minCount is
                 * non-zero and we are using the scores and all of a sudden it
                 * looks like there are no active services to recommend. This
                 * basically codifies a decision point where we accept that this
                 * service is active. We choose this as the first active and
                 * non- excluded service so that it will be as under-utilized as
                 * possible.
                 */
                
                UUID knownGood = null;
                if (minCount > 0 && exclude != null) {
                    
                    /*
                     * Verify that we have a score for at least one active
                     * service that is not excluded. If not, then we will use
                     * the variant w/o scores that awaits a service join.
                     */
                    int nok = 0;
                    for (int i = 0; i < scores.length && nok < 1; i++) {
                    
                        final UUID serviceUUID = scores[i].serviceUUID;
                        
                        if(exclude.equals(serviceUUID)) continue;
                        
                        if(!services.containsKey(serviceUUID)) continue;
                        
                        if(knownGood==null) knownGood = serviceUUID;
                        
                        nok++;
                        
                    }
                    
                    if (nok < 1) {
                        
                        /*
                         * Since we do not have ANY active and scored
                         * non-excluded services, use the variant that does not
                         * use scores and that awaits a service join.
                         */
                        
                        return getUnderUtilizedDataServicesWithoutScores(minCount,
                                maxCount, exclude);
                        
                    }
                    
                }
                
                /*
                 * Use the scores to compute the under-utilized services. 
                 */
                
                return getUnderUtilizedDataServicesWithScores(minCount,
                        maxCount, exclude, knownGood, scores);

            } finally {

                lock.unlock();

            }

        } finally {

            clearLoggingContext();

        }

    }

    /**
     * Computes the under-utilized services in the case where where <i>minCount</i>
     * is non-zero and we do not have pre-computed {@link #scores} on hand. If
     * there are also no active services, then this awaits the join of at least
     * one service. Once it has at least one service that is not the optionally
     * <i>exclude</i>d service, it returns the "under-utilized" services.
     * 
     * @throws TimeoutException
     *             If the result could not be obtained before the timeout was
     *             triggered.
     * 
     * @throws InterruptedException
     */
    protected UUID[] getUnderUtilizedDataServicesWithoutScores(int minCount,
            int maxCount, UUID exclude) throws TimeoutException,
            InterruptedException {

        log.info("begin");
        
        long begin = System.currentTimeMillis();

        while (true) {

            final long elapsed = System.currentTimeMillis() - begin;

            if (elapsed > timeout)
                throw new TimeoutException();

            // all services that we know about right now.
            final UUID[] knownServiceUUIDs = services.keySet().toArray(
                    new UUID[] {});

            if (knownServiceUUIDs.length == 0) {

                // await a join.
                joined.await(100, TimeUnit.MILLISECONDS);

                continue;

            }

            /*
             * Scan and see if we have anything left after verifying that the
             * service is still on our "live" list and after excluding this the
             * option [exclude] service.
             */

            int nok = 0;

            for (int i = 0; i < knownServiceUUIDs.length; i++) {

                if (exclude != null && exclude.equals(knownServiceUUIDs[i])) {

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
             * Now that we have at least one UUID, populate the return array.
             * 
             * Note: We return at least minCount UUIDs, even if we have to
             * return the same UUID in each slot.
             */

            final UUID[] uuids = new UUID[Math.max(minCount, nok)];

            int n = 0, i = 0;

            while (n < uuids.length) {

                UUID tmp = knownServiceUUIDs[i++ % knownServiceUUIDs.length];

                if (tmp == null)
                    continue;

                uuids[n++] = tmp;

            }

            return uuids;

        }
        
    }
    
    /**
     * Handles the case when we have per-service scores.
     * <p>
     * Note: Pre-condition: the service scores must exist and there must be at
     * least one active service with a score that is not excluded.
     * 
     * @param minCount
     * @param maxCount
     * @param exclude
     * @param knownGood
     *            The service that we will recommend iff minCount is non-zero
     *            and it does not appear that there are any active and
     *            non-excluded services.
     * @param scores
     *            The service scores.
     * @return
     * 
     * @throws TimeoutException
     * @throws InterruptedException
     */
    protected UUID[] getUnderUtilizedDataServicesWithScores(int minCount,
            int maxCount, UUID exclude, UUID knownGood, ServiceScore[] scores)
            throws TimeoutException, InterruptedException {

        log.info("begin");
        
        long begin = System.currentTimeMillis();

        while (true) {

            final long elapsed = System.currentTimeMillis() - begin;

            if (elapsed > timeout)
                throw new TimeoutException();

            /*
             * Decide on the set of active services that we consider to be
             * under-utilized based on their scores. When maxCount is non-zero,
             * this set will be no larger than maxCount. When maxCount is zero
             * the set will contain all services that satisify the
             * "under-utilized" criteria.
             */
            final List<UUID> underUtilized = new ArrayList<UUID>(Math.max(
                    minCount, maxCount));
            
            int nok = 0;
            for(int i=0; i<scores.length; i++) {
                
                final ServiceScore score = scores[i];

                // excluded?
                if (score.serviceUUID.equals(exclude))
                    continue;

                // not active?
                if (!services.containsKey(score.serviceUUID))
                    continue;
                
                if (isUnderUtilizedDataService(score,scores)) {

                    underUtilized.add(score.serviceUUID);

                    nok++;

                }

                if (maxCount > 0 && nok >= maxCount) {

                    log.info("Satisifed maxCount=" + maxCount);

                    break;

                }

                if (minCount > 0 && maxCount == 0 && nok >= minCount) {

                    log.info("Satisifed minCount=" + minCount);

                    break;

                }

            }

            log.info("Found "+underUtilized.size()+" under-utilized and non-excluded services");
            
            if(minCount > 0 && underUtilized.isEmpty()) {

                /*
                 * Since we did not find anything we default to the one service
                 * that the caller provided to us as our fallback. This service
                 * might not be under-utilized and it might no longer be active,
                 * but it is the service that we are going to return.
                 */
                
                assert knownGood != null;

                log.warn("Will report fallback service: "+knownGood);
                
                underUtilized.add(knownGood);
                
            }
            
            /*
             * Populate the return array, choosing at least minCount services
             * and repeating services if necessary.
             * 
             * Note: We return at least minCount UUIDs, even if we have to
             * return the same UUID in each slot.
             */

            final UUID[] uuids = new UUID[Math.max(minCount, nok)];

            int n = 0, i = 0;

            while (n < uuids.length) {

                uuids[n++] = underUtilized.get(i++ % nok);

            }
            
            return uuids;

        }

    }
    
    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * The implementation adds the "serviceUUID" parameter to the {@link MDC}
     * for <i>this</i> service. The serviceUUID is, in general, assigned
     * asynchronously by the service registrar. Once the serviceUUID becomes
     * available it will be added to the {@link MDC}. This datum can be
     * injected into log messages using %X{serviceUUID} in your log4j pattern
     * layout.
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
        
        MDC.remove("hostname");
        
    }

}
