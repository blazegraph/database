package com.bigdata.service;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.HistoryInstrument;
import com.bigdata.counters.ICounter;
import com.bigdata.counters.ICounterSet;
import com.bigdata.counters.IHostCounters;
import com.bigdata.counters.IRequiredHostCounters;
import com.bigdata.counters.ICounterSet.IInstrumentFactory;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.journal.QueueStatisticsTask.IQueueCounters;
import com.bigdata.service.DataService.IDataServiceCounters;
import com.bigdata.service.mapred.IMapService;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.httpd.AbstractHTTPD;

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
 * @todo logging on this {@link LoadBalancerService#log} can provide a single
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
abstract public class LoadBalancerService extends AbstractService
    implements ILoadBalancerService, IServiceShutdown {

    final static protected Logger log = Logger.getLogger(LoadBalancerService.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    static final protected boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    static final protected boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    final protected String ps = ICounterSet.pathSeparator;
    
    /**
     * Service join timeout in milliseconds - used when we need to wait for a
     * service to join before we can recommend an under-utilized service.
     * 
     * @todo config
     */
    final protected long JOIN_TIMEOUT = 3 * 1000;

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
     * The active hosts (one or more services).
     * 
     * @todo get rid of hosts that are no longer active. e.g., we no longer
     *       receive {@link #notify(String, byte[])} events from the host and
     *       the host can not be pinged. this will require tracking the #of
     *       services on the host which we do not do directly right now.
     */
    protected ConcurrentHashMap<String/* hostname */, HostScore> activeHosts = new ConcurrentHashMap<String, HostScore>();

    /**
     * The set of active services.
     */
    protected ConcurrentHashMap<UUID/* serviceUUID */, ServiceScore> activeServices = new ConcurrentHashMap<UUID, ServiceScore>();

    /**
     * Scores for the hosts in ascending order (least utilized to most
     * utilized).
     * <p>
     * This array is initially <code>null</code> and gets updated periodically
     * by the {@link UpdateTask}. The main consumer of this information is the
     * logic in {@link UpdateTask} that computes the service utilization.
     */
    protected AtomicReference<HostScore[]> hostScores = new AtomicReference<HostScore[]>(null);
    
    /**
     * Scores for the services in ascending order (least utilized to most
     * utilized).
     * <p>
     * This array is initially <code>null</code> and gets updated periodically
     * by the {@link UpdateTask}. The methods that report service utilization
     * and under-utilized services are all based on the data in this array.
     * Since services can leave at any time, that logic MUST also test for
     * existence of the service in {@link #activeServices} before assuming that the
     * service is still live.
     */
    protected AtomicReference<ServiceScore[]> serviceScores = new AtomicReference<ServiceScore[]>(null);
    
    /**
     * Aggregated performance counters for all hosts and services. The services
     * are all listed under their host, e.g., <code>hostname/serviceUUID</code>
     */
    protected CounterSet counters = new CounterSet();

    /**
     * The #of {@link UpdateTask}s which have run so far.
     */
    protected int nupdates = 0;
    
    /**
     * The directory in which the service will log the {@link CounterSet}s.
     */
    protected final File logDir;
    
    /**
     * A copy of the properties used to start the service.
     */
    private final Properties properties;
    
    /**
     * An object wrapping the properties provided to the constructor.
     */
    public Properties getProperties() {
        
        return new Properties(properties);
        
    }
    
    /**
     * Per-host metadata and a score for that host which gets updated
     * periodically by {@link UpdateTask}. {@link HostScore}s are a
     * <em>resource utilization</em> measure. They are higher for a host which
     * is more highly utilized. There are several ways to look at the score,
     * including the {@link #rawScore}, the {@link #rank}, and the
     * {@link #drank normalized double-precision rank}. The ranks move in the
     * same direction as the {@link #rawScore}s - a higher rank indicates
     * higher utilization. The least utilized host is always rank zero (0). The
     * most utilized host is always in the last rank.
     * 
     * @todo We carry some additional metadata for hosts that bears on the
     *       question of whether or not the host might be subject to non-linear
     *       performance degredation. The most important bits of information are
     *       whether or not the host is swapping heavily, since that will bring
     *       performance to a standstill, and whether or not the host is about
     *       to run out of disk space, since that can cause all services on the
     *       host to suddenly fail.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class HostScore implements Comparable<HostScore> {

        public final String hostname;

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
         * for the host and could not compute its rawScore.
         * 
         * @param hostname
         */
        public HostScore(String hostname) {
            
            this(hostname,0d);
            
        }
        
        /**
         * Constructor variant used when you have computed the rawStore.
         * 
         * @param hostname
         * @param rawScore
         */
        public HostScore(String hostname, double rawScore) {
            
            assert hostname != null;
            
            assert hostname.length() > 0;
            
            this.hostname = hostname;
            
            this.rawScore = rawScore;
            
        }
        
        public String toString() {
            
            return "HostScore{hostname=" + hostname + ", rawScore=" + rawScore
                    + ", score=" + score + ", rank=" + rank + ", drank="
                    + drank + "}";
            
        }
        
        /**
         * Places elements into order by ascending {@link #rawScore} (aka
         * increasing utilization). The {@link #hostname} is used to break any
         * ties.
         */
        public int compareTo(HostScore arg0) {
            
            if(rawScore < arg0.rawScore) {
                
                return -1;
                
            } else if(rawScore> arg0.rawScore ) {
                
                return 1;
                
            }
            
            return hostname.compareTo(arg0.hostname);
            
        }

        /**
         * Normalizes a raw score in the context of totals for some host.
         * 
         * @param rawScore
         *            The raw score.
         * @param totalRawScore
         *            The raw score computed from the totals.
         *            
         * @return The normalized score.
         */
        static public double normalize(double rawScore, double totalRawScore ) {
            
            if(totalRawScore == 0d) {
                
                return 0d;
                
            }
            
            return rawScore / totalRawScore;
            
        }

    }
    
    /**
     * Per-service metadata and a score for that service which gets updated
     * periodically by the {@link UpdateTask}. {@link ServiceScore}s are a
     * <em>resource utilization</em> measure. They are higher for a service
     * which is more highly utilized. There are several ways to look at the
     * score, including the {@link #rawScore}, the {@link #rank}, and the
     * {@link #drank normalized double-precision rank}. The ranks move in the
     * same direction as the {@link #rawScore}s - a higher rank indicates
     * higher utilization. The least utilized service is always rank zero (0).
     * The most utilized service is always in the last rank.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    static protected class ServiceScore implements Comparable<ServiceScore> {
        
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

        /**
         * Normalizes a raw score in the context of totals for some service.
         * 
         * @param rawScore
         *            The raw score.
         * @param totalRawScore
         *            The raw score computed from the totals.
         *            
         * @return The normalized score.
         */
        static public double normalize(double rawScore, double totalRawScore ) {
            
            if(totalRawScore == 0d) {
                
                return 0d;
                
            }
            
            return rawScore / totalRawScore;
            
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
     * The optional httpd service.
     */
    private AbstractHTTPD httpd;

    /**
     * The delay between writes of the {@link CounterSet} on a log file.
     */
    private final long logDelayMillis; 
    /**
     * The #of distinct log files to retain.
     */
    private final long logMaxFiles;
    /**
     * Time that the {@link CounterSet} was last written onto a log file.
     */
    private long logLastMillis = System.currentTimeMillis();
    /**
     * A one-up counter of the #of times the {@link CounterSet} was written onto
     * a log file.
     */
    private int logFileCount = 0;
    
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
        String UPDATE_DELAY = "updateDelay";
        
        /**
         * The default {@link #UPDATE_DELAY}.
         */
        String DEFAULT_UPDATE_DELAY = ""+(60*1000);
    
        /**
         * The port on which the load balancer will run its <code>httpd</code>
         * service (default {@value #DEFAULT_HTTPD_PORT}) -or- ZERO (0) to NOT
         * start the <code>httpd</code> service. This service may be used to
         * view the telemetry reported by the various services in the federation
         * that are, or have been, joined with the load balancer.
         */
        String HTTPD_PORT = "httpd.port";
        
        String DEFAULT_HTTPD_PORT = "8080";
     
        /**
         * The path of the directory where the load balancer will log a copy of
         * the counters every time it runs its {@link UpdateTask}. By default
         * it will log the files in the directory from which the load balancer
         * service was started. You may specify an alternative directory using
         * this property.
         */
        String LOG_DIR = "log.dir";
        
        String DEFAULT_LOG_DIR = ".";

        /**
         * The delay in milliseconds between writes of the {@link CounterSet} on
         * a log file (default is {@value #DEFAULT_LOG_DELAY}, which is
         * equivilent to one hour).
         */
        String LOG_DELAY = "log.delay";
        
        String DEFAULT_LOG_DELAY = "" + 1000 * 60 * 60;

        /**
         * The maximum #of distinct log files to retain (default is one week
         * based on a {@link #LOG_DELAY} equivilant to one hour).
         */
        String LOG_MAX_FILES = "log.maxFiles";

        String DEFAULT_LOG_MAX_FILES = "" + 24 * 7;

    }

    /**
     * 
     * @param properties
     *            See {@link Options}
     */
    public LoadBalancerService(Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();
        
        this.properties = (Properties) properties.clone();

        // setup the log directory.
        {

            final String val = properties.getProperty(
                    Options.LOG_DIR,
                    Options.DEFAULT_LOG_DIR);

            logDir = new File(val);

            log.info(Options.LOG_DIR + "=" + logDir);                

            // ensure exists.
            logDir.mkdirs();
            
        }

        // logDelayMillis
        {
            
            logDelayMillis = Long.parseLong(properties.getProperty(
                    Options.LOG_DELAY, Options.DEFAULT_LOG_DELAY));

            log.info(Options.LOG_DELAY + "=" + logDelayMillis);
            
        }

        // logMaxFiles
        {

            logMaxFiles = Integer.parseInt(properties.getProperty(
                    Options.LOG_MAX_FILES, Options.DEFAULT_LOG_MAX_FILES));

            log.info(Options.LOG_MAX_FILES + "=" + logMaxFiles);

        }
        
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

        /*
         * HTTPD service reporting out statistics. This will be shutdown with
         * the load balancer.
         */
        {
            
            final int port = Integer.parseInt(properties.getProperty(
                    Options.HTTPD_PORT,
                    Options.DEFAULT_HTTPD_PORT));

            log.info(Options.HTTPD_PORT+"="+port);
            
            if (port < 0)
                throw new RuntimeException(Options.HTTPD_PORT
                        + " may not be negative");
            
            AbstractHTTPD httpd = null;
            if (port != 0) {
                try {
                    httpd = new CounterSetHTTPD(port,counters);
                } catch (IOException e) {
                    log.error("Could not start httpd on port=" + port, e);
                }
            }
            this.httpd = httpd;
            
        }
        
    }
    
    public boolean isOpen() {
        
        return ! updateService.isShutdown();
        
    }
    
    protected void finalized() throws Throwable {
        
        super.finalize();
        
        shutdownNow();
        
    }
    
    synchronized public void shutdown() {

        if(!isOpen()) return;
        
        log.info("begin");
        
        updateService.shutdown();
        
        if (httpd != null) {
            
            httpd.shutdown();
         
            httpd = null;
            
        }

        // log the final state of the counters.
        logCounters("final");
        
        log.info("done");

    }

    synchronized public void shutdownNow() {

        if(!isOpen()) return;

        log.info("begin");
        
        updateService.shutdownNow();
        
        if (httpd != null) {

            httpd.shutdownNow();
         
            httpd = null;
            
        }

        // log the final state of the counters.
        logCounters("final");

        log.info("done");

    }

    /**
     * Extended to setup the generic per-service {@link CounterSet}, but NOT
     * the per-host counters. This means that the {@link LoadBalancerService}
     * will report the performance counters for its own service to itself, but
     * it will not have any history for those counters. Since the counters as
     * collected from the O/S do not have history, the
     * {@link LoadBalancerService} does NOT collect the per-host counters
     * directly from the O/S but relies on the existence of at least one other
     * service running on the same host as it to collect and report the counters
     * to the {@link LoadBalancerService}.
     */
    public void setServiceUUID(UUID serviceUUID) {
        
        super.setServiceUUID(serviceUUID);
        
        final String ps = ICounterSet.pathSeparator;
        
        final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;
                   
        final String pathPrefix = ps + hostname + ps + "service" + ps
                + getServiceIface().getName() + ps + serviceUUID + ps;

        final CounterSet serviceRoot = counters.makePath(pathPrefix);

        /*
         * Service generic counters. 
         */
        
        AbstractStatisticsCollector.addBasicServiceOrClientCounters(
                serviceRoot, this, properties);

    }
    
    /**
     * Returns {@link ILoadBalancerService}.
     */
    public Class getServiceIface() {
        
        return ILoadBalancerService.class;
        
    }
    
    /**
     * Normalizes the {@link ServiceScore}s and set them in place.
     * 
     * @param a
     *            The new service scores.
     */
    protected void setHostScores(HostScore[] a) {

        if(INFO) log.info("#hostScores=" + a.length);
        
        /*
         * sort scores into ascending order (least utilized to most utilized).
         */
        Arrays.sort(a);

        /*
         * Compute the totalRawScore.
         */
        double totalRawScore = 0d;

        for (HostScore s : a) {
        
            totalRawScore += s.rawScore;
            
        }
        
        /*
         * compute normalized score, rank, and drank.
         */
        for (int i = 0; i < a.length; i++) {
            
            final HostScore score = a[i];
            
            score.rank = i;
            
            score.drank = ((double)i)/a.length;

            score.score = HostScore.normalize(score.rawScore, totalRawScore);
            
            // update score in global map.
            activeHosts.put(score.hostname, score);

            if (INFO)
                log.info(score.toString()); //@todo debug?
            
        }
        
        if(INFO) {
            
            log.info("The most active host was: " + a[a.length - 1]);

            log.info("The least active host was: " + a[0]);
        
        }
        
        // Atomic replace of the old scores.
        LoadBalancerService.this.hostScores.set( a );

        if(INFO) log.info("Updated scores for "+a.length+" hosts");
     
    }
    
    /**
     * Normalizes the {@link ServiceScore}s and set them in place.
     * 
     * @param a
     *            The new service scores.
     */
    protected void setServiceScores(ServiceScore[] a) {
        
        if(INFO) log.info("#serviceScores=" + a.length);
        
        /*
         * sort scores into ascending order (least utilized to most utilized).
         */
        Arrays.sort(a);

        /*
         * Compute the totalRawScore.
         */
        double totalRawScore = 0d;

        for (ServiceScore s : a) {
        
            totalRawScore += s.rawScore;
            
        }

        /*
         * compute normalized score, rank, and drank.
         */
        for (int i = 0; i < a.length; i++) {
            
            final ServiceScore score = a[i];
            
            score.rank = i;
            
            score.drank = ((double)i)/a.length;
           
            score.score = HostScore.normalize(score.rawScore, totalRawScore);

            // update score in global map.
            activeServices.put(score.serviceUUID, score);

            if (INFO)
                log.info(score.toString()); //@todo debug?

        }
        
        if(INFO) {
            
            log.info("The most active service was: " + a[a.length - 1]);

            log.info("The least active service was: " + a[0]);
            
        }
        
        // Atomic replace of the old scores.
        LoadBalancerService.this.serviceScores.set( a );

        if (INFO)
            log.info("Updated scores for " + a.length + " services");

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

        /** @todo configure how much history to use for averages. */
        final int TEN_MINUTES = 10;
        
        public UpdateTask() {
        }
        
        /**
         * Note: Don't throw anything here since we don't want to have the task
         * suppressed!
         */
        public void run() {

            try {

                updateHostScores();
                
                updateServiceScores();
                
                setupCounters();
                
                logCounters();
                
            } catch (Throwable t) {

                log.error("Problem in update task?", t);

            } finally {
                
                nupdates++;
                
            }

        }

        /**
         * (Re-)compute the utilization score for each active host.
         */
        protected void updateHostScores() {

            if(activeHosts.isEmpty()) {
                
                log.warn("No active hosts");
                
                return;
                
            }

            /*
             * Update scores for the active hosts.
             */

            final Vector<HostScore> scores = new Vector<HostScore>();
            
            // For each host
            final Iterator<ICounterSet> itrh = counters.counterSetIterator();
            
            while(itrh.hasNext()) {
                
                final CounterSet hostCounterSet = (CounterSet) itrh.next();
                
                // Note: name on hostCounterSet is the fully qualified hostname.
                final String hostname = hostCounterSet.getName();
                
                if(!activeHosts.containsKey(hostname)) {

                    // Host is not active.
                    if(INFO) log.info("Host is not active: "+hostname);
                    
                    continue;
                    
                }
                
                /*
                 * Compute the score for that host.
                 */
                HostScore score;
                try {

                    score = computeScore(hostname, hostCounterSet);

                } catch (Exception ex) {

                    log.error("Problem computing host score: " + hostname, ex);

                    /*
                     * Keep the old score if we were not able to compute a new
                     * score.
                     * 
                     * Note: if the returned value is null then the host was
                     * asynchronously removed from the set of active hosts.
                     */
                    score = activeHosts.get(hostname);

                    if (score == null) {

                        log.warn("Host gone during update task: " + hostname);

                        continue;

                    }

                }

                /*
                 * Add to collection of scores.
                 */
                scores.add(score);

            }

            if (scores.isEmpty()) {

                log.warn("No performance counters for hosts, but "
                        + activeHosts.size() + " active hosts");
                
                LoadBalancerService.this.hostScores.set( null );
                
                return;
                
            }
            
            // scores as an array.
            final HostScore[] a = scores.toArray(new HostScore[] {});

            setHostScores(a);
            
        }

        /**
         * (Re-)compute the utilization score for each active service.
         */
        protected void updateServiceScores() {
            
            if(activeServices.isEmpty()) {
                
                log.warn("No active services");
                
                LoadBalancerService.this.serviceScores.set( null );
                
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

                // Pre-computed score for the host on which the service is running.
                final HostScore hostScore = activeHosts.get(hostname);

                if (hostScore == null) {

                    // Host is not active.
                    if(INFO) log.info("Host is not active: " + hostname);
                    
                    continue;
                    
                }

                // lookup path: /hostname/service
                final CounterSet serviceIfacesCounterSet = (CounterSet) hostCounterSet
                        .getPath("service");

                if (serviceIfacesCounterSet == null) {

                    log.warn("No services interfaces? hostname=" + hostname);

                    continue;

                }

                // for each service interface type: /hostname/service/iface
                final Iterator<ICounterSet> itrx = serviceIfacesCounterSet
                        .counterSetIterator();

                // for each service under that interface type
                while (itrx.hasNext()) {

                    // path: /hostname/service/iface/UUID
                    final CounterSet servicesCounterSet = (CounterSet) itrx
                            .next();

                    // For each service.
                    final Iterator<ICounterSet> itrs = servicesCounterSet
                            .counterSetIterator();

                    while (itrs.hasNext()) {

                        final CounterSet serviceCounterSet = (CounterSet) itrs
                                .next();

                        // Note: name on serviceCounterSet is the serviceUUID.
                        final String serviceName = serviceCounterSet.getName();
                        final UUID serviceUUID;
                        try {
                            serviceUUID = UUID.fromString(serviceName);
                        } catch (Exception ex) {
                            log.error("Could not parse service name as UUID?\n"
                                    + "hostname=" + hostname
                                    + ", serviceCounterSet.path="
                                    + serviceCounterSet.getPath()
                                    + ", serviceCounterSet.name="
                                    + serviceCounterSet.getName(), ex);
                            continue;
                        }

                        if (!activeServices.containsKey(serviceUUID)) {

                            /*
                             * @todo I am seeing some services reported here as
                             * not active???
                             */

                            log.warn("Service is not active: "
                                    + serviceCounterSet.getPath());

                            continue;

                        }

                        /*
                         * Compute the score for that service.
                         */
                        ServiceScore score;
                        try {

                            score = computeScore(hostScore, serviceUUID,
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
                            score = activeServices.get(serviceUUID);

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

            }

            if (scores.isEmpty()) {

                log.warn("No performance counters for services, but "
                        + activeServices.size() + " active services");
                
                LoadBalancerService.this.serviceScores.set( null );
                
                return;
                
            }

            // scores as an array.
            final ServiceScore[] a = scores.toArray(new ServiceScore[] {});

            // normalize and set in place.
            setServiceScores(a);

        }
        
        /**
         * Compute the score for a host.
         * <p>
         * The host scores MUST reflect critical resource exhaustion, especially
         * DISK free space, which can take down all services on the host, and
         * SWAPPING, which can bring the effective throughput of the host to a
         * halt. All other resources fail soft, by causing the response time to
         * increase.
         * <p>
         * Note: DISK exhaustion can lead to immediate failure of all services
         * on the same host. A host that is nearing DISK exhaustion SHOULD get
         * heavily dinged and an admin SHOULD be alerted.
         * <p>
         * The correct response for heavy swapping is to alert an admin to
         * shutdown one or more processes on that host. <strong>If you do not
         * have failover provisioned for your data services then don't shutdown
         * data services or you WILL loose data!</strong>
         * <p>
         * Note: If we are not getting critical counters for some host then we
         * are assuming a reasonable values for the missing data and computing
         * the utilization based on those assumptions. Note that a value of zero
         * (0) may be interepreted as either critically high utilization or no
         * utilization depending on the performance counter involved and that
         * the impact of the different counters can vary depending on the
         * formula used to compute the utilization score.
         * 
         * @param hostname
         *            The fully qualified hostname.
         * @param hostCounterSet
         *            The performance counters for that host.
         * @param serviceCounterSet
         *            The performance counters for that service.
         * 
         * @return The computed host score.
         */
        protected HostScore computeScore(String hostname,
                ICounterSet hostCounterSet) {

            /*
             * Is the host swapping heavily?
             * 
             * @todo if heavy swapping persists then lower the score even
             * further.
             */
            final double majorFaultsPerSec = getCurrentValue(hostCounterSet,
                    IRequiredHostCounters.Memory_majorFaultsPerSecond, 0d/* default */);

            /*
             * Is the host out of disk?
             * 
             * FIXME Really needs to check on each partition on which we have a
             * data service (dataDir). This is available to us now, as is the
             * free disk space for the tmpDir for each data service.
             * 
             * @todo this will issue a warning for a windows host on which a
             * service is just starting up. For some reason, it takes a few
             * cycles to begin reporting performance counters on a windows host
             * and the initial counters will therefore all be reported as zeros.
             * This problem should be fixed, but we also need to discount an
             * average whose result is zero if the #of samples is also zero. In
             * that case we just don't have any information. Likewise, when the
             * #of samples to date (cumulative or just the #of minutes in 0:60
             * of data in the minutes history) is less than 5 minutes worth of
             * data then we may still need to discount the data. Also consider
             * adding a "moving average" computation to the History so that we
             * can smooth short term spikes.
             */
            final double percentDiskFreeSpace = getCurrentValue(hostCounterSet,
                    IRequiredHostCounters.LogicalDisk_PercentFreeSpace, .5d/* default */);

            /*
             * The percent of the time that the CPUs are idle.
             */
            final double percentProcessorIdle = 1d - getAverageValueForMinutes(
                    hostCounterSet, IHostCounters.CPU_PercentProcessorTime,
                    .5d, TEN_MINUTES);

            /*
             * The percent of the time that the CPUs are idle when there is an
             * outstanding IO request.
             */
            final double percentIOWait = getAverageValueForMinutes(
                    hostCounterSet, IHostCounters.CPU_PercentIOWait,
                    .01d/* default */, TEN_MINUTES);

            /*
             * @todo This reflects the disk IO utilization primarily through
             * induced IOWAIT. Play around with other forumulas too.
             */
            double rawScore = (1d + percentIOWait * 100d)
                    / (1d + percentProcessorIdle);

            if (INFO) {
                log.info("hostname="+hostname+", majorFaultsPerSec="+majorFaultsPerSec);
                log.info("hostname="+hostname+", percentDiskSpaceFree="+percentDiskFreeSpace);
                log.info("hostname="+hostname+", percentProcessorIdle="+percentProcessorIdle);
                log.info("hostname="+hostname+", percentIOWait="+percentIOWait);
                log.info("hostname=" + hostname + " : rawScore(" + rawScore
                        + ") = (1d + percentIOWait(" + percentIOWait
                        + ") * 100d) / (1d + percentProcessorIdle("
                        + percentProcessorIdle + ")");
            }

            if (majorFaultsPerSec > 50) {

                // much higher utilization if the host is swapping heavily.
                rawScore *= 10;

                log.warn("hostname=" + hostname
                                + " : swapping heavily: pages/sec="
                                + majorFaultsPerSec);

            } else if (majorFaultsPerSec > 10) {

                // higher utilization if the host is swapping.
                rawScore *= 2d;

                log.warn("hostname=" + hostname + " : swapping: pages/sec="
                        + majorFaultsPerSec);

            }

            if (percentDiskFreeSpace < .05) {

                // much higher utilization if the host is very short on disk.
                rawScore *= 10d;

                log.warn("hostname=" + hostname
                        + " : very short on disk: freeSpace="
                        + percentDiskFreeSpace * 100 + "%");

            } else if (percentDiskFreeSpace < .10) {

                // higher utilization if the host is short on disk.
                rawScore *= 2d;

                log.warn("hostname=" + hostname
                        + " : is short on disk: freeSpace="
                        + percentDiskFreeSpace * 100 + "%");

            }
            
            if(INFO) {
                
                log.info("hostname="+hostname+" : final rawScore="+rawScore);
                
            }
            
            final HostScore hostScore = new HostScore(hostname, rawScore);

            return hostScore;

        }
        
        /**
         * Compute the score for a service.
         * <p>
         * Note: utilization is defined in terms of transient system resources :
         * CPU, IO (DISK and NET), RAM. A host with enough CPU/RAM/IO/DISK can
         * support more than one data service. Therefore it is important to look
         * at not just host utilization but also at process utilization.
         * 
         * @param hostScore
         *            The pre-computed score for the host on which the service
         *            is running.
         * @param serviceUUID
         *            The service {@link UUID}.
         * @param hostCounterSet
         *            The performance counters for that host (in case you need
         *            anything that is not already in the {@link HostScore}).
         * @param serviceCounterSet
         *            The performance counters for that service.
         * 
         * @return The computed score for that service.
         */
        protected ServiceScore computeScore(HostScore hostScore, UUID serviceUUID,
                ICounterSet hostCounterSet, ICounterSet serviceCounterSet) {

            assert hostScore != null;
            assert serviceUUID != null;
            assert hostCounterSet != null;
            assert serviceCounterSet != null;
            
            // verify that the host score has been normalized.
            assert hostScore.rank != -1 : hostScore.toString();
            
            /*
             * A simple approximation based on the average task queuing time
             * (how long it takes a task to execute on the service) for the
             * unisolated write service.
             * 
             * @todo There is a lot more that can be considered and under linux
             * we have access to per-process counters for CPU, DISK, and MEMORY.
             */

            final double averageTaskQueuingTime = getAverageValueForMinutes(
                    hostCounterSet, IDataServiceCounters.concurrencyManager
                            + ps + IConcurrencyManagerCounters.writeService
                            + ps + IQueueCounters.averageTaskQueuingTime,
                    100d/* ms */, TEN_MINUTES);

            final double rawScore = (averageTaskQueuingTime + 1) * (hostScore.score + 1);

            if (INFO) {
             
                log.info("rawScore(" + rawScore + ") = ("
                        + IQueueCounters.averageTaskQueuingTime + "("
                        + averageTaskQueuingTime + ")+1) * (hostScore("
                        + hostScore.score + ")+1)");
                
            }

            final ServiceScore score = new ServiceScore(hostScore.hostname,
                    serviceUUID, rawScore);
            
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

        /**
         * Return the average of the counter having the given path over the last
         * <i>minutes</i> minutes.
         * 
         * @param counterSet
         * @param path
         * @param defaultValue
         * @param minutes
         * @return
         */
        protected double getAverageValueForMinutes(ICounterSet counterSet, String path,
                double defaultValue, int minutes) {

            assert counterSet != null;
            assert path != null;

            ICounter c = (ICounter) counterSet.getPath(path);

            if (c == null)
                return defaultValue;

            try {

                HistoryInstrument inst = (HistoryInstrument)c.getInstrument();
                
                double val = (Double) inst.minutes.getAverage(minutes);

                return val;

            } catch (Exception ex) {

                log.warn("Could not read double value: counterSet="
                        + counterSet.getPath() + ", counter=" + path);

                return defaultValue;

            }

        }

        /**
         * Sets up reporting for the computed per-host and per-service scores.
         * These counters are reported under the service {@link UUID} for the
         * {@link LoadBalancerService} itself. This makes it easy to consult the
         * scores for the various hosts and services.
         * <p>
         * Note: The host and service scores will not appear until the
         * {@link UpdateTask} has executed and those scores have been computed.
         * By default, the first execution of the {@link UpdateTask} is delayed
         * until we expect to have the host and service counters from two
         * reporting periods. This means that you can expect to wait at least
         * two minutes before these data will appear.
         * 
         * @todo counters for service scores should be eventually removed after
         *       the service leaves. Likewise for host scores. However, these
         *       counters SHOULD remain available for a while for post-mortem of
         *       the service/host, e.g., at least 2-3 days.
         */
        protected void setupCounters() {
            
            final String ps = ICounterSet.pathSeparator;
            
            final String hostname = AbstractStatisticsCollector.fullyQualifiedHostName;

            final UUID serviceUUID = getServiceUUID();

            if (serviceUUID == null) {

                log.warn("No serviceUUID for this service?");
                
                return;
                
            }
            
            final String pathPrefix = ps + hostname + ps + "service" + ps
                    + getServiceIface().getName() + ps + serviceUUID + ps;

            final CounterSet serviceRoot = counters.makePath(pathPrefix);

            final long now = System.currentTimeMillis();
            
            // per-host scores.
            {

                if (INFO)
                    log.info("Will setup counters for " + activeHosts.size()
                            + " hosts");
                
                final CounterSet tmp = serviceRoot.makePath("hosts");

                synchronized (tmp) {
                    
                    for (HostScore hs : activeHosts.values()) {

                        final String hn = hs.hostname;

                        if (tmp.getChild(hn) == null) {

                            if (INFO)
                                log.info("Adding counter for host: " + hn);
                            
                            tmp.addCounter(hn, new HistoryInstrument<String>(new String[]{}));
                            
                        }
                        
                        {

                            final ICounter counter = (ICounter) tmp.getChild(hn);

                            final HistoryInstrument<String> inst = (HistoryInstrument<String>) counter
                                    .getInstrument();

                            final HostScore score = activeHosts.get(hn);
                            
                            if (score != null) {

                                inst.add(now, score.toString());
                                
                            }

                        }

                    }

                }
                
            }
            
            // per-service scores.
            {
                
                if (INFO)
                    log.info("Will setup counters for " + activeServices.size()
                            + " services");

                final CounterSet tmp = serviceRoot.makePath("services");

                synchronized (tmp) {

                    for (ServiceScore ss : activeServices.values()) {

                        final String uuidStr = ss.serviceUUID.toString();

                        if (tmp.getChild(uuidStr) == null) {

                            log.info("Adding counter for service: "+uuidStr);

                            tmp.addCounter(uuidStr, new HistoryInstrument<String>(new String[]{}));
                            
                        }
                        
                        {

                            final ICounter counter = (ICounter) tmp.getChild(uuidStr);

                            final HistoryInstrument<String> inst = (HistoryInstrument<String>) counter
                                    .getInstrument();

                            final ServiceScore score = activeServices
                                    .get(ss.serviceUUID);

                            if (score != null) {

                                inst.add(now, score.toString());

                            }

                        }

                    }

                }
                
            }
            
        }
        
        /**
         * Writes the counters on a file.
         * 
         * @see Options
         */
        protected void logCounters() {

            final long now = System.currentTimeMillis();

            final long elapsed = now - logLastMillis;

            if (elapsed > logDelayMillis) {
                
                final String basename = "" + (logFileCount % logMaxFiles);

                log.info("Writing counters on: "+basename);

                LoadBalancerService.this.logCounters(basename);

                logFileCount++;

                logLastMillis = now;

            }
            
        }
        
    }

    /**
     * Writes the counters on a file.
     * 
     * @param basename
     *            The basename of the file. The file will be written in the
     *            {@link #logDir}.
     * 
     * @throws IOException
     */
    protected void logCounters(String basename) {
        
        final File file = new File(logDir, "counters" + basename + ".xml");

        if (INFO)
            log.info("Writing counters on " + file);
        
        OutputStream os = null;
        
        try {

            os = new BufferedOutputStream( new FileOutputStream(file) );
            
            counters.asXML(os, "UTF-8", null/*filter*/);
        
        } catch(Exception ex) {
            
            log.error(ex.getMessage(), ex);

        } finally {

            if (os != null) {

                try {
                
                    os.close();
                    
                } catch (Exception ex) {
                    
                    // Ignore.
                    
                }
                
            }

        }
        
    }
    
    /**
     * Notify the {@link LoadBalancerService} that a new service is available.
     * <p>
     * Note: You CAN NOT use this method to handle a Jini service join (where a
     * client notices the join of some service with a jini registrar) since you
     * will not have access to the hostname. The hostname is only available when
     * the service itself uses {@link #notify(String, byte[])}.
     * 
     * @param serviceUUID
     */
    protected void join(UUID serviceUUID, String hostname) {
        
        if (INFO)
            log.info("serviceUUID=" + serviceUUID + ", hostname=" + hostname);
        
        lock.lock();
        
        try {

            if(activeHosts.putIfAbsent(hostname, new HostScore(hostname))==null) {

                if (INFO)
                    log.info("New host joined: hostname="+hostname);
                
            }
            
            /*
             * Add to set of known services.
             */
            if(activeServices.putIfAbsent(serviceUUID, new ServiceScore(hostname, serviceUUID))!=null) {
             
                if (INFO)
                    log.info("Already joined: serviceUUID=" + serviceUUID
                            + ", hostname=" + hostname);

                return;
                
            }

            if (INFO)
                log.info("New service joined: hostname=" + hostname
                        + ", serviceUUID=" + serviceUUID);

            /*
             * Create history in counters.
             * 
             * path: /host/service/iface/serviceUUID.
             */
            counters.makePath(//
                    hostname + //
                    ICounterSet.pathSeparator + //
                    "service" + //
                    ICounterSet.pathSeparator+ //
                    IDataService.class.getName() + //
                    ICounterSet.pathSeparator
                    + serviceUUID);

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
    public void leave(String msg, UUID serviceUUID) {

        if (INFO)
            log.info("msg=" + msg + ", serviceUUID=" + serviceUUID);

        lock.lock();

        try {

            final ServiceScore info = activeServices.get(serviceUUID);

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
            
            activeServices.remove(serviceUUID);
            
        } finally {
            
            lock.unlock();
            
        }
        
    }

    /**
     * Used to read {@link CounterSet} XML.
     */
    final private IInstrumentFactory instrumentFactory = DefaultInstrumentFactory.INSTANCE;
    
    /**
     * Note: Only {@link IDataService}s are entered into the internal tables
     * for the load balancer. Other kinds of services are noted and the reported
     * counters are aggregated.
     */
    public void notify(final String msg, final UUID serviceUUID,
            final String serviceIFace, final byte[] data) {

        setupLoggingContext();
        
        try {
        
            if (INFO)
                log.info(msg + " : iface=" + serviceIFace + ", uuid="
                        + serviceUUID);

            if (IDataService.class.getName().equals(serviceIFace)) {

                /*
                 * Note: only IDataService gets into the internal tables.
                 */
                
                join(serviceUUID, getClientHostname());

            }
            
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

            final ServiceScore[] scores = this.serviceScores.get();

            // No scores yet?
            if (scores == null) {

                log.info("No scores yet");

                return false;

            }

            final ServiceScore score = activeServices.get(serviceUUID);

            if (score == null) {

                if (INFO)
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

            final ServiceScore[] scores = this.serviceScores.get();

            // No scores yet?
            if (scores == null) {

                log.info("No scores yet");

                return false;

            }

            final ServiceScore score = activeServices.get(serviceUUID);

            if (score == null) {

                if (INFO)
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

        if (INFO)
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

        if (INFO)
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

            final ServiceScore[] scores = this.serviceScores.get();

            try {

                if (scores == null) {
                    
                    if (minCount == 0) {

                        if(INFO) log.info("No scores, minCount is zero - will return null.");
                        
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
                        
                        if(!activeServices.containsKey(serviceUUID)) continue;
                        
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
     * is non-zero and we do not have pre-computed {@link #serviceScores} on hand. If
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

            if (elapsed > JOIN_TIMEOUT) {

                log.warn("Timeout waiting for service to join.");
                
                throw new TimeoutException();
                
            }

            // all services that we know about right now.
            final UUID[] knownServiceUUIDs = activeServices.keySet().toArray(
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

                if (!activeServices.containsKey(knownServiceUUIDs[i])) {

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

            if (elapsed > JOIN_TIMEOUT)
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
                if (!activeServices.containsKey(score.serviceUUID))
                    continue;
                
                if (isUnderUtilizedDataService(score,scores)) {

                    underUtilized.add(score.serviceUUID);

                    nok++;

                }

                if (maxCount > 0 && nok >= maxCount) {

                    if(INFO) log.info("Satisifed maxCount=" + maxCount);

                    break;

                }

                if (minCount > 0 && maxCount == 0 && nok >= minCount) {

                    if(INFO) log.info("Satisifed minCount=" + minCount);

                    break;

                }

            }

            if (INFO)
                log.info("Found " + underUtilized.size()
                        + " under-utilized and non-excluded services");
            
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
        
    }

}
