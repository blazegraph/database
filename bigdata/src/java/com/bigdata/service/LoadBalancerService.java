package com.bigdata.service;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
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
import com.bigdata.journal.ConcurrencyManager.IConcurrencyManagerCounters;
import com.bigdata.rawstore.Bytes;
import com.bigdata.resources.ResourceManager.IResourceManagerCounters;
import com.bigdata.resources.StoreManager.IStoreManagerCounters;
import com.bigdata.service.DataService.IDataServiceCounters;
import com.bigdata.service.mapred.IMapService;
import com.bigdata.util.concurrent.DaemonThreadFactory;
import com.bigdata.util.concurrent.QueueStatisticsTask;
import com.bigdata.util.concurrent.QueueStatisticsTask.IQueueCounters;

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
 * @todo Consider an SNMP adaptor for the clients so that they can report to
 *       SNMP aware applications. In this context we could report both the
 *       original counters (as averaged) and the massaged metrics on which we
 *       plan to make decisions.
 * 
 * @todo Would it make sense to replace the counters XML mechanism with MXBeans
 *       specific to bigdata and additional MXBeans for performance counters for
 *       the operating system?
 */
abstract public class LoadBalancerService extends AbstractService
    implements ILoadBalancerService, IServiceShutdown {

    final static protected Logger log = Logger.getLogger(LoadBalancerService.class);

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    static final protected boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    static final protected boolean DEBUG = log.isDebugEnabled();

    final protected String ps = ICounterSet.pathSeparator;
    
    /**
     * Used to read {@link CounterSet} XML.
     */
    final private IInstrumentFactory instrumentFactory = DefaultInstrumentFactory.INSTANCE;
    
    /**
     * Service join timeout in milliseconds - used when we need to wait for a
     * service to join before we can recommend an under-utilized service.
     * 
     * @see Options#SERVICE_JOIN_TIMEOUT
     */
    final protected long serviceJoinTimeout;

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
    protected ConcurrentHashMap<UUID/* serviceUUID */, ServiceScore> activeDataServices = new ConcurrentHashMap<UUID, ServiceScore>();

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
     * existence of the service in {@link #activeDataServices} before assuming that the
     * service is still live.
     */
    protected AtomicReference<ServiceScore[]> serviceScores = new AtomicReference<ServiceScore[]>(null);
    
    /**
     * The #of {@link UpdateTask}s which have run so far.
     * 
     * @see Options#INITIAL_ROUND_ROBIN_UPDATE_COUNT
     * 
     * @see #getUnderUtilizedDataServices(int, int, UUID)
     */
    protected long nupdates = 0;
    
    /**
     * The #of updates during which
     * {@link #getUnderUtilizedDataServices(int, int, UUID)} will apply a round
     * robin policy.
     * 
     * @see Options#INITIAL_ROUND_ROBIN_UPDATE_COUNT
     */
    protected final long initialRoundRobinUpdateCount;

    /**
     * Used to make round-robin assignments.
     */
    private final RoundRobinServiceLoadHelper roundRobinServiceLoadHelper;
    
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
     * Return the canonical hostname of the client in the context of a RMI
     * request. If the request is not remote then return the canonical hostname
     * for this host.
     */
    abstract protected String getClientHostname();
    
    /**
     * Runs a periodic {@link UpdateTask}.
     */
    final protected ScheduledExecutorService updateService;

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
     * The #of minutes of history that will be smoothed into an average when
     * {@link UpdateTask} updates the {@link HostScore}s and the
     * {@link ServiceScore}s.
     * 
     * @see Options#HISTORY_MINUTES
     */
    protected final int historyMinutes;
    
    /**
     * Options understood by the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * The load balancer service will use a round robin approach to
         * recommending under-utilized services until this the load balancer has
         * re-computed the service scores N times (default
         * {@value #DEFAULT_INITIAL_ROUND_ROBIN_UPDATE_COUNT}). This makes it
         * more likely that the initial index partitions will be allocated on
         * services on different hosts for a new federation, but it is really a
         * hack since it depends entirely on the time elapsed since the load
         * balancer service (re-)started. This "feature" may be disabled by
         * setting this property to ZERO (0).
         */
        String INITIAL_ROUND_ROBIN_UPDATE_COUNT = LoadBalancerService.class
                .getName()
                + ".initialRoundRobinUpdateCount";

        /**
         * The default gives you a few minutes after you setup the federation in
         * which newly registered indices will be allocated based on a
         * round-robin.
         */
        String DEFAULT_INITIAL_ROUND_ROBIN_UPDATE_COUNT = "5";
        
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
         * @see AbstractStatisticsCollector.Options#PERFORMANCE_COUNTERS_SAMPLE_INTERVAL
         */
        String UPDATE_DELAY = LoadBalancerService.class.getName()+".updateDelay";
        
        /**
         * The default {@link #UPDATE_DELAY}.
         */
        String DEFAULT_UPDATE_DELAY = ""+(60*1000);

        /**
         * The #of minutes of history that will be smoothed into an average when
         * {@link UpdateTask} updates the {@link HostScore}s and the
         * {@link ServiceScore}s (default {@value #DEFAULT_HISTORY_MINUTES}).
         * 
         * @see QueueStatisticsTask
         */
        String HISTORY_MINUTES = LoadBalancerService.class.getName()
                + ".historyMinutes"; 

        String DEFAULT_HISTORY_MINUTES = "5";
        
        /**
         * The path of the directory where the load balancer will log a copy of
         * the counters every time it runs its {@link UpdateTask}. By default
         * it will log the files in the directory from which the load balancer
         * service was started. You may specify an alternative directory using
         * this property.
         */
        String LOG_DIR = LoadBalancerService.class.getName()+".log.dir";
        
        String DEFAULT_LOG_DIR = ".";

        /**
         * The delay in milliseconds between writes of the {@link CounterSet} on
         * a log file (default is {@value #DEFAULT_LOG_DELAY}, which is
         * equivilent to one hour).
         */
        String LOG_DELAY = LoadBalancerService.class.getName()+".log.delay";
        
        String DEFAULT_LOG_DELAY = "" + 1000 * 60 * 60;

        /**
         * The maximum #of distinct log files to retain (default is one week
         * based on a {@link #LOG_DELAY} equivilant to one hour).
         */
        String LOG_MAX_FILES = LoadBalancerService.class.getName()+".log.maxFiles";

        String DEFAULT_LOG_MAX_FILES = "" + 24 * 7;

        /**
         * Service join timeout in milliseconds - used when we need to wait for
         * a service to join before we can recommend an under-utilized service.
         */
        String SERVICE_JOIN_TIMEOUT = LoadBalancerService.class.getName()
                + ".serviceJoinTimeout";

        String DEFAULT_SERVICE_JOIN_TIMEOUT = "" + (3 * 1000);
        
    }

    /**
     * 
     * Note: The load balancer MUST NOT collect host statistics unless it is the
     * only service running on that host. Normally it relies on another service
     * running on the same host to collect statistics for that host and those
     * statistics are then reported to the load balancer and aggregated along
     * with the rest of the performance counters reported by the other services
     * in the federation. However, if the load balanacer itself collects host
     * statistics then it will only know about and report the current (last 60
     * seconds) statistics for the host rather than having the historical data
     * for the host.
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

            if (INFO)
                log.info(Options.LOG_DIR + "=" + logDir);                

            // ensure exists.
            logDir.mkdirs();
            
        }

        // logDelayMillis
        {
            
            logDelayMillis = Long.parseLong(properties.getProperty(
                    Options.LOG_DELAY, Options.DEFAULT_LOG_DELAY));

            if (INFO)
                log.info(Options.LOG_DELAY + "=" + logDelayMillis);

        }

        // logMaxFiles
        {

            logMaxFiles = Integer.parseInt(properties.getProperty(
                    Options.LOG_MAX_FILES, Options.DEFAULT_LOG_MAX_FILES));

            if (INFO)
                log.info(Options.LOG_MAX_FILES + "=" + logMaxFiles);

        }
        
        {
            
            historyMinutes = Integer.parseInt(properties.getProperty(
                    Options.HISTORY_MINUTES,
                    Options.DEFAULT_HISTORY_MINUTES));
            
            if (INFO)
                log.info(Options.HISTORY_MINUTES+ "="
                        + historyMinutes);
            
            // a reasonable range check.
            if (historyMinutes <= 0 || historyMinutes > 60)
                throw new RuntimeException(Options.HISTORY_MINUTES
                        + " must be in [1:60].");
            
        }

        {
            
            serviceJoinTimeout = Long.parseLong(properties.getProperty(
                    Options.SERVICE_JOIN_TIMEOUT,
                    Options.DEFAULT_SERVICE_JOIN_TIMEOUT));
            
            if (INFO)
                log.info(Options.SERVICE_JOIN_TIMEOUT + "="
                        + serviceJoinTimeout);
            
            if (serviceJoinTimeout <= 0L)
                throw new RuntimeException(Options.SERVICE_JOIN_TIMEOUT
                        + " must be positive.");
            
        }
        
        // setup scheduled runnable for periodic updates of the service scores.
        {

            initialRoundRobinUpdateCount = Long.parseLong(properties
                    .getProperty(Options.INITIAL_ROUND_ROBIN_UPDATE_COUNT,
                            Options.DEFAULT_INITIAL_ROUND_ROBIN_UPDATE_COUNT));

            if (INFO)
                log.info(Options.INITIAL_ROUND_ROBIN_UPDATE_COUNT + "="
                        + initialRoundRobinUpdateCount);

            this.roundRobinServiceLoadHelper = new RoundRobinServiceLoadHelper();
            
            final long delay = Long.parseLong(properties.getProperty(
                    Options.UPDATE_DELAY,
                    Options.DEFAULT_UPDATE_DELAY));

            if (INFO)
                log.info(Options.UPDATE_DELAY + "=" + delay);

            /*
             * Wait a bit longer for the first update task since service may be
             * starting up as well and we need to have the performance counter
             * data on hand before we can do anything.
             */
            final long initialDelay = delay * 2;
            
            final TimeUnit unit = TimeUnit.MILLISECONDS;

            updateService = Executors
                    .newSingleThreadScheduledExecutor(new DaemonThreadFactory
                            (getClass().getName()+".updateService"));
            
            updateService.scheduleWithFixedDelay(new UpdateTask(), initialDelay,
                    delay, unit);

        }
        
    }
    
    @Override
    synchronized public LoadBalancerService start() {
        
        return this;
        
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
        
        if (INFO)
            log.info("begin");
        
        updateService.shutdown();

        // log the final state of the counters.
        logCounters("final");

        super.shutdown();
        
        if (INFO)
            log.info("done");

    }

    synchronized public void shutdownNow() {

        if(!isOpen()) return;

        if (INFO)
            log.info("begin");
        
        updateService.shutdownNow();
        
        // log the final state of the counters.
        logCounters("final");

        super.shutdownNow();
        
        if (INFO)
            log.info("done");

    }

    /**
     * Returns {@link ILoadBalancerService}.
     */
    @Override
    final public Class getServiceIface() {
        
        return ILoadBalancerService.class;
        
    }
    
    /**
     * Normalizes the {@link ServiceScore}s and set them in place.
     * 
     * @param a
     *            The new service scores.
     */
    protected void setHostScores(final HostScore[] a) {

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
         * Compute normalized score, rank, and drank.
         */
        for (int i = 0; i < a.length; i++) {
            
            final HostScore score = a[i];
            
            score.rank = i;
            
            score.drank = ((double)i)/a.length;

            score.score = HostScore.normalize(score.rawScore, totalRawScore);
            
            // update score in global map.
            activeHosts.put(score.hostname, score);

            if (INFO)
                log.info(score.toString());
            
        }
        
        if(INFO) {
            
            log.info("The most active host was: " + a[a.length - 1]);

            log.info("The least active host was: " + a[0]);
        
        }
        
        // Atomic replace of the old scores.
        LoadBalancerService.this.hostScores.set( a );
     
    }
    
    /**
     * Normalizes the {@link ServiceScore}s and set them in place.
     * 
     * @param a
     *            The new service scores.
     */
    protected void setServiceScores(final ServiceScore[] a) {
        
        /*
         * Sort scores into ascending order (least utilized to most utilized).
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
            
            score.drank = ((double) i) / a.length;

            score.score = HostScore.normalize(score.rawScore, totalRawScore);

            // update score in global map.
            activeDataServices.put(score.serviceUUID, score);

            if (INFO)
                log.info(score.toString());

        }
        
        if (INFO) {

            log.info("The most active service was: " + a[a.length - 1]);

            log.info("The least active service was: " + a[0]);

        }

        // Atomic replace of the old scores.
        LoadBalancerService.this.serviceScores.set(a);

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
    protected class UpdateTask implements Runnable {

        /**
         * Note: The logger is named for this class, but since it is an inner
         * class the name uses a "$" delimiter (vs a ".") between the outer and
         * the inner class names.
         */
        final protected Logger log = Logger.getLogger(UpdateTask.class);

        final protected boolean INFO = log.isInfoEnabled();

        final protected boolean DEBUG = log.isDebugEnabled();

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
                
                if (INFO)
                    log.info("No active hosts");
                
                return;
                
            }

            /*
             * Update scores for the active hosts.
             */

            final Vector<HostScore> scores = new Vector<HostScore>();
            
            // For each host
            final Iterator<ICounterSet> itrh = getFederation().getCounterSet()
                    .counterSetIterator();
            
            while(itrh.hasNext()) {
                
                final CounterSet hostCounterSet = (CounterSet) itrh.next();
                
                // Note: name on hostCounterSet is the fully qualified hostname.
                final String hostname = hostCounterSet.getName();
                
                if(!activeHosts.containsKey(hostname)) {

                    // Host is not active.
                    if (DEBUG)
                        log.debug("Host is not active: " + hostname);
                    
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
         * <p>
         * Note: There is a dependency on
         * {@link AbstractFederation#getServiceCounterPathPrefix(UUID, Class, String)}.
         * This method assumes that the service {@link UUID} is found in a
         * specific place in the constructed path.
         */
        protected void updateServiceScores() {
            
            if(activeDataServices.isEmpty()) {
                
                if (INFO)
                    log.info("No active services");
                
                LoadBalancerService.this.serviceScores.set( null );
                
                return;
                
            }
            
            /*
             * Update scores for the active services.
             */

            final Vector<ServiceScore> scores = new Vector<ServiceScore>();
            
            // For each host
            final Iterator<ICounterSet> itrh = getFederation().getCounterSet()
                    .counterSetIterator();
            
            while(itrh.hasNext()) {
                
                final CounterSet hostCounterSet = (CounterSet) itrh.next();
                
                // Note: name on hostCounterSet is the fully qualified hostname.
                final String hostname = hostCounterSet.getName();

                // Pre-computed score for the host on which the service is running.
                final HostScore hostScore = activeHosts.get(hostname);

                if (hostScore == null) {

                    // Host is not active.
                    if (INFO)
                        log.info("Host is not active: " + hostname);

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

                        /*
                         * Note: [name] on serviceCounterSet is the serviceUUID.
                         * 
                         * Note: This creates a dependency on
                         * AbstractFederation#getServiceCounterPathPrefix(...)
                         */
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

                        if (!activeDataServices.containsKey(serviceUUID)) {

                            /*
                             * Note: Only data services are entered in this map,
                             * so this filters out the non-dataServices from the
                             * load balancer's computations.
                             */

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
                            score = activeDataServices.get(serviceUUID);

                            if (score == null) {

                                if (INFO)
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
                        + activeDataServices.size() + " active services");
                
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
         * have failover provisioned for your data services then DO NOT shutdown
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
        protected HostScore computeScore(final String hostname,
                final ICounterSet hostCounterSet) {

            /*
             * Is the host swapping heavily?
             * 
             * @todo if heavy swapping persists then lower the score even
             * further.
             * 
             * @todo The % of the physical memory and the % of the swap space
             * that have been used are also strong indicators.
             */
            final int majorFaultsPerSec = (int) getCurrentValue(hostCounterSet,
                    IRequiredHostCounters.Memory_majorFaultsPerSecond, 0d/* default */);

            /*
             * Is the host out of disk?
             * 
             * FIXME Need the swap space remaining. Low swap presages heavy
             * swapping.
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
                    hostCounterSet, IRequiredHostCounters.CPU_PercentProcessorTime,
                    .5d, historyMinutes);

            /*
             * The percent of the time that the CPUs are idle when there is an
             * outstanding IO request.
             */
            final double percentIOWait = getAverageValueForMinutes(
                    hostCounterSet, IHostCounters.CPU_PercentIOWait,
                    .01d/* default */, historyMinutes);

            /*
             * Note: This reflects the disk IO utilization primarily through
             * IOWAIT.
             * 
             * @todo Play around with other forumulas too.
             */
            double adjustedRawScore;
            final double baseRawScore = adjustedRawScore = (1d + percentIOWait * 100d)
                    / (1d + percentProcessorIdle);

            if (majorFaultsPerSec > 50) {

                // much higher utilization if the host is swapping heavily.
                adjustedRawScore *= 10;

                log.warn("hostname=" + hostname
                                + " : swapping heavily: pages/sec="
                                + majorFaultsPerSec);

            } else if (majorFaultsPerSec > 10) {

                // higher utilization if the host is swapping.
                adjustedRawScore *= 2d;

                log.warn("hostname=" + hostname + " : swapping: pages/sec="
                        + majorFaultsPerSec);

            }

            if (percentDiskFreeSpace < .05) {

                // much higher utilization if the host is very short on disk.
                adjustedRawScore *= 10d;

                log.warn("hostname=" + hostname
                        + " : very short on disk: freeSpace="
                        + percentDiskFreeSpace * 100 + "%");

            } else if (percentDiskFreeSpace < .10) {

                // higher utilization if the host is short on disk.
                adjustedRawScore *= 2d;

                log.warn("hostname=" + hostname
                        + " : is short on disk: freeSpace="
                        + percentDiskFreeSpace * 100 + "%");

            }

            if (INFO) {

                log.info("hostname=" + hostname + " : adjustedRawScore("
                        + scoreFormat.format(adjustedRawScore)
                        + "), baseRawScore(" + scoreFormat.format(baseRawScore)
                        + ") = (1d + percentIOWait("
                        + percentFormat.format(percentIOWait)
                        + ") * 100d) / (1d + percentProcessorIdle("
                        + percentFormat.format(percentProcessorIdle)
                        + "), majorFaultsPerSec=" + majorFaultsPerSec
                        + ", percentDiskSpaceFree="
                        + percentFormat.format(percentDiskFreeSpace));

            }
            
            final HostScore hostScore = new HostScore(hostname, adjustedRawScore);

            return hostScore;

        }
        
        /**
         * Format for the computed scores.
         */
        final NumberFormat scoreFormat;
        {
            scoreFormat = NumberFormat.getInstance();
            scoreFormat.setMaximumFractionDigits(2);
            scoreFormat.setMinimumIntegerDigits(1);
        }
        /**
         * Format for percentages such as <code>IO Wait</code> where the
         * values are in [0.00:1.00].
         */
        final NumberFormat percentFormat;
        {
            percentFormat = NumberFormat.getInstance();
            percentFormat.setMaximumFractionDigits(2);
            percentFormat.setMinimumIntegerDigits(1);
        }
        
        /**
         * Format for elapsed times measured in milliseconds.
         */
        final NumberFormat millisFormat;
        {
           millisFormat = NumberFormat.getIntegerInstance();
        }
        
        /**
         * Format for bytes.
         */
        final NumberFormat bytesFormat;
        {
           bytesFormat = NumberFormat.getIntegerInstance();
           bytesFormat.setGroupingUsed(true);
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
        protected ServiceScore computeScore(final HostScore hostScore,
                final UUID serviceUUID, final ICounterSet hostCounterSet,
                final ICounterSet serviceCounterSet) {

            assert hostScore != null;
            assert serviceUUID != null;
            assert hostCounterSet != null;
            assert serviceCounterSet != null;
            
            // verify that the host score has been normalized.
            assert hostScore.rank != -1 : hostScore.toString();
            
            // resolve the service name : @todo refactor RMI out of this method.
            String serviceName = "N/A";
            try {
                serviceName = getFederation().getDataService(serviceUUID)
                        .getServiceName();
            } catch (Throwable t) {
                log.warn(t.getMessage(), t);
            }
            
            /*
             * The average queuing time for the unisolated write service is used
             * as the primary indicator of the write load of the service. The
             * average queueing time is preferred to the average queue length as
             * the queueing time is directly correlated to the throughput of the
             * service.
             * 
             * Note: We use the measure of write load to drive load balancing
             * decisions. This is in contrast to high availability for readers,
             * where readers can be directed to failover instances.
             * 
             * @todo verify that the queueing time measurement in millis is
             * sufficient rather than nanos as queuing times can become quite
             * short.
             * 
             * @todo There is a lot more that can be considered and under linux
             * we have access to per-process counters for CPU, DISK, and MEMORY.
             */

            final double averageQueueLength = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.concurrencyManager
                            + ps + IConcurrencyManagerCounters.writeService
                            + ps + IQueueCounters.AverageQueueLength,
                    0d/* default (queueLength) */, historyMinutes);

            final double averageQueueingTime = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.concurrencyManager
                            + ps + IConcurrencyManagerCounters.writeService
                            + ps + IQueueCounters.AverageQueuingTime,
                    100d/* default (ms) */, historyMinutes);

            final double dataDirBytesAvailable = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.resourceManager
                            + ps + IResourceManagerCounters.StoreManager
                            + ps + IStoreManagerCounters.DataDirBytesAvailable,
                    Bytes.gigabyte * 20/* default */, historyMinutes);
            
            final double tmpDirBytesAvailable = getAverageValueForMinutes(
                    serviceCounterSet, IDataServiceCounters.resourceManager
                            + ps + IResourceManagerCounters.StoreManager
                            + ps + IStoreManagerCounters.TmpDirBytesAvailable,
                    Bytes.gigabyte * 2/* default */, historyMinutes);

            final double rawScore = (averageQueueLength + 1) * (hostScore.score + 1);

            double adjustedRawScore = rawScore;
            
            /*
             * dataDir
             */
            
            if (dataDirBytesAvailable < Bytes.gigabyte * 1) {

                // much higher utilization if the host is very short on disk.
                adjustedRawScore *= 10d;

                log.warn("service=" + serviceName
                        + " : very short on disk: "+IStoreManagerCounters.TmpDirBytesAvailable+"="
                        + bytesFormat.format(dataDirBytesAvailable));

            } else if (dataDirBytesAvailable < Bytes.gigabyte * 10) {

                // higher utilization if the host is short on disk.
                adjustedRawScore *= 2d;

                log.warn("service=" + serviceName
                        + " : is short on disk: "+IStoreManagerCounters.DataDirBytesAvailable+"="
                        + bytesFormat.format(dataDirBytesAvailable));

            }

            /*
             * tmpDir
             */

            if (tmpDirBytesAvailable < Bytes.gigabyte * 1) {

                // much higher utilization if the host is very short on disk.
                adjustedRawScore *= 10d;

                log.warn("service=" + serviceName
                        + " : very short on disk: "+IStoreManagerCounters.TmpDirBytesAvailable+"="
                        + bytesFormat.format(tmpDirBytesAvailable));

            } else if (tmpDirBytesAvailable < Bytes.gigabyte * 10) {

                // higher utilization if the host is short on disk.
                adjustedRawScore *= 2d;

                log.warn("service=" + serviceName
                        + " : is short on disk: "+IStoreManagerCounters.TmpDirBytesAvailable+"="
                        + bytesFormat.format(tmpDirBytesAvailable));

            }

            if (INFO) {
             
                log.info("serviceName=" + serviceName//
                        + ", serviceUUID=" + serviceUUID //
                        + ", averageQueueLength=" + averageQueueLength//
                        + ", averageQueueingTime=" + millisFormat.format(averageQueueingTime)//
                        + ", dataDirBytesAvail="+bytesFormat.format(dataDirBytesAvailable)//
                        + ", tmpDirBytesAvail="+bytesFormat.format(tmpDirBytesAvailable)//
                        + ", adjustedRawStore="+adjustedRawScore//
                        + ", rawScore(" + scoreFormat.format(rawScore) + ") "//
                        + "= (averageQueueingTime("+ averageQueueingTime+ ") + 1) "//
                        + "* (hostScore("+ scoreFormat.format(hostScore.score) + ") + 1)"//
                        );
                
            }

            final ServiceScore score = new ServiceScore(hostScore.hostname,
                    serviceUUID, serviceName, adjustedRawScore);
            
            return score;
            
        }

        protected double getCurrentValue(ICounterSet counterSet, String path,
                double defaultValue) {

            assert counterSet != null;
            assert path != null;

            final ICounter c = (ICounter) counterSet.getPath(path);

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
        protected double getAverageValueForMinutes(
                final ICounterSet counterSet, final String path,
                final double defaultValue, final int minutes) {

            assert counterSet != null;
            assert path != null;

            final ICounter c = (ICounter) counterSet.getPath(path);

            if (c == null)
                return defaultValue;

            try {

                HistoryInstrument inst = (HistoryInstrument)c.getInstrument();
                
                double val = (Double) inst.minutes.getAverage(minutes);

                return val;

            } catch (Exception ex) {

                log.warn("Could not read double value: counterSet="
                        + counterSet.getPath() + ", counter=" + path, ex);

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
            
            final CounterSet serviceRoot = getFederation()
                    .getServiceCounterSet();

            final long now = System.currentTimeMillis();
            
            // per-host scores.
            {

//                if (INFO)
//                    log.info("Will setup counters for " + activeHosts.size()
//                            + " hosts");
                
                final CounterSet tmp = serviceRoot.makePath("hosts");

                synchronized (tmp) {
                    
                    for (HostScore hs : activeHosts.values()) {

                        final String hn = hs.hostname;

                        if (tmp.getChild(hn) == null) {

//                            if (INFO)
//                                log.info("Adding counter for host: " + hn);
                            
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
                
//                if (INFO)
//                    log.info("Will setup counters for " + activeDataServices.size()
//                            + " services");

                final CounterSet tmp = serviceRoot.makePath("services");

                synchronized (tmp) {

                    for (ServiceScore ss : activeDataServices.values()) {

                        final String uuidStr = ss.serviceUUID.toString();

                        if (tmp.getChild(uuidStr) == null) {

//                            if(INFO)
//                                log.info("Adding counter for service: "+uuidStr);

                            tmp.addCounter(uuidStr, new HistoryInstrument<String>(new String[]{}));
                            
                        }
                        
                        {

                            final ICounter counter = (ICounter) tmp.getChild(uuidStr);

                            final HistoryInstrument<String> inst = (HistoryInstrument<String>) counter
                                    .getInstrument();

                            final ServiceScore score = activeDataServices
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
    protected void logCounters(final String basename) {

        final File file = new File(logDir, "counters" + basename + ".xml");

        if (INFO)
            log.info("Writing counters on " + file);
        
        OutputStream os = null;
        
        try {

            os = new BufferedOutputStream( new FileOutputStream(file) );
            
            getFederation().getCounterSet().asXML(os, "UTF-8", null/* filter */);
            
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
     * Note: Embedded services must invoke this method <em>directly</em> when
     * they start up.
     * <p>
     * Note: Distributed services implementations MUST discover services using a
     * framework, such as jini, and invoke this method the first time a given
     * service is discovered.
     * 
     * @param serviceUUID
     * @param serviceIface
     * @param hostname
     * 
     * @see IFederationDelegate#serviceJoin(IService, UUID)
     * @see #leave(String, UUID)
     */
    public void join(final UUID serviceUUID, final Class serviceIface,
            final String hostname) {
      
        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (serviceIface == null)
            throw new IllegalArgumentException();

        if (hostname == null)
            throw new IllegalArgumentException();

        if (INFO)
            log.info("serviceUUID=" + serviceUUID + ", serviceIface="
                    + serviceIface + ", hostname=" + hostname);
        
        String serviceName = "N/A";//@todo should really be passed in to avoid boundback RMI.
        if (IDataService.class == serviceIface) {
        try {
            serviceName = getFederation().getDataService(serviceUUID).getServiceName();
        } catch(Throwable t) {
            log.warn(t.getMessage(),t);
        }}
        
        lock.lock();

        try {

            if (activeHosts.putIfAbsent(hostname, new HostScore(hostname)) == null) {

                if (INFO)
                    log.info("New host joined: hostname=" + hostname);

            }

            if (IDataService.class == serviceIface) {

                /*
                 * Add to set of known services.
                 * 
                 * Only data services are registered as [activeServices] since
                 * we only make load balancing decisions for the data services.
                 */
                if (activeDataServices.putIfAbsent(serviceUUID,
                        new ServiceScore(hostname, serviceUUID, serviceName)) == null) {

                    if (INFO)
                        log.info("Data service join: hostname=" + hostname
                                + ", serviceUUID=" + serviceUUID);

                }

            }

            if (getServiceUUID() != null) {
                
                /*
                 * Create node for the joined service's history in the load
                 * balancer's counter set. This just gives eager feedback in the
                 * LBS's counter set if you are using the httpd service to watch
                 * for service joins.
                 * 
                 * Note: We can't do this until the load balancer has its own
                 * serviceUUID. If that is not available now, then the node for
                 * the joined service will be created when that service
                 * notify()s the LBS (60 seconds later).
                 */

                getFederation().getCounterSet().makePath(
                        AbstractFederation.getServiceCounterPathPrefix(
                                serviceUUID, serviceIface, hostname));
                
            }

            joined.signal();

        } finally {

            lock.unlock();

        }
        
    }

    /**
     * Notify the {@link LoadBalancerService} that a service is no longer
     * available.
     * <p>
     * Note: Embedded services must invoke this method <em>directly</em> when
     * they shut down.
     * <p>
     * <p>
     * Note: Distributed services implementations MUST discover services using a
     * framework, such as jini, and invoke this method when a service is no
     * longer registered.
     * 
     * @param serviceUUID
     *            The service {@link UUID}.
     * 
     * @see IFederationDelegate#serviceLeave(UUID)
     * @see #join(UUID, Class, String)
     */
    public void leave(final UUID serviceUUID) {

        if (INFO)
            log.info("serviceUUID=" + serviceUUID);

        try {

            lock.lock();

            /*
             * Note: [activeServices] only contains the DataServices so a null
             * return means either that this is not a data service -or- that we
             * do not have a score for that data service yet.
             */
            final ServiceScore info = activeDataServices.remove(serviceUUID);

            if (info != null) {

                /*
                 * @todo remove history from counters - path is
                 * /host/serviceUUID? Consider scheduling removal after a few
                 * hours or just sweeping periodically for services with no
                 * updates in the last N hours so that people have access to
                 * post-mortem data. For the same reason, we should probably
                 * snapshot the data prior to the leave (especially if there are
                 * WARN or URGENT events for the service) and perhaps
                 * periodically snapshot all of the counter data onto rolling
                 * log files.
                 */

                // root.deletePath(path);
            }

        } finally {

            lock.unlock();

        }

    }

    public void notify(final UUID serviceUUID, final byte[] data) {

        setupLoggingContext();
        
        try {
        
            if (INFO)
                log.info("serviceUUID=" + serviceUUID);

            if (!serviceUUID.equals(getServiceUUID())) {

                /*
                 * Don't do this for the load balancer itself!
                 * 
                 * @todo the LBS probably should not bother to notify() itself.
                 */
                
                try {

                    // read the counters into our local history.
                    getFederation().getCounterSet().readXML(
                            new ByteArrayInputStream(data), instrumentFactory,
                            null/* filter */);

                } catch (Exception e) {

                    log.warn(e.getMessage(), e);

                    throw new RuntimeException(e);

                }

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

    public boolean isHighlyUtilizedDataService(final UUID serviceUUID)
            throws IOException {
    
        setupLoggingContext();

        try {

            final ServiceScore[] scores = this.serviceScores.get();

            // No scores yet?
            if (scores == null) {

                if(INFO) log.info("No scores yet");

                return false;

            }

            final ServiceScore score = activeDataServices.get(serviceUUID);

            if (score == null) {

                if (INFO)
                    log.info("Service is not scored: " + serviceUUID);

                return false;

            }

            return isHighlyUtilizedDataService(score, scores);
            
        } finally {

            clearLoggingContext();

        }
        
    }

    public boolean isUnderUtilizedDataService(final UUID serviceUUID)
            throws IOException {

        setupLoggingContext();
        
        try {

            final ServiceScore[] scores = this.serviceScores.get();

            // No scores yet?
            if (scores == null) {

                if(INFO) log.info("No scores yet");

                return false;

            }

            final ServiceScore score = activeDataServices.get(serviceUUID);

            if (score == null) {

                if (INFO)
                    log.info("Service is not scored: " + serviceUUID);

                return false;

            }
          
            return isUnderUtilizedDataService(score, scores);
            
        } finally {
            
            clearLoggingContext();

        }

    }

    protected boolean isHighlyUtilizedDataService(final ServiceScore score,
            final ServiceScore[] scores) {

        if (score == null)
            throw new IllegalArgumentException();

        if (scores == null)
            throw new IllegalArgumentException();

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

    protected boolean isUnderUtilizedDataService(final ServiceScore score,
            final ServiceScore[] scores) {

        if (score == null)
            throw new IllegalArgumentException();

        if (scores == null)
            throw new IllegalArgumentException();

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
    
    public UUID getUnderUtilizedDataService() throws IOException,
            TimeoutException, InterruptedException {

        return getUnderUtilizedDataServices(1, 1, null/* exclude */)[0];

    }

    public UUID[] getUnderUtilizedDataServices(final int minCount,
            final int maxCount, final UUID exclude) throws IOException,
            TimeoutException, InterruptedException {

        setupLoggingContext();

        try {
            
            if (minCount < 0)
                throw new IllegalArgumentException();

            if (maxCount < 0)
                throw new IllegalArgumentException();

            final UUID[] uuids;
            
            lock.lock();

            try {

                uuids = getUnderUtilizedDataServicesWithLock(minCount,
                        maxCount, exclude);

            } finally {

                lock.unlock();

            }

            if (INFO)
                log.info("minCount=" + minCount + ", maxCount=" + maxCount
                        + ", exclude=" + exclude + " : reporting "
                        + uuids.length
                        + " under-utilized and non-excluded services: "
                        + Arrays.toString(uuids));
            
            return uuids;

        } finally {

            clearLoggingContext();

        }

    }

    /**
     * Impl. runs with {@link #lock}.
     * 
     * @param minCount
     * @param maxCount
     * @param exclude
     * @return
     * @throws TimeoutException
     * @throws InterruptedException
     */
    private UUID[] getUnderUtilizedDataServicesWithLock(final int minCount,
            final int maxCount, final UUID exclude) throws TimeoutException,
            InterruptedException {

        if (DEBUG)
            log.debug("minCount=" + minCount + ", maxCount=" + maxCount
                    + ", exclude=" + exclude);
        
        if (nupdates < initialRoundRobinUpdateCount) {

            /*
             * Use a round-robin assignment for the first N updates while the
             * LBS develops some history on the hosts and services.
             */
            return roundRobinServiceLoadHelper.getUnderUtilizedDataServices(
                    minCount, maxCount, exclude);

        }

        /*
         * Scores for the services in ascending order (least utilized to most
         * utilized).
         */
        final ServiceScore[] scores = this.serviceScores.get();

        if (scores == null || scores.length == 0) {

            if (minCount == 0) {

                if (DEBUG)
                    log
                            .debug("No scores, minCount is zero - will return null.");

                return null;

            }

            /*
             * Scores are not available immediately. This will await a
             * non-excluded service join and then return the "under-utilized"
             * services without reference to computed service scores. This path
             * is used when the load balancer first starts up (unless the round
             * robin is enabled) since it will not have scores for at least one
             * pass of the UpdateTask.
             */

            return new ServiceLoadHelperWithoutScores()
                    .getUnderUtilizedDataServices(minCount, maxCount, exclude);

        }

        /*
         * Count the #of non-excluded active services - this is [nok].
         * 
         * Note: [knownGood] is set to a service that (a) is not excluded; and
         * (b) is active. This is the fallback service that we will recommend if
         * minCount is non-zero and we are using the scores and all of a sudden
         * it looks like there are no active services to recommend. This
         * basically codifies a decision point where we accept that this service
         * is active. We choose this as the first active and non-excluded
         * service so that it will be as under-utilized as possible.
         */
        int nok = 0;
        UUID knownGood = null;
        for (int i = 0; i < scores.length && nok < 1; i++) {

            final UUID serviceUUID = scores[i].serviceUUID;

            if (exclude != null && exclude.equals(serviceUUID))
                continue;

            if (!activeDataServices.containsKey(serviceUUID))
                continue;

            if (knownGood == null)
                knownGood = serviceUUID;

            nok++;

        }

        if (nok == 0) {

            /*
             * There are no non-excluded active services.
             */

            if (DEBUG)
                log.debug("No non-excluded services.");

            if (minCount == 0) {

                /*
                 * Since there was no minimum #of services demanded by the
                 * caller, we return [null].
                 */

                if (DEBUG)
                    log
                            .debug("No non-excluded services, minCount is zero - will return null.");

                return null;

            } else {

                /*
                 * We do not have ANY active and scored non-excluded services
                 * and [minCount GT ZERO]. In this case we use a he variant that
                 * does not use scores and that awaits a service join.
                 */

                if (DEBUG)
                    log.debug("Will await a service join.");

                return new ServiceLoadHelperWithoutScores()
                        .getUnderUtilizedDataServices(minCount, maxCount,
                                exclude);

            }

        }

        /*
         * Use the scores to compute the under-utilized services.
         */

        if (DEBUG)
            log.debug("Will recommend services based on scores: #scored="
                    + scores.length + ", nok=" + nok + ", knownGood="
                    + knownGood + ", exclude=" + exclude);

        assert nok > 0;
        assert knownGood != null;
        assert scores != null;
        assert scores.length != 0;

        return new ServiceLoadHelperWithScores(knownGood, scores)
                .getUnderUtilizedDataServices(minCount, maxCount, exclude);

    }

    /**
     * Integration with the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class RoundRobinServiceLoadHelper extends
            AbstractRoundRobinServiceLoadHelper {

        protected UUID[] awaitServices(int minCount, long timeout)
                throws InterruptedException, TimeoutException {

            return ((AbstractScaleOutFederation) LoadBalancerService.this
                    .getFederation()).awaitServices(minCount, timeout);

        }

    }

    /**
     * Integration with the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ServiceLoadHelperWithoutScores extends
            AbstractServiceLoadHelperWithoutScores {

        public ServiceLoadHelperWithoutScores() {

            super(serviceJoinTimeout);

        }

        @Override
        protected void awaitJoin(long timeout, TimeUnit unit) throws InterruptedException {

            // await a join.
            joined.await(timeout, unit);
            
        }

        @Override
        protected UUID[] getActiveServices() {
            
            return activeDataServices.keySet().toArray(new UUID[] {});
            
        }

        @Override
        protected boolean isActiveDataService(UUID serviceUUID) {

            return activeDataServices.containsKey(serviceUUID);

        }

        @Override
        protected boolean isUnderUtilizedDataService(ServiceScore score,
                ServiceScore[] scores) {

            return LoadBalancerService.this.isUnderUtilizedDataService(score,
                    scores);
            
        }

    }
    
    /**
     * Integration with the {@link LoadBalancerService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class ServiceLoadHelperWithScores extends
            AbstractServiceLoadHelperWithScores {

        public ServiceLoadHelperWithScores(final UUID knownGood,
                final ServiceScore[] scores) {

            super(serviceJoinTimeout, knownGood, scores);

        }

        @Override
        protected void awaitJoin(long timeout, TimeUnit unit) throws InterruptedException {

            // await a join.
            joined.await(timeout, unit);
            
        }

        @Override
        protected UUID[] getActiveServices() {
            
            return activeDataServices.keySet().toArray(new UUID[] {});
            
        }

        @Override
        protected boolean isActiveDataService(UUID serviceUUID) {

            return activeDataServices.containsKey(serviceUUID);

        }

        @Override
        protected boolean isUnderUtilizedDataService(ServiceScore score,
                ServiceScore[] scores) {

            return LoadBalancerService.this.isUnderUtilizedDataService(score,
                    scores);
            
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
