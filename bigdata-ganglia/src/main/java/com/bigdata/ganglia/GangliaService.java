/*
   Copyright (C) SYSTAP, LLC 2006-2016.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.bigdata.ganglia.util.DaemonThreadFactory;

/**
 * A full ganglia-aware service suitable for embedding with a Java application.
 * The service listens for metrics declarations (ganglia metadata records),
 * metric data, and metric requests. Metric requests are honored as is the
 * restart protocol necessary to avoid multicast storms. In addition to its role
 * as a listener, the service multicasts metric declarations and metrics
 * obtained either from the underlying platform (OS/host) or from the
 * application into which this service is embedded.
 * <p>
 * Applications can embed this service by adding their own
 * {@link IGangliaMetadataFactory} instances (to get nice metric declarations)
 * and can register an {@link IGangliaMetricsCollector} to periodically report
 * performance metrics out to the ganglia network. You can also use
 * {@link #setMetric(String, Object)} to directly update a metric.
 * <p>
 * The ganglia protocol replicates soft state into all <code>gmond</code> and
 * <code>gmetad</code> instances. The {@link GangliaService} is a full
 * participant in the protocol and will also develop a snapshot of the soft
 * state of the cluster. This protocol has a lot of benefits, but the metadata
 * declarations can stick around and be discovered long after you have shutdown
 * the {@link GangliaService}.
 * <p>
 * When developing with the embedded {@link GangliaService} it is a Good Idea to
 * use a private <code>gmond</code> / <code>gmetad</code> configuration
 * (non-default address and/or port). Then, if you wind up making some mistakes
 * when declaring your application counters and those mistakes replicated into
 * the soft state of the ganglia services you can just shutdown your private
 * service instances.
 * <p>
 * Another trick is to use the [mock] mode (it is a constructor argument) while
 * you are developing so the {@link GangliaService} does not actually send out
 * any packets.
 * <p>
 * Finally, there is also a utility class (in the test suite) which can be used
 * to monitor the ganglia network for packets which can not be round-tripped.
 * This can be used to identify bugs if you are doing development on embedded
 * {@link GangliaService} code base.
 * 
 * @see <a href="http://ganglia.sourceforge.net/"> The ganglia monitoring
 *      system</a>
 * @see <a
 *      href="http://stackoverflow.com/questions/8393726/how-can-i-diagnose-our-java-ip-multicast-application">
 *      diagnose multicast applications </a>
 * 
 * @author thompsonbry@users.sourceforge.net
 * 
 *         TODO Only allows one join group and port for listening and only
 *         listens by joining a multicast group.
 *         <p>
 *         Unicast support should not be that different. The list of
 *         metricsServers is fine for sending data whether using unicast or
 *         multicast. I need to experiment with a (list of) addresses for
 *         receiving data. Note that ganglia supports more than one address and
 *         port for unicast or multicast. It should not be difficult to do the
 *         same thing under java.
 * 
 *         FIXME Setup a JXM bridge for counters. Anything which make it into
 *         ganglia or perhaps anything which is declared in our counters
 *         hierarchy.
 * 
 *         TODO Turn off heartbeat automatically if we detect a heartbeat with a
 *         different timestamp while this GangliaService is running. That will
 *         help us avoid stomping on gmond.
 * 
 *         TODO Setup a listener for TCP connections on the same port that we
 *         listen on for UDP. If there is a connection, then respond by
 *         streaming the XML data for the current counters. The XML DTD (and
 *         sample data) can be obtained using
 *         <code>telenet localhost 8649</code>. This is the hook used for the
 *         ganglia web UI, some nagios integrations, etc.
 * 
 *         FIXME Provide a sample collector for the JVM and use it in main().
 * 
 *         TODO Port the more sophisticated collectors into this module?
 */
public class GangliaService implements Runnable, IGangliaMetricsReporter {

	private static final Logger log = Logger.getLogger(GangliaService.class);

	/**
	 * Timestamp when the service starts and zero if the service is not running.
	 */
	private final AtomicLong serviceStartTime = new AtomicLong();

	/**
	 * The name of this host.
	 */
	private final String hostName;

	/**
	 * The name of this metrics reporting process.
	 */
	private final String serviceName;

	/**
	 * The ganglia service(s) to which we will send metric data.
	 */
	private final InetSocketAddress[] metricsServers;

	private final InetAddress listenGroup;
	private final int listenPort;

	/**
	 * When <code>true</code> the service will listen for, and respond to,
	 * ganglia messages.
	 */
	private final boolean listen;

	/**
	 * When <code>true</code> the service will report metrics to the configured
	 * ganglia network.
	 */
	private final boolean report;

	/**
	 * When <code>true</code> the service will NOT transmit any packets. This
	 * makes it possible to run the service, including the listener, without
	 * having any side effect on the state of the ganglia network.
	 * <p>
	 * Note: Another technique for testing is to setup a
	 * {@link GangliaListener} and a {@link GangliaService} talking to one
	 * another on a non-default unicast address and port or a non-default
	 * multicast join group and port.
	 */
	private final boolean mock;
	
	/**
	 * The duration in seconds of the quiet period after a ganglia service
	 * start.
	 * 
	 * @see IGangliaDefaults#QUIET_PERIOD
	 */
	private final int quietPeriod;
	
	/** 
	 * The waiting period in seconds before the first reporting pass.
	 * <p>
	 * Note: The core ganglia metrics will be self-declared once this period
	 * has expired if they have not already been observed by listening to the
	 * ganglia network.
	 * 
	 * @see IGangliaDefaults#INITIAL_DELAY
	 */
	private final int initialDelay;

	/**
	 * The delay in seconds between ganglia host heartbeat messages.
	 */
	private final int heartbeatInterval;
	
	/**
	 * The interval at which we monitor the metrics which are collected on this
	 * node. When we examine a metric, its current value is copied into
	 * {@link #gangliaState}. A metric update will be sent at that time if the
	 * metric has never been published (timestamp is zero) or if TMax might
	 * expire before we sample the metric again.
	 * 
	 * @see IGangliaDefaults#MONITORING_INTERVAL
	 */
	private final int monitoringInterval;

	/**
	 * The period after which a host which has not been updated will have its
	 * metrics deleted from the {@link GangliaState}.
	 */
	@SuppressWarnings("unused")
    private final int globalDMax;
	
	/**
	 * Object which knows how to generate {@link IGangliaMetricMessage}s which
	 * are consistent with the {@link IGangliaMetadataMessage} for a given
	 * metric.
	 */
	private final RichMetricFactory metricFactory;
	
	/**
	 * Object which knows how to decode the ganglia wire format into an
	 * {@link IGangliaMessage}.
	 */
	private final IGangliaMessageDecoder messageDecoder;

	/**
	 * Object which knows how to generate the ganglia wire format from an
	 * {@link IGangliaMessage}.
	 */
	private final IGangliaMessageEncoder messageEncoder;

	/**
	 * The known metadata (shared across all hosts) and the current metric
	 * values (for each known host).
	 */
	private final GangliaState gangliaState;

	private ExecutorService sendService = null;
	private ExecutorService listenService = null;
	private ScheduledExecutorService scheduledService = null;
	private GangliaSender gangliaSender = null;
	private FutureTask<Void> listenerFuture = null;

	/*
	 * Host report stuff
	 */
	
	/**
	 * The default set of metrics used when generating a host report. This set
	 * of metrics is the same as the metrics reported by <code>gstat</code>.
	 */
	private static final String[] defaultHostReportOn = new String[] {//
			"cpu_num",//
			"procs_total",//
			"procs_run",//
			"load_one",//
			"load_five",//
			"load_fifteen",//
			"cpu_user",//
			"cpu_nice",//
			"cpu_system",//
			"cpu_idle",//
			"cpu_wio",//
			"gexec"//
	};

    /**
     * Return a copy of the default metrics used to generate {@link IHostReport}
     * s.
     * 
     * @see #getHostReport()
     */
    public String[] getDefaultHostReportOn() {

        return Arrays.copyOf(defaultHostReportOn, defaultHostReportOn.length);
        
	}
	
	/** Place into descending order by load_one. */
	private static final Comparator<IHostReport> defaultHostReportComparator = new HostReportComparator(
			"load_one", false/* asc */);
	
    /**
     * Simple constructor uses the defaults for everything <strong>except the
     * heartbeart</strong>.
     * <p>
     * Note: The heartbeat is set to ZERO (0) under the assumption that
     * <code>gmond</code> will be running on the same host. The
     * {@link GangliaService} MUST NOT send out a heartbeat if
     * <code>gmond</code> is also running on the host since the two heartbeats
     * will be different (they are the start time of <code>gmond</code>) and
     * <code>gmond</code> and <code>gmetad</code> will both get confused.
     * 
     * @param serviceName
     *            The name of the service in which you are embedding ganglia
     *            support.
     * 
     * @throws UnknownHostException
     */
	public GangliaService(final String serviceName) throws UnknownHostException {
			this(
					getCanonicalHostName(),// host name
					serviceName, // embedded service name.
					new InetSocketAddress[] {//metricsServers
							new InetSocketAddress(//
									IGangliaDefaults.DEFAULT_GROUP,//
									IGangliaDefaults.DEFAULT_PORT//
									)},//
					InetAddress.getByName(//listenGroup
							IGangliaDefaults.DEFAULT_GROUP//
							),
					IGangliaDefaults.DEFAULT_PORT,//listenPort
					true,// listen
					true,// report
					false,// mock (does not transmit when true).
					IGangliaDefaults.QUIET_PERIOD,//
					IGangliaDefaults.INITIAL_DELAY,//
					0,// heartbeat => NO HEARTBEAT BY DEFAULT!
					//IGangliaDefaults.HEARTBEAT_INTERVAL,//
					IGangliaDefaults.MONITORING_INTERVAL, //
					IGangliaDefaults.DEFAULT_DMAX,//
					new GangliaMetadataFactory(//metadataFactory
							new DefaultMetadataFactory(//
									IGangliaDefaults.DEFAULT_UNITS,//
									IGangliaDefaults.DEFAULT_SLOPE,//
									IGangliaDefaults.DEFAULT_TMAX,//
									IGangliaDefaults.DEFAULT_DMAX//
									)));
	}
	
    /**
     * Core constructor for an embedded {@link GangliaService} - see
     * {@link #run()} to actually run the service.
     * 
     * @param hostName
     *            The name of this host.
     * @param serviceName
     *            The name of the service which is embedding ganglia support.
     * @param metricsServers
     *            The unicast or multicast addresses for sending out packets.
     * @param listenGroup
     *            The multicast join group on which to listen for packets.
     * @param listenPort
     *            The port for the multicast join group.
     * @param listen
     *            <code>true</code> iff the service should listen for packets.
     * @param report
     *            <code>true</code> iff the service should report metrics.
     * @param mock
     *            <code><code>true</code> iff the service should do everything
     *            EXCEPT sending out the packets (they are logged instead). This
     *            is a debugging option.
     * @param quietPeriod
     *            The ganglia quiet period.
     * @param initialDelay
     *            The initial delay before metrics are reported.
     * @param heartbeatInterval
     *            The ganglia heartbeat interval. <strong>Use ZERO (0) if you
     *            are running <code>gmond</code> on the same host</strong>. That
     *            will prevent the {@link GangliaService} from transmitting a
     *            different heartbeat, which would confuse <code>gmond</code>
     *            and <code>gmetad</code>.
     * @param monitoringInterval
     *            The delay between sweeps to collect performance counters from
     *            the application.
     * @param globalDMax
     *            The global value of DMax.
     * @param metadataFactory
     *            An application hook for providing nice metric declarations.
     * 
     * @see IGangliaDefaults
     */
	public GangliaService(
			final String hostName,//
			final String serviceName,//
			// socket(s) to send on.
			final InetSocketAddress[] metricsServers,//
			// multicast IP address and port to listen on
			final InetAddress listenGroup, final int listenPort,//
			final boolean listen,//
			final boolean report,//
			final boolean mock,
			final int quietPeriod,//
			final int initialDelay,//
			final int heartbeatInterval,//
			final int monitoringInterval,//
			final int globalDMax,
			final IGangliaMetadataFactory metadataFactory//
			) {

		if (hostName == null)
			throw new IllegalArgumentException();

		if (serviceName == null)
			throw new IllegalArgumentException();

		if (metricsServers == null || metricsServers.length == 0)
			throw new IllegalArgumentException();

		if (listenGroup == null)
			throw new IllegalArgumentException();

		if (listenPort <= 0)
			throw new IllegalArgumentException();

		if (quietPeriod < 0)
			throw new IllegalArgumentException();

		if (initialDelay <= 0)
			throw new IllegalArgumentException();

		if (heartbeatInterval < 0)
			throw new IllegalArgumentException();

		if (monitoringInterval <= 0)
			throw new IllegalArgumentException();

		if (globalDMax <= 0)
			throw new IllegalArgumentException();

		if (metadataFactory == null)
			throw new IllegalArgumentException();

		this.hostName = hostName;

		this.serviceName = serviceName;

		this.metricsServers = metricsServers;

		this.listenGroup = listenGroup;

		this.listenPort = listenPort;

		this.listen = listen;
		
		this.report = report;
		
		this.mock = mock;
		
		this.quietPeriod = quietPeriod;

		this.initialDelay = initialDelay;
		
		this.heartbeatInterval = heartbeatInterval;
		
		this.monitoringInterval = monitoringInterval;
		
		this.globalDMax = globalDMax;

		/*
		 * Note: The 3.1 format is assumed is wired into these two implementation
		 * classes.
		 */ 
		this.messageEncoder = new GangliaMessageEncoder31();
		
		this.messageDecoder = new GangliaMessageDecoder31();
		
		this.metricFactory = new RichMetricFactory();
		
		this.gangliaState = new GangliaState(hostName, metadataFactory);

	}

	/**
	 * Return the time in seconds since the last service start.
	 * 
	 * @throws IllegalStateException
	 *             if the service is not running.
	 */
	protected long uptime() {

		final long t = serviceStartTime.get();

		if (t == 0) {

			throw new IllegalStateException("Not running.");

		}

		return TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - t);

	}

	/**
	 * Return <code>true</code> iff this service is in its quite period.
	 * <p>
	 * Ganglia has a quiet period for 10m after a service start. During that
	 * quiet period a metadata request will not cause the timestamp of the
	 * metrics to be reset.
	 */
	protected boolean isQuietPeriod() {

		return uptime() < quietPeriod;

	}

    /**
     * Return <code>true</code> if the {@link GangliaService} is currently
     * listening.
     */
	protected boolean isListening() {
	    
	    if(!listen)
	        return false;
	    
	    final Future<?> f = listenerFuture;
	    
	    if(f == null)
	        return false;
	    
	    if(f.isDone())
	        return false;
	    
	    return true;
	    
	}
	
	/**
	 * Run the ganglia service.
	 */
	public void run() {

	    GangliaListener gangliaListener = null;
	    
		try {

			final ThreadFactory threadFactory = new DaemonThreadFactory(
					"GangliaService");

			// Timestamp when the service starts.
			if (!serviceStartTime.compareAndSet(0L, System.currentTimeMillis())) {

				throw new IllegalStateException("Already running.");

			}

			/*
			 * Start thread pools.
			 */

			if(listen) 
				listenService = Executors.newSingleThreadExecutor(threadFactory);

			if(report)
				sendService = Executors.newSingleThreadExecutor(threadFactory);

			/*
			 * corePoolSize := HeartBeatTask + GatherTask + PurgeMetricsTask
			 */
			scheduledService = Executors.newScheduledThreadPool(3,
					threadFactory);

			// Setup sender.
			gangliaSender = new GangliaSender(metricsServers,
                    IGangliaDefaults.BUFFER_SIZE);

            /*
             * Start processes.
             */

            if (listen) {

                // Setup listener.
                gangliaListener = new GangliaListener(listenGroup, listenPort,
                        messageDecoder, new GangliaServiceHandler());

                // Wrap as Future.
                listenerFuture = new FutureTask<Void>(gangliaListener);

                // Start listener
                listenService.submit(listenerFuture);

            }

            if (report) {

                if (heartbeatInterval > 0) {

                    /*
                     * Heartbeat for the host.
                     * 
                     * Note: When ZERO (0), we assume that gmond is running and
                     * that it will take care of this metric for us.
                     * 
                     * Note: DO NOT enable the heartbeat if gmond is running on
                     * the host. The heartbeat is the start time of gmond.
                     * Having two different heartbeats for the same host will
                     * look like gmond is being bounced every time a heatbeat
                     * is sent out by the GangliaService or gmond!
                     */

				    scheduledService.scheduleWithFixedDelay(new HeartBeatTask(),
							initialDelay, heartbeatInterval, TimeUnit.SECONDS);
					
				}

				// metric collection and reporting.
				scheduledService.scheduleAtFixedRate(//
						new GatherMetricsTask(),//
						initialDelay,//
						monitoringInterval,// period
						TimeUnit.SECONDS// units
						);

				// Schedule task to prune old hosts and metrics.
				scheduledService.scheduleAtFixedRate(new PurgeMetricsTask(),
						60/* initialDelay */, 60/* period */, TimeUnit.SECONDS);
				
			}

            if (log.isInfoEnabled())
                log.info("Running on " + hostName + " for " + serviceName);

			if (listen) {

				/*
				 * Blocks while the listener is running or until this thread is
				 * interrupted.
				 */

				listenerFuture.get();

			} else {

				/*
				 * Wait until the server is terminated.
				 */

				synchronized (keepAlive) {

					try {

						keepAlive.wait();

					} catch (InterruptedException ex) {

						// Ignore.

					}

				}

			}

		} catch (InterruptedException t) {

		    // Ignore. Normal shutdown.
		    
		} catch (Throwable t) {

			log.error(t, t);

		} finally {
		    
            if (listenerFuture != null) {
				listenerFuture.cancel(true/* mayInterruptIfRunning */);
			}

			if (listenService != null) {
				listenService.shutdownNow();
			}

            if (gangliaSender != null) {
                /*
                 * Send through a mock message. The GangliaListener can be
                 * blocked in DatagramSocket.receive(). This will get it
                 * unblocked so it can exit in a timely manner.
                 * 
                 * TODO Java 7 supports non-blocking multicast channels. We
                 * would not have to do this with a listener class which is
                 * using NIO multicast support since it would not be blocked
                 * until the next packet.
                 */
                sendMessage(new GangliaRequestMessage(hostName, "shutdown",
                        false/* spoof */));
            }
            
			if (sendService != null) {
				sendService.shutdownNow();
			}

			if (gangliaSender != null)
				gangliaSender.close();

			// Reset the soft state.
			gangliaState.reset();
			
			// Clear the service start time.
			serviceStartTime.set(0L);

		}

	}

	private final Object keepAlive = new Object();

	/**
	 * Handler for ganglia messages.
	 */
	private class GangliaServiceHandler implements IGangliaMessageHandler {

		@Override
		public void accept(final IGangliaMessage msg) {

			if (msg == null)
				return;

			/*
			 * true iff the message is from this host.
			 * 
			 * Note: A message from this host could be from either an embedded
			 * ganglia service (such as ourselves) or from gmond or even gmetad
			 * (if we are running on the same host as gmetad).
			 * 
			 * It is Ok to accept metric value updates from ourselves, and this
			 * makes it possible to listen to a gmond instance on the same host.
			 * However, the receipt of a metric value MUST NOT cause us to send
			 * out a new metric value record.
			 */

			if (msg.isMetricRequest()) {

				/*
				 * Metadata request.
				 */
				if (isQuietPeriod()) {
					/*
					 * Ignore requests during the quiet period.
					 * 
					 * TODO Review the quite period behavior.  Is this correct?
					 */
					return;
				}

				/*
				 * Lookup the metric value for *this* host.
				 */
				final TimestampMetricValue tmv = gangliaState.getMetric(
						hostName, msg.getMetricName());

				if (tmv != null && tmv.getTimestamp() != 0L) {

					/*
					 * If the requested metric is known on this host then we
					 * reset it now. The next time we sample that metric it will
					 * have a timestamp of ZERO (0) and will be sent out to all
					 * listeners. Since the timestamp is ZERO (0) (rather than a
					 * valid timestamp at which the metric was last sent), the
					 * metadata record will also be sent out.
					 */

					tmv.resetTimestamp();

					if (log.isInfoEnabled())
						log.info("Reset metric timestamp: " + msg.toString());

				}

			} else if (msg.isMetricMetadata()) {

				/*
				 * Handle metadata message.
				 * 
				 * TODO Allow a pattern for metrics that we will accept. Do not
				 * store the declaration unless it satisfies that pattern.
				 */

				if (gangliaState.putIfAbsent((IGangliaMetadataMessage) msg) == null) {

					if (log.isInfoEnabled())
						log.info("declared by host: " + msg.toString());

				}

			} else if (msg.isMetricValue()) {

				/*
				 * TODO Allow a pattern for metrics that we will accept. Do not
				 * store the declaration unless it satisfies that pattern.
				 */
				
				if (gangliaState.getMetadata(msg.getMetricName()) == null) {
					
					/*
					 * Request the metadata for the metric.
					 * 
					 * Note: We will not accept the metric record into our
					 * internal state until we have received a metadata record
					 * for that metric.
					 */

					if (log.isDebugEnabled())
						log.debug("No metadata for metric: "
								+ msg.getMetricName());

					sendMessage(new GangliaRequestMessage(hostName,
							msg.getMetricName(), false/* spoof */));

					return;
					
				}

				/*
				 * Update the counters for the appropriate host.
				 * 
				 * Note: At this point we have verified that we have a metadata
				 * declaration on hand for this metric so we can go ahead and
				 * store the metric value in our internal state.
				 */

				final IGangliaMetricMessage m2 = (IGangliaMetricMessage) msg;

				final TimestampMetricValue tmv = gangliaState.getMetric(
						msg.getHostName(), msg.getMetricName());

				// Guaranteed non-null since metadata decl exists for metric.
				assert tmv != null;
				
				/*
				 * Update the value.
				 * 
				 * Note: The return value is ignored. Since the metric was
				 * received over the wire we ignore the age of the metric rather
				 * than using the metric age to trigger a send of the metric
				 * value. (I.e., avoid re-sending received metrics.)
				 */
				tmv.setValue(m2.getValue());

				if (log.isDebugEnabled())
					log.debug("Updated value: " + msg);

			} else {

				log.error("Unknown message type: " + msg);

			}

		}

	}

	/**
	 * Queue an {@link IGangliaMessage} to be sent to the network.
	 * 
	 * @param msg
	 *            The message.
	 */
	protected void sendMessage(final IGangliaMessage msg) {

		if (msg == null)
			log.error("No message");

		final ExecutorService service = sendService;

		if (sendService != null) {

			try {

				if (mock) {
					/*
					 * Do not actually send anything.
					 */
					if (log.isInfoEnabled())
						log.info(msg);
				} else {
					/*
					 * Submit a task to send the message. 
					 */
					service.submit(new SendMessage31(msg));
				}

			} catch (RejectedExecutionException ex) {

				/*
				 * Note: This is the expected behavior if the sendService has
				 * been asynchronously closed.
				 * 
				 * Note: If you see this while the GangliaService is live then
				 * the sendService may have a bounded queue.
				 * 
				 * TODO The sendService should be configured to drop tasks if it
				 * is too busy (nothing wrong with that for ganglia), but not to
				 * reject tasks.
				 */

			    if(log.isInfoEnabled()) log.info(ex);

			}

		}

	}

	/**
	 * Class dispatches an {@link IGangliaMessage}.
	 */
	private class SendMessage31 implements Callable<Void> {
		
		private final IGangliaMessage msg;
		
		public SendMessage31(final IGangliaMessage msg) {
		
			if(msg == null)
				throw new IllegalArgumentException();
			
			this.msg = msg;
			
		}

		public Void call() throws Exception {

			final GangliaSender tmp = gangliaSender;

			if (gangliaSender != null) {

				if (msg.isMetricMetadata()) {

					messageEncoder.writeMetadata(tmp.xdr,
							(IGangliaMetadataMessage) msg);

				} else if (msg.isMetricRequest()) {

					messageEncoder.writeRequest(tmp.xdr,
							(IGangliaRequestMessage) msg);

				} else if (msg.isMetricValue()) {

					messageEncoder.writeMetric(tmp.xdr,
							((RichMetricMessage) msg).getMetadata(),
							(IGangliaMetricMessage) msg);

				} else {

					throw new AssertionError();

				}

				tmp.sendMessage(tmp.xdr);

			}

			return (Void) null;
			
		}
		
	}
	
	/**
	 * Class sends out a heartbeat metric for this host.
	 * <p>
	 * Note: The problem with both the {@link GangliaService} and gmond issuing
	 * heartbeats for the same host is that they need to synchronize the counter
	 * values that they are issuing or it will confuse other gmond and gmetad
	 * instances.
	 */
	private class HeartBeatTask implements Runnable {

		public HeartBeatTask() {
			
		}
		
		public void run() {

			try {

				IGangliaMetadataMessage decl = gangliaState
						.getMetadata("heartbeat");

				if (decl == null) {
					/*
					 * Inject an appropriate metadata declaration for the
					 * heartbeat.
					 */
					decl = gangliaState.putIfAbsent(GangliaCoreMetricDecls
							.heartbeat(hostName, heartbeatInterval));
				}

				/*
				 * Note: The heartbeat is a UINT32 parameter and expresses the
				 * start time of the service in seconds since the epoch.
				 * 
				 * Note: The message generator will handle the conversion of the
				 * [long] non-negative integer into an XDR message formatted for
				 * uint32 decode. We just ensure that the value represents the
				 * right thing (seconds) and has sufficient width (long) to
				 * represent those seconds.
				 */

				// Timestamp when the GangliaService started (seconds).
				final long value = TimeUnit.MILLISECONDS
						.toSeconds(serviceStartTime.get());

				final IGangliaMetricMessage msg = metricFactory
						.newMetricMessage(getHostName(), decl,
								false/* spoof */, value);

				sendMessage(msg);
				
			} catch (Throwable t) {

				/*
				 * Note: Do NOT throw anything out of this task. It will cause
				 * the task to not be re-executed and metrics reporting will
				 * halt!
				 */

				log.warn(t, t);

			}
			
		}
		
	}
	
	/**
	 * Class will delete hosts from which we have not heard in
	 * {@link #globalDMax} seconds and individual metrics whose DMax has been
	 * exceeded.
	 * <p>
	 * Note: There should be an initialDelay of maybe a minute before running
	 * this task. It does not need to run all that frequently. Once per minute
	 * should be fine. It will scan all the counters when it does run, so once a
	 * minute seems like a good balance.
	 * 
	 * FIXME Purge of old hosts/metrics is broken. We are not updating the
	 * timestamp when we receive a metric value (because we rely on the
	 * timestamp not changing when the metric is for this host to let the
	 * service send out the metrics for this host). We will need to track the
	 * timestamp of received updates separately from the timestamp of sent
	 * values. The "sent" timestamp will be zero unless it is a record that we
	 * are sending out for this host, but that does not mean we should delete
	 * the metric since we are still receiving the metric from other hosts (and
	 * potentially from gmond on this host).
	 */
	private class PurgeMetricsTask implements Runnable {

		public void run() {

			try {
				
//				gangliaState.purgeOldHostsAndMetrics(globalDMax);

			} catch (Throwable t) {

				/*
				 * Note: Do NOT throw anything out of this task. It will cause
				 * the task to not be re-executed and metrics reporting will
				 * halt!
				 */

				log.error(t, t);

			}

		}
		
	}
	
	/**
	 * Task is run periodically and is responsible for harvesting metrics to be
	 * reported.
	 */
	private class GatherMetricsTask implements Runnable {

		public void run() {

			try {

				/*
				 * Note: If the user code responsible for gathering metrics
				 * blocks then metrics will no longer be updated.
				 * 
				 * TODO One way to make this more robust is to have a scheduled
				 * task for each set of metrics to be collected. That way, if
				 * one metric collections task dies the others will still run.
				 * This could be done with a scheduled thread pool having more
				 * than one thread. Also, the metrics to be updated can just be
				 * dumped onto a queue. We can then run one thread to drain that
				 * queue, formatting the XDR messages and sending them out.
				 */

				gatherMetrics();
				
				/*
				 * You can uncomment this to log out periodic host reports.
				 */
//				final IHostReport[] hostReports = getHostReport();
//				
//				for (IHostReport r : hostReports) {
//					log.info(r.getHostName() + " "
//							+ r.getMetrics().get("load_one"));
//				}

			} catch (Throwable t) {

				/*
				 * Note: Do NOT throw anything out of this task. It will cause
				 * the task to not be re-executed and metrics reporting will
				 * halt!
				 */

				log.warn(t, t);

			}

		}

	}

	/**
	 * Runs each {@link IGangliaMetricsCollector} in turn.
	 */
	protected void gatherMetrics() {

		final Iterator<IGangliaMetricsCollector> itr = metricCollectors
				.iterator();

		while (itr.hasNext()) {

			final IGangliaMetricsCollector c = itr.next();

			try {

				c.collect(this);
				
			} catch (Throwable t) {
				
				log.warn(t, t);
				
			}

		}
		
	}

	/*
	 * Application facing metrics API.
	 */

	private final CopyOnWriteArraySet<IGangliaMetricsCollector> metricCollectors = new CopyOnWriteArraySet<IGangliaMetricsCollector>();
	
	/**
	 * Register a metrics collector.
	 * 
	 * @param c
	 *            The collector.
	 *            
	 * @return <code>true</code> iff it was not already registered.
	 */
	public boolean addMetricCollector(final IGangliaMetricsCollector c) {
		
		if(c == null)
			throw new IllegalArgumentException();
		
		return metricCollectors.add(c);
		
	}
	
	/**
	 * Remove a metrics collector.
	 * 
	 * @param c
	 *            The collector.
	 *            
	 * @return <code>true</code> iff it is no longer registered.
	 */
	public boolean removeMetricCollector(final IGangliaMetricsCollector c) {
		
		if(c == null)
			throw new IllegalArgumentException();
		
		return metricCollectors.remove(c);
		
	}
	
	/**
	 * {@inheritDoc}
	 * 
	 * This routine is typically invoked by {@link IGangliaMetricsCollector}s.
	 * However, you can also simply invoke it directly.
	 * <p>
	 * Note: In order to get a nice metadata declaration for the record, the
	 * application should also register an {@link IGangliaMetadataFactory}.
	 * 
	 * @param metricName
	 *            The name of the metric.
	 * @param value
	 *            The metric value.
	 * 
	 * @see #getMetadataFactory()
	 */
	@Override
	public void setMetric(final String metricName, final Object value) {

		final GangliaSender sender = this.gangliaSender;

		if (sender != null) {

			try {

				if (metricName == null) {
					log.warn("Metric was emitted with no name.");
					return;
				}

				if (value == null) {
					log.warn("Metric was emitted with a null value: metricName="
							+ metricName);
					return;
				}

				IGangliaMetadataMessage decl;
				{

					// Look for a pre-declared metric.
					decl = gangliaState.getMetadata(metricName);
					
					if (decl == null) {

						// Obtain declaration.
						decl = gangliaState.getMetadataFactory().newDecl(
								hostName, metricName, value);

						if(decl == null) {
						
							log.error("Could not declare: " + metricName);
							return;
							
						}
						
						// Atomically declare/resolve.
						decl = gangliaState.putIfAbsent(decl);

					}

				}
				
				/*
				 * Lookup the current value for the metric.
				 * 
				 * Note: At this point we are guaranteed that a metadata
				 * declaration exists so the return value must be non-null.
				 */
				final TimestampMetricValue tmv = gangliaState.getMetric(
						getHostName(), decl.getMetricName());

				// Return must be non-null since declaration exists.
				assert tmv != null;

				// Should be the same declaration reference.
				assert decl == tmv.getMetadata();

				if (tmv.getTimestamp() == 0L) {

					/*
					 * Send metadata record.
					 * 
					 * TODO Since the captured metadata record might have
					 * originated on another host, we should build a new
					 * metadata record either now (so we send out the record
					 * with our host name) or when we capture the record (as
					 * part of obtaining a richer metadata record object).
					 */

					sendMessage(tmv.getMetadata());

				}

				// Update the current metric value.
				if (tmv.setValue(value)) {

					/*
					 * Send the metric record.
					 * 
					 * Note: Either the metric value has never been transmitted,
					 * or it has changed significantly, or TMax might expire if
					 * we do not retransmit it now.
					 */

					// update the timestamp when sending out the metric value.
					tmv.update();

					final IGangliaMetricMessage msg = metricFactory
							.newMetricMessage(getHostName(), decl,
									false/* spoof */, value);

					sendMessage(msg);

				}

			} catch (Throwable e) {

				log.warn(e, e);

			}

		}

	}
	
	/**
	 * The name of the host on which this {@link GangliaService} is running as
	 * it will be reported through {@link IGangliaMessage}s to other ganglia
	 * services.
	 */
	public String getHostName() {
		
		return hostName;
		
	}
	
    /**
     * The soft-state as maintained using the ganglia protocol and including any
     * metrics collected by this host regardless of whether they are being
     * reported to other hosts.
     */
    public IGangliaState getGangliaState() {
    
	    return gangliaState;
	    
	}
	
	/**
	 * Return the factory for metric declarations. If you supply a
	 * {@link GangliaMetadataFactory} instance to the constructor (which is the
	 * default behavior for the reduced constructor) then you can extend the
	 * default behavior in order to get nice metadata declarations for your
	 * application metrics.
	 */
	public IGangliaMetadataFactory getMetadataFactory() {
		
		return gangliaState.getMetadataFactory();
		
	}

	/**
	 * Return a host report based on the current state (similar to
	 * <code>gstat -a</code>).
	 * <p>
	 * Note: The report will not be accurate immediately as the
	 * {@link GangliaService} needs to build up a model of the current state of
	 * the monitored hosts.
	 */
	public IHostReport[] getHostReport() {

		return getHostReport(defaultHostReportOn, defaultHostReportComparator);
		
	}

	/**
	 * Return a host report based on the current state (similar to
	 * <code>gstat -a</code>).
	 * <p>
	 * Note: The report will not be accurate immediately as the
	 * {@link GangliaService} needs to build up a model of the current state of
	 * the monitored hosts.
	 * 
	 * @param reportOn
	 *            The metrics to be reported for each host. The
	 *            {@link IHostReport#getMetrics()} is an ordered map and will
	 *            reflect the metrics in the order in which they are requested
	 *            here.
	 * @param comparator
	 *            The comparator used to order the {@link IHostReport}s.
	 * 
	 * @return The {@link IHostReport}s for each known host ordered by the given
	 *         {@link Comparator}.
	 */
	public IHostReport[] getHostReport(final String[] reportOn,
			final Comparator<IHostReport> comparator) {
	    
        final String[] hosts = gangliaState.getKnownHosts();

        return getHostReport(hosts, reportOn, comparator);
        
	}
	
    /**
     * Return a host report based on the current state (similar to
     * <code>gstat -a</code>).
     * <p>
     * Note: The report will not be accurate immediately as the
     * {@link GangliaService} needs to build up a model of the current state of
     * the monitored hosts.
     * 
     * @param hosts
     *            The hosts for which host reports will be returned.
     * @param reportOn
     *            The metrics to be reported for each host. The
     *            {@link IHostReport#getMetrics()} is an ordered map and will
     *            reflect the metrics in the order in which they are requested
     *            here.
     * @param comparator
     *            The comparator used to order the {@link IHostReport}s
     *            (optional).
     * 
     * @return The {@link IHostReport}s for each specified host ordered by the
     *         given {@link Comparator}.
     */
    public IHostReport[] getHostReport(final String[] hosts,
            final String[] reportOn, final Comparator<IHostReport> comparator) {

		if (reportOn == null || reportOn.length == 0)
			throw new IllegalArgumentException();

//		if (comparator == null)
//			throw new IllegalArgumentException();
		
		final IHostReport[] a = new IHostReport[hosts.length];

		for (int i = 0; i < a.length; i++) {

			final String hostName = hosts[i];

			// Note: This map preserves the insert order of the metrics.
			final Map<String, IGangliaMetricMessage> m = new LinkedHashMap<String, IGangliaMetricMessage>();

			for (String metricName : reportOn) {

				final TimestampMetricValue tmv = gangliaState.getMetric(
						hostName, metricName);

				if (tmv == null) {
					// No score for that metric for that host.
					continue;

				}

				final Object value = tmv.getValue();

				if (value == null) {
					// Should never happen.
					continue;
				}

				final IGangliaMetricMessage metricValue = metricFactory
						.newMetricMessage(hostName, tmv.getMetadata(),
								false/* spoof */, value);

				if (log.isDebugEnabled())
					log.debug("host=" + hostName + ", metric=" + metricName
							+ ", value=" + value + ", record=" + metricValue);

				// Mock up a metric record.
				m.put(metricName/* metricName */, metricValue);

			}

			a[i] = new HostReport(hostName, m);
			
		}

		// Sort
        if (comparator != null) {
         
            Arrays.sort(a, comparator);
            
        }

		return a;

	}

    /**
     * The name of an environment variable whose value will be used as the
     * canoncial host name for the host running this JVM. This information is
     * used by the {@link GangliaService}, which is responsible for obtaining
     * and reporting the canonical hostname for host metrics reporting.
     * 
     * @see <a href="http://trac.blazegraph.com/ticket/886" >Provide workaround for
     *      bad reverse DNS setups</a>
     */
    public static final String HOSTNAME = "com.bigdata.hostname";

    /**
     * The name for this host.
     * 
     * @see #HOSTNAME
     * @see <a href="http://trac.blazegraph.com/ticket/886" >Provide workaround for
     *      bad reverse DNS setups</a>
     */
    public static final String getCanonicalHostName() {
        String s = System.getProperty(HOSTNAME);
        if (s != null) {
            // Trim whitespace.
            s = s.trim();
        }
        if (s != null && s.length() != 0) {
            log.warn("Hostname override: hostname=" + s);
        } else {
            try {
                /*
                 * Note: This should be the host *name* NOT an IP address of a
                 * preferred Ethernet adaptor.
                 */
                s = InetAddress.getLocalHost().getCanonicalHostName();
            } catch (Throwable t) {
                log.warn("Could not resolve canonical name for host: " + t);
            }
            try {
                s = InetAddress.getLocalHost().getHostName();
            } catch (Throwable t) {
                log.warn("Could not resolve name for host: " + t);
                s = "localhost";
            }
        }
        return s;
	}

	/**
	 * Runs a {@link GangliaService} as a standalone application.
	 * <p>
	 * Note: This routine is mainly for test as the primary purpose of the
	 * {@link GangliaService} is to embed it within another application.
	 * 
	 * @param args
	 *            ignored.
	 * 
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		/*
		 * The host name for this host.
		 */
		final String hostName = getCanonicalHostName();

		final String serviceName = GangliaService.class.getSimpleName();

		final int quietPeriod = IGangliaDefaults.QUIET_PERIOD;

		final int initialDelay = IGangliaDefaults.INITIAL_DELAY;

		/*
		 * Note: Use ZERO (0) if you are running gmond on the same host. That
		 * will prevent the GangliaService from transmitting a different
		 * heartbeat, which would confuse gmond and gmetad.
		 */
		final int heartbeatInterval = 0; // IFF using gmond.
//		final int heartbeatInterval = IGangliaDefaults.HEARTBEAT_INTERVAL;
		
		final int monitoringInterval = IGangliaDefaults.MONITORING_INTERVAL;
		
		final InetAddress listenGroup = InetAddress
				.getByName(IGangliaDefaults.DEFAULT_GROUP);
		
		final int listenPort = IGangliaDefaults.DEFAULT_PORT;

		final String defaultUnits = IGangliaDefaults.DEFAULT_UNITS;
		
		final GangliaSlopeEnum defaultSlope = IGangliaDefaults.DEFAULT_SLOPE;

		final int defaultTMax = IGangliaDefaults.DEFAULT_TMAX;

		final int defaultDMax = IGangliaDefaults.DEFAULT_DMAX;
		
		final InetSocketAddress[] metricsServers = new InetSocketAddress[] { new InetSocketAddress(//
				IGangliaDefaults.DEFAULT_GROUP,//
				IGangliaDefaults.DEFAULT_PORT//
		) };

		/*
		 * Extensible factory for declaring and resolving metrics.
		 * 
		 * Note: you can layer on the ability to (a) recognize and align your
		 * own host performance counters hierarchy with those declared by
		 * ganglia and; (b) provide nice declarations for various application
		 * counters of interest.
		 */
		final GangliaMetadataFactory metadataFactory = new GangliaMetadataFactory(
				new DefaultMetadataFactory(//
						defaultUnits,//
						defaultSlope,//
						defaultTMax,//
						defaultDMax//
						));

		// The embedded ganglia service.
		GangliaService service = null;

		try {

			service = new GangliaService(//
					hostName,//
					serviceName, //
					metricsServers, //
					listenGroup, listenPort,//
					true,// listen
					true,// report
					false,// mock (does not transmit when true).
					quietPeriod,//
					initialDelay,//
					heartbeatInterval,//
					monitoringInterval, //
					defaultDMax,//
					metadataFactory//
			);
			
			/*
			 * Run the ganglia service.
			 */
			service.run();

			/*
			 * Start host/application metric collection here.
			 */
			
		} finally {

			/*
			 * Stop host/application metric collection here.
			 */

		}

	} // main()

}
