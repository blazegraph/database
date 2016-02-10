/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.sail.webapp.lbs.policy.counters;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.apache.log4j.Logger;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpMethod;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.DefaultInstrumentFactory;
import com.bigdata.counters.ICounterNode;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.journal.PlatformStatsPlugIn;
import com.bigdata.rdf.sail.webapp.CountersServlet;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.EntityContentProvider;
import com.bigdata.rdf.sail.webapp.client.IMimeTypes;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.lbs.AbstractHostLBSPolicy;
import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;
import com.bigdata.rdf.sail.webapp.lbs.IHostMetrics;
import com.bigdata.rdf.sail.webapp.lbs.IHostScoringRule;
import com.bigdata.rdf.sail.webapp.lbs.ServiceScore;

/**
 * Stochastically proxy the request to the services based on their load.
 * <p>
 * Note: This {@link IHALoadBalancerPolicy} has a dependency on the
 * {@link PlatformStatsPlugIn}. The plugin must be setup to publish out
 * performance counters using the {@link CounterServlet}. This policy will
 * periodically query the different {@link HAJournalServer} instances to
 * obtain their current metrics using that {@link CountersServlet}.
 * <p>
 * This is not as efficient as using ganglia. However, this plugin creates
 * fewer dependencies and is significantly easier to administer if the
 * network does not support UDP multicast.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CountersLBSPolicy extends AbstractHostLBSPolicy {

    private static final Logger log = Logger.getLogger(CountersLBSPolicy.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Servlet <code>init-param</code> values understood by the
     * {@link CountersLBSPolicy}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface InitParams extends AbstractHostLBSPolicy.InitParams {

    }

    /**
     * The most recent host metrics for each host running a service of interest.
     * 
     * TODO Does not track per-service metrics unless we change to the service
     * {@link UUID} as the key. This means that we can not monitor the GC load
     * associated with a specific JVM instance. [Another problem is that the
     * metrics that we are collecting have the hostname as a prefix. The service
     * metrics do not. This will cause problems in the path prefix in the
     * {@link CounterSet} when we try to resolve the performance counter name.
     * We could work around that by using a regex pattern to match the counters
     * of interest, ignoring where they appear in the {@link CounterSet}
     * hierarchy.]
     */
    private final ConcurrentHashMap<String/* hostname */, IHostMetrics> hostMetricsMap = new ConcurrentHashMap<String, IHostMetrics>();

    @Override
    protected void toString(final StringBuilder sb) {

        super.toString(sb);

    }

    @Override
    public void init(final ServletConfig servletConfig,
            final IIndexManager indexManager) throws ServletException {

        super.init(servletConfig, indexManager);

    }

    @Override
    public void destroy() {

        super.destroy();

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation issues HTTP requests to obtain the up to date
     * performance counters for each host on which a service is known to be
     * running.
     */
    @Override
    protected Map<String, IHostMetrics> getHostReportForKnownServices(
            final IHostScoringRule scoringRule,
            final ServiceScore[] serviceScores) {
        
        /*
         * The set of hosts having services that are joined with the met quorum.
         */
        final String[] hosts;
        {
            final List<String> tmp = new LinkedList<String>();
            for (ServiceScore serviceScore : serviceScores) {
                if (serviceScore == null) // should never be null.
                    continue;
                final String hostname = serviceScore.getHostname();
                if (hostname == null) // should never be null.
                    continue;
                tmp.add(hostname);
            }
            // dense array of hosts names for services.
            hosts = tmp.toArray(new String[tmp.size()]);
        }

        final HttpClient cm = getClientConnectionManager();

        for (String hostname : hosts) {

            final String baseRequestURI = getServiceScoreForHostname(hostname)
                    .getRequestURI();
            
            // HTTP GET => Counters XML
            final CounterSet counterSet;
            try {
                counterSet = doCountersQuery(cm, hostname, baseRequestURI,
                        nextValue.incrementAndGet());
            } catch (Exception ex) {
                log.error(ex, ex);
                continue;
            }

            if (counterSet.isRoot() && counterSet.isLeaf()) {
                log.warn("No data: hostname=" + hostname);
                continue;
            }

            // Look for the child named by the hostname.
            final ICounterNode childNode = counterSet.getPath(hostname);
            if (childNode == null) {
                log.warn("No data: hostname=" + hostname);
                continue;
            }

            /*
             * Add to the map.
             * 
             * Note: We are adding the childNode. This is the CounterSet for the
             * specific host. This means that the IHostScoringRules do not need
             * to be aware of the hostname on which they are running. (The other
             * approach would be to pass in the hostname as a prefix to the
             * wrapper class that we are placing into the hostMetricsMap.)
             */
            hostMetricsMap.put(hostname, new CounterSetHostMetricsWrapper(
                    (CounterSet) childNode));

        }

        return hostMetricsMap;

    }

    private HttpClient getClientConnectionManager() {

        final Journal journal = (Journal) getJournal();

        QueryEngine queryEngine = QueryEngineFactory.getInstance()
                .getExistingQueryController(journal);

        if (queryEngine == null) {

            /*
             * No queries have been run. We do not have access to the HTTPClient
             * yet.
             * 
             * TODO This could cause a race condition with the shutdown of the
             * journal. Perhaps use synchronized(journal) {} here?
             */
            queryEngine = QueryEngineFactory.getInstance().getQueryController(journal);

        }

        final HttpClient cm = queryEngine
                .getClientConnectionManager();

        return cm;

    }
    
    /**
     * Do an HTTP GET to the remote service and return the platform performance
     * metrics for that service.
     * 
     * @param cm
     * @param hostname
     * @param baseRequestURI
     * @param uniqueId
     * @return
     * @throws Exception
     */
    private static CounterSet doCountersQuery(final HttpClient cm,
            final String hostname, final String baseRequestURI,
            final int uniqueId) throws Exception {

        final String uriStr = baseRequestURI + "/counters";

        final ConnectOptions o = new ConnectOptions(uriStr);

        o.setAcceptHeader(ConnectOptions.MIME_APPLICATION_XML);

        o.method = "GET";
                
        // OS counters are under the hostname.
        o.addRequestParam("path", "/" + hostname + "/");

        // Note: Necessary to each counters. E.g., /hostname/CPU/XXXX.
        o.addRequestParam("depth", "3");

        // Used to defeat the httpd cache on /counters.
        o.addRequestParam("uniqueId", Integer.toString(uniqueId));

        boolean didDrainEntity = false;
        JettyResponseListener response = null;
        try {

            response = doConnect(cm, o);

            RemoteRepository.checkResponseCode(response);

            // Check the mime type for something we can handle.
            final String contentType = response.getContentType();

            if (!contentType.startsWith(IMimeTypes.MIME_APPLICATION_XML)) {

                throw new IOException("Expecting "
                        + IMimeTypes.MIME_APPLICATION_XML
                        + ", not Content-Type=" + contentType);
 
            }

            final CounterSet counterSet = new CounterSet();

            final InputStream is = response.getInputStream();

            try {

                /*
                 * Note: This will throw a runtime exception if the source
                 * contains more than 60 minutes worth of history data.
                 */
                counterSet
                        .readXML(is, DefaultInstrumentFactory.NO_OVERWRITE_60M,
                                null/* filter */);

                didDrainEntity = true;

                if (log.isDebugEnabled())
                    log.debug("hostname=" + hostname + ": counters="
                            + counterSet);

                return counterSet;
                
            } finally {

                try {
                    is.close();
                } catch (IOException ex) {
                    log.warn(ex);
                }

            }

        } finally {
            
			if (response != null && !didDrainEntity) {
				response.abort();
            }
            
        }
        
    }
    
    /**
     * Connect to an HTTP end point.
     * 
     * @param opts
     *            The connection options.
     * 
     * @return The connection.
     */
    static private JettyResponseListener doConnect(final HttpClient httpClient,
            final ConnectOptions opts) throws IOException {

        /*
         * Generate the fully formed and encoded URL.
         */
        // The requestURL (w/o URL query parameters).
        final String requestURL = opts.serviceURL;
        
        final StringBuilder urlString = new StringBuilder(requestURL);

        ConnectOptions.addQueryParams(urlString, opts.requestParams);

        if (log.isDebugEnabled()) {
            log.debug("*** Request ***");
            log.debug(requestURL);
            log.debug(opts.method);
            log.debug(urlString.toString());
        }

        Request request = null;
        try {

//            request = RemoteRepository.newRequest(httpClient, urlString.toString(),
//                    opts.method);
            request = httpClient.newRequest(urlString.toString()).method(
                  HttpMethod.GET);

            if (opts.requestHeaders != null) {

                for (Map.Entry<String, String> e : opts.requestHeaders
                        .entrySet()) {

                    request.header(e.getKey(), e.getValue());

                    if (log.isDebugEnabled())
                        log.debug(e.getKey() + ": " + e.getValue());

                }

            }
            
            if (opts.entity != null) {

            	final EntityContentProvider cp = new EntityContentProvider(opts.entity);
                request.content(cp, cp.getContentType());

            }

			final JettyResponseListener listener = new JettyResponseListener(
					request, TimeUnit.SECONDS.toMillis(300));

            request.send(listener);
            
            return listener;

        } catch (Throwable t) {
            /*
             * If something goes wrong, then close the http connection.
             * Otherwise, the connection will be closed by the caller.
             */
            try {
                
                if (request != null)
                    request.abort(t);
                
                
            } catch (Throwable t2) {
                log.warn(t2); // ignored.
            }
            throw new RuntimeException(requestURL + " : " + t, t);
        }

    }
    
    @Override
    protected String getDefaultScoringRule() {

        return DefaultHostScoringRule.class.getName();

    }

    /**
     * This is used to defeat the httpd cache for the <code>counters</code>
     * servlet.
     */
    private final AtomicInteger nextValue = new AtomicInteger();
    
}
