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
package com.bigdata.rdf.sail.webapp.lbs.policy.ganglia;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;

import org.apache.log4j.Logger;

import com.bigdata.ganglia.GangliaListener;
import com.bigdata.ganglia.GangliaService;
import com.bigdata.ganglia.IHostReport;
import com.bigdata.journal.GangliaPlugIn;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.sail.webapp.lbs.AbstractHostLBSPolicy;
import com.bigdata.rdf.sail.webapp.lbs.IHALoadBalancerPolicy;
import com.bigdata.rdf.sail.webapp.lbs.IHostMetrics;
import com.bigdata.rdf.sail.webapp.lbs.IHostScoringRule;
import com.bigdata.rdf.sail.webapp.lbs.ServiceScore;

/**
 * Stochastically proxy the request to the services based on their load.
 * <p>
 * Note: This {@link IHALoadBalancerPolicy} has a dependency on the
 * {@link GangliaPlugIn}. The {@link GangliaPlugIn} must be setup to listen to
 * the Ganglia protocol and build up an in-memory model of the load on each
 * host. Ganglia MUST be reporting metrics for each host running an
 * {@link HAJournalServer} instance. This can be achieved either using the
 * <code>gmond</code> utility from the ganglia distribution or using the
 * {@link GangliaPlugIn}.
 * <p>
 * Note: The actual performance metrics will be reported by a given ganglia
 * daemon on some host depend on the ganglia implementation, ganglia
 * configuration, and the host OS. Some key performance metrics may not be
 * available on all hosts. For example, IO Wait is not available under OSX. The
 * available performance metrics constraints the {@link IHostScoringRule}s that
 * you can utilize on a given cluster. You can monitor the available performance
 * metrics by running the {@link GangliaListener} utility.
 * <p>
 * Note: The ganglia updates are not synchronized across a cluster. They pour in
 * ever N seconds from each host. However, the hosts do not begin to report on
 * the same N second boundary. All you know is that (on average) all hosts
 * should have reported in within N seconds.
 * 
 * @see GangliaService#getDefaultHostReportOn()
 * @see GangliaListener#main(String[])
 * @see <a
 *      href="https://sourceforge.net/apps/trac/ganglia/wiki/Ganglia%203.1.x%20Installation%20and%20Configuration"
 *      >Ganglia Wiki (has a section on the defined metics and the platform
 *      where they are available)</a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class GangliaLBSPolicy extends AbstractHostLBSPolicy {

    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(GangliaLBSPolicy.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Servlet <code>init-param</code> values understood by the
     * {@link GangliaLBSPolicy}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public interface InitParams extends AbstractHostLBSPolicy.InitParams {

    }

//    /**
//     * Place into descending order by load_one.
//     * <p>
//     * Note: We do not rely on the ordering imposed by this comparator. Instead,
//     * we filter the hosts for those that correspond to the joined services in
//     * the met quorum, compute a score for each such host, and then normalize
//     * those scores.
//     */
//    private final static Comparator<IHostReport> comparator = new HostReportComparator(
//            "load_one", false/* asc */);

    /**
     * The ganglia service - it must be configured at least as a listener.
     */
    private final AtomicReference<GangliaService> gangliaServiceRef = new AtomicReference<GangliaService>();

    @Override
    protected void toString(final StringBuilder sb) {

        super.toString(sb);

        sb.append(",gangliaService=" + gangliaServiceRef.get());

    }

    @Override
    public void init(final ServletConfig servletConfig,
            final IIndexManager indexManager) throws ServletException {

        super.init(servletConfig, indexManager);

        gangliaServiceRef.set((GangliaService) ((Journal) indexManager)
                .getGangliaService());

        if (gangliaServiceRef.get() == null) {
            // LBS requires ganglia to load balance requests.
            throw new ServletException("LBS requires "
                    + GangliaPlugIn.class.getName());
        }

    }

    @Override
    public void destroy() {

        // comparator = null;

        gangliaServiceRef.set(null);

        super.destroy();

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation queries the in-memory model of the cluster that is
     * built up and maintained by the integrated {@link GangliaService}.
     */
    protected Map<String, IHostMetrics> getHostReportForKnownServices(
            final IHostScoringRule scoringRule,
            final ServiceScore[] serviceScores) {

        final GangliaService gangliaService = gangliaServiceRef.get();

        if(gangliaService == null) {
            
            // No ganglia.
            return null;
            
        }
        
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

        final String[] reportOn = scoringRule.getMetricNames();
        
        final IHostReport[] hostReport = gangliaService.getHostReport(//
                hosts,// the hosts for our joined services.
                reportOn,// metrics to be reported.
                null // comparator (unused)
                );

        final Map<String/* hostname */, IHostMetrics> hostMetricsMap = new HashMap<String, IHostMetrics>();

        // Consider each host report from ganglia.
        for (int i = 0; i < hostReport.length; i++) {

            final IHostReport theHostReport = hostReport[i];

            // Consider each service in turn.
            for (int j = 0; j < serviceScores.length; j++) {

                final ServiceScore theServiceScore = serviceScores[j];

                // The hostname as understood by a service.
                final String hostname = theServiceScore.getHostname();

                if(hostname == null)
                    continue;
                
                final int index = hostname.indexOf('.');
                
                // The local name of the host (w/o the domain).
                final String localname = (index == -1) ? null : hostname
                        .substring(0/* beginIndex */, index/* endIndex */);

                if (hostname.equals(theHostReport.getHostName())) {

                    // Store under the canonical host name.
                    hostMetricsMap.put(hostname, new GangliaHostMetricWrapper(theHostReport));

                    break;

                }
                
                /*
                 * Note: This recognizes the local host name as well as the
                 * canonical hostname. ganglia defaults to reporting the local
                 * name of the host. bigdata defaults to using the canonical
                 * name of the host. To make things line up, we check for a
                 * match on the local name if we do not find any match on the
                 * hostname as self-reported by the service.
                 */
                if (localname.equals(theHostReport.getHostName())) {

                    // Note: We ALWAYS use the canonical host name here.
                    hostMetricsMap.put(hostname, new GangliaHostMetricWrapper(theHostReport));

                    break;

                }

            }

        }

        return hostMetricsMap;

    }

    @Override
    protected String getDefaultScoringRule() {
        
        return DefaultHostScoringRule.class.getName();
        
    }

}
