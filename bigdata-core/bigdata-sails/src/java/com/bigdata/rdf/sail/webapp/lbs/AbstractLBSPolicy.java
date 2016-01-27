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
package com.bigdata.rdf.sail.webapp.lbs;

import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.IHAJournal;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.IIndexManager;
import com.bigdata.quorum.AbstractQuorum;
import com.bigdata.quorum.Quorum;
import com.bigdata.quorum.QuorumEvent;
import com.bigdata.quorum.QuorumListener;
import com.bigdata.rdf.sail.webapp.BigdataServlet;
import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.concurrent.FutureTaskMon;

/**
 * Abstract base class establishes a listener for quorum events, tracks the
 * services that are members of the quorum, and caches metadata about those
 * services (especially the requestURL at which they will respond).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
abstract public class AbstractLBSPolicy implements IHALoadBalancerPolicy,
        QuorumListener, Serializable {

    private static final Logger log = Logger.getLogger(AbstractLBSPolicy.class);

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface InitParams {

    }

    /**
     * The {@link ServletContext#getContextPath()} is cached in
     * {@link #init(ServletConfig, IIndexManager)}.
     */
    private final AtomicReference<String> contextPath = new AtomicReference<String>();

    /**
     * A {@link WeakReference} to the {@link HAJournal} avoids pinning the
     * {@link HAJournal}.
     */
    protected final AtomicReference<WeakReference<IHAJournal>> journalRef = new AtomicReference<WeakReference<IHAJournal>>();

    /**
     * The {@link UUID} of the HAJournalServer.
     */
    protected final AtomicReference<UUID> serviceIDRef = new AtomicReference<UUID>();

    /**
     * This is the table of known services. We can scan the table for a service
     * {@link UUID} and then forward a request to the pre-computed requestURL
     * associated with that {@link UUID}. If the requestURL is <code>null</code>
     * then we do not know how to reach that service and can not proxy the
     * request.
     */
    protected final AtomicReference<ServiceScore[]> serviceTableRef = new AtomicReference<ServiceScore[]>(
            null);

    /**
     * Note: implementation is non-blocking!
     */
    @Override
    public String toString() {

        final ServiceScore[] serviceTable = serviceTableRef.get();

        final String tmp;
        if (serviceTable != null) {
            tmp = Arrays.toString(serviceTable);
        } else {
            tmp = "N/A";
        }

        final StringBuilder sb = new StringBuilder(256);

        sb.append(this.getClass().getName());
        sb.append("{contextPath=" + contextPath.get());
        sb.append(",journal=" + journalRef.get());
        sb.append(",serviceID=" + serviceIDRef.get());
        sb.append(",services=" + tmp);
        toString(sb); // extension hook
        sb.append("}");

        return sb.toString();

    }

    /**
     * Extension hook for {@link #toString()} - implementation MUST NOT block.
     * 
     * @param sb
     *            Buffer where you can write additional state.
     */
    protected void toString(final StringBuilder sb) {
        
    }
    
    /**
     * Return the cached reference to the {@link HAJournal}.
     * 
     * @return The reference or <code>null</code> iff the reference has been
     *         cleared or has not yet been set.
     */
    protected IHAJournal getJournal() {

        final WeakReference<IHAJournal> ref = journalRef.get();

        if (ref == null)
            return null;

        return ref.get(); // Note: MAY be null (iff weak reference is cleared).

    }

    @Override
    public void destroy() {

        contextPath.set(null);

        journalRef.set(null);

        serviceTableRef.set(null);

    }

    @Override
    public void init(final ServletConfig servletConfig,
            final IIndexManager indexManager) throws ServletException {

        final ServletContext servletContext = servletConfig.getServletContext();

        contextPath.set(servletContext.getContextPath());

        final IHAJournal journal = (IHAJournal) BigdataServlet
                .getIndexManager(servletContext);

        if (journal == null)
            throw new ServletException("No journal?");

        serviceIDRef.compareAndSet(null/* expect */,
                journal.getServiceID()/* update */);

        this.journalRef.set(new WeakReference<IHAJournal>(journal));

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();

        quorum.addListener(this);

    }

    @Override
    public boolean service(//
            final boolean isLeaderRequest,
            final HALoadBalancerServlet servlet,//
            final HttpServletRequest request, //
            final HttpServletResponse response//
    ) throws ServletException, IOException {

        /*
         * Figure out whether the quorum is met and if this is the quorum
         * leader.
         */
        final IHAJournal journal = getJournal();
        Quorum<HAGlue, QuorumService<HAGlue>> quorum = null;
        QuorumService<HAGlue> quorumService = null;
        long token = Quorum.NO_QUORUM; // assume no quorum.
        boolean isLeader = false; // assume false.
        boolean isQuorumMet = false; // assume false.
        if (journal != null) {
            quorum = journal.getQuorum();
            if (quorum != null) {
                try {
                    // Note: This is the *local* HAGlueService.
                    quorumService = (QuorumService<HAGlue>) quorum.getClient();
                    token = quorum.token();
                    isLeader = quorumService.isLeader(token);
                    isQuorumMet = token != Quorum.NO_QUORUM;
                } catch (IllegalStateException ex) {
                    // Note: Not available (quorum.start() not
                    // called).
                }
            }
        }

        if ((isLeader && isLeaderRequest) || !isQuorumMet) {

            /*
             * (1) If this service is the leader and the request is an UPDATE,
             * then we forward the request to the local service. It will handle
             * the UPDATE request.
             * 
             * (2) If the quorum is not met, then we forward the request to the
             * local service. It will produce the appropriate error message.
             * 
             * @see #forwardToThisService()
             */
            servlet.forwardToLocalService(isLeaderRequest, request, response);

            // request was handled.
            return true;

        }

        /*
         * Hook the request to update the service/host tables if they are not
         * yet defined.
         */
        conditionallyUpdateServiceTable();

        if (!isLeaderRequest) {
            /*
             * Provide an opportunity to forward a read request to the local
             * service.
             */
            if (conditionallyForwardReadRequest(servlet, request, response)) {

                // Handled.
                return true;
            }

        }

        // request was not handled.
        return false;

    }

    /**
     * Hook provides the {@link IHALoadBalancerPolicy} with an opportunity to
     * forward a read-request to the local service rather than proxying the
     * request to a service selected by the load balancer (a local forward has
     * less overhead than proxying to either the local host or a remote service,
     * which makes it more efficient under some circumstances to handle the
     * read-request on the service where it was originally received).
     * 
     * @throws IOException
     */
    protected boolean conditionallyForwardReadRequest(
            final HALoadBalancerServlet servlet,
            final HttpServletRequest request, //
            final HttpServletResponse response//
            ) throws IOException {

        return false;

    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation rewrites the requestURL such that the request will be
     * proxied to the quorum leader.
     */
    @Override
    final public String getLeaderURI(final HttpServletRequest request) {

        final ServletContext servletContext = request.getServletContext();

        final IHAJournal journal = (IHAJournal) BigdataServlet
                .getIndexManager(servletContext);

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();

        final UUID leaderId = quorum.getLeaderId();

        if (leaderId == null) {
            // No quorum, so no leader. Can not proxy the request.
            return null;
        }

        /*
         * Scan the services table to locate the leader and then proxy the
         * request to the pre-computed requestURL for the leader. If that
         * requestURL is null then we do not know about a leader and can not
         * proxy the request at this time.
         */

        final ServiceScore[] services = serviceTableRef.get();

        if (services == null) {

            // No services. Can't proxy.
            return null;

        }

        for (ServiceScore s : services) {

            if (s.getServiceUUID().equals(leaderId)) {

                // Found it. Proxy if the serviceURL is defined.
                return s.getRequestURI();

            }

        }

        // Not found. Won't proxy.
        return null;

    }

    /**
     * Return the {@link ServiceScore} for the {@link HAGlue} service running on
     * this host within this webapp.
     */
    protected ServiceScore getLocalServiceScore() {

        final ServiceScore[] services = serviceTableRef.get();

        if (services == null) {

            // No services. Can't proxy.
            return null;

        }

        for (ServiceScore s : services) {

            if (s.getServiceUUID().equals(serviceIDRef.get())) {

                // Found it.
                return s;

            }

        }

        // Not found.
        return null;

    }

    /**
     * Return the first service found for the indicated host.
     * 
     * @param hostname
     *            The hostname.
     * 
     * @return The first service for that host.
     */
    protected ServiceScore getServiceScoreForHostname(final String hostname) {

        if (hostname == null)
            throw new IllegalArgumentException();

        final ServiceScore[] services = serviceTableRef.get();

        if (services == null) {

            // No services. Can't proxy.
            return null;

        }

        for (ServiceScore s : services) {

            if (hostname.equals(s.getHostname())) {

                // Found it.
                return s;

            }

        }

        // Not found.
        return null;

    }

    /**
     * {@inheritDoc}
     * <p>
     * The services table is updated if a services joins or leaves the quorum.
     * 
     * FIXME The {@link QuorumListener} is unregistered by
     * {@link AbstractQuorum#terminate()}. This happens any time the
     *  HAJournalServer goes into the error state. When this occurs, we
     * stop getting {@link QuorumEvent}s and the policy stops being responsive
     * (it can not proxy the request to a service that is still up because it
     * does not know what services are up, or maybe it just can not learn if
     * services go down).
     * <p>
     * We probably need to either NOT clear the quorum listener and/or add an
     * event type that is sent when {@link Quorum#terminate()} is called and/or
     * use our own listener (independent of the HAJournalServer, which would
     * require us to use an HAClient).
     * <p>
     * This should be robust even when the HAQuorumService is not running. We do
     * not want to be unable to proxy to another service just because this one
     * is going through an error state. Would it make more sense to have a 2nd
     * Quorum object for this purpose - one that is not started and stopped by
     * the HAJournalServer?
     * 
     * @see http://trac.blazegraph.com/ticket/775 (HAJournal start() delay)
     */
    @Override
    public void notify(final QuorumEvent e) {
        switch (e.getEventType()) {
        case SERVICE_JOIN:
        case SERVICE_LEAVE:
            /*
             * Note: We do not want to run any blocking code in the ZK event
             * thread!
             */
            getJournal().getExecutorService().execute(
                    new FutureTaskMon<Void>(new Runnable() {
                        @Override
                        public void run() {
                            updateServiceTable();
                        }
                    }, (Void) null/* result */));
            break;
        }
   }

    /**
     * Conditionally update the {@link #serviceTableRef} iff it does not exist or
     * is empty.
     */
    protected void conditionallyUpdateServiceTable() {

        final ServiceScore[] services = serviceTableRef.get();

        if (services == null || services.length == 0) {

            /*
             * Ensure that the service table exists (more correctly, attempt to
             * populate it, but we can only do that if the HAQuorumService is
             * running.)
             * 
             * Note: Synchronization here is used to ensure only one thread runs
             * this logic if the table does not exist and we get a barrage of
             * requests.
             * 
             * TODO This does not sufficiently prevent against duplicate work
             * for non-conditional service table update paths.
             */
            synchronized (serviceTableRef) {

                updateServiceTable();

            }

        }

    }

    /**
     * Update the per-service table.
     * 
     * @see #serviceTableRef
     */
    protected void updateServiceTable() {

        final IHAJournal journal = getJournal();

        if (journal == null) {
            // Can't do anything if there is no journal.
            return;
        }

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();

        final UUID[] joined = quorum.getJoined();

        /*
         * If there is an existing service table, then we search it for an
         * existing definition for a service. This let's avoid doing an RMI to a
         * Service that is already defined in the current service table.
         * 
         * Note: We need to hold all discovered services in a map if we want to
         * keep the statistics [nrequests] across leave/joins. Right now, a
         * service leave followed by a join will cause a new ServiceScore and
         * that will reset the counters to zero.
         */
        final ServiceScore[] oldTable = serviceTableRef.get();
        
        final ServiceScore[] serviceScores = new ServiceScore[joined.length];

        for (int i = 0; i < joined.length; i++) {

            final UUID serviceId = joined[i];

            if (oldTable != null) {

                // Check old service table before doing RMI.
                for (int j = 0; j < oldTable.length; j++) {

                    final ServiceScore oldScore = oldTable[j];

                    if (oldScore != null
                            && serviceId.equals(oldScore.getServiceUUID())) {

                        // Found existing declaration for this service.
                        serviceScores[i] = oldTable[j];

                        break;

                    }

                }

            }

            try {

                // Do RMI to create declaration for this service.
                serviceScores[i] = ServiceScore.newInstance(journal,
                        contextPath.get(), serviceId);

            } catch (Exception ex) {

                // Service is not usable.
                log.warn(ex, ex);

                continue;

            }

        }

        if (log.isInfoEnabled())
            log.info("Updated servicesTable: #services=" + serviceScores.length);

        this.serviceTableRef.set(serviceScores);

    }

}
