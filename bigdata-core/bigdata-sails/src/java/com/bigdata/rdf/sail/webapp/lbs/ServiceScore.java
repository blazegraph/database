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
import java.util.UUID;

import com.bigdata.counters.CAT;
import com.bigdata.ha.HAGlue;
import com.bigdata.ha.IHAJournal;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.IIndexManager;
import com.bigdata.quorum.Quorum;

/**
 * Helper class caches metadata about an {@link HAGlue} service.
 */
public class ServiceScore {

    /**
     * The service {@link UUID} for the remote service.
     * 
     * @see HAGlue#getServiceUUID()
     */
    final private UUID serviceUUID;
    
    /**
     * The hostname for the remote service.
     * 
     * @see HAGlue#getHostname()
     */
    private String hostname;
    
    /**
     * The constructed <code>Request-URI</code> for the root of the servlet
     * context for the NSS on the remote service -or- <code>null</code> if
     * anything goes wrong.
     */
    private String requestURI;
    
    /**
     * The service {@link UUID} for the remote service.
     * 
     * @see HAGlue#getServiceUUID()
     */
    public UUID getServiceUUID() {

        return serviceUUID;
        
    }

    /**
     * The hostname for the remote service -or- <code>null</code> if something
     * goes wrong.
     * 
     * @see HAGlue#getHostname()
     */
    public String getHostname() {

        return hostname;
        
    }

    /**
     * The <code>Request-URI</code> for the root of the web application on the
     * target host. This is assigned IFF everything succeeds. This is what we
     * will use to proxy a request to the service having the {@link UUID} given
     * to the constructor.
     * <p>
     * Note: This needs to be a URL, not just a relative path. Otherwise you get
     * an NPE.
     * <p>
     * This is formed as:
     * 
     * <pre>
     * requestURI = &quot;http://&quot; + hostname + &quot;:&quot; + port + contextPath;
     * </pre>
     * 
     * The <code>hostname</code> is obtained from {@link HAGlue#getHostname()}.
     * <p>
     * The <code>port</code> is obtained from {@link HAGlue#getNSSPort()}.
     * 
     * TODO How do we configure the protocol for the remote NSS instance? This
     * code assumes that it is <code>http</code>, but <code>https</code> is also
     * possible. This could be handled by an {@link IHARequestURIRewriter} but
     * maybe the {@link HAGlue} interface should be declaring this too?
     */
    public String getRequestURI() {

        return requestURI;
        
    }

    /**
     * The #of requests that have been directed to this service.
     */
    public final CAT nrequests = new CAT();
    
    @Override
    public String toString() {
        
        return getClass().getName() + "{serviceUUID=" + serviceUUID
                + ", hostname=" + hostname + ", requestURI=" + requestURI
                + ", nrequests=" + nrequests.get() + "}";

    }

    /**
     * 
     * @param serviceUUID
     *            The {@link UUID} for the service.
     * @param hostname
     *            The hostname of the host on which the service is running.
     * @param requestURI
     *            The Request-URI for the service.
     *            
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     */
    ServiceScore(final UUID serviceUUID, final String hostname,
            final String requestURI) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        if (hostname == null)
            throw new IllegalArgumentException();

        if (requestURI == null)
            throw new IllegalArgumentException();

        this.serviceUUID = serviceUUID;

        this.hostname = hostname;
        
        this.requestURI = requestURI;
        
    }
    
    /**
     * Factory for {@link ServiceScore} instances.
     * 
     * @param indexManager
     *            The index manager (required).
     * @param contextPath
     *            The Context-Path of the web application (/bigdata).
     * @param serviceUUID
     *            The {@link UUID} of the service.
     * 
     * @return The {@link ServiceScore}.
     * 
     * @throws IllegalArgumentException
     *             if any argument is <code>null</code>.
     * @throws ClassCastException
     *             if the {@link IIndexManager} is not an {@link HAJournal}.
     * @throws IOException
     *             If an RMI to the {@link HAGlue} proxy fails.
     * @throws RuntimeException
     *             if anything else goes wrong.
     */
    static public ServiceScore newInstance(final IIndexManager indexManager,
            final String contextPath, final UUID serviceUUID)
            throws IllegalArgumentException, ClassCastException, IOException,
            RuntimeException {

        if (indexManager == null)
            throw new IllegalArgumentException();

        if (contextPath == null)
            throw new IllegalArgumentException();

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        final IHAJournal journal = (IHAJournal) indexManager;

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();

        if (quorum == null) {
            // No quorum.
            return null;
        }

        /*
         * Note: This is the *local* HAGlueService.
         * 
         * This page must be robust to some new failure modes. The ZooKeeper
         * client can now be associated with an expired session, River discovery
         * can now be disabled, and the HAQuorumService might not be available
         * from quorum.getClient(). All of those things can happen if there is a
         * zookeeper session expiration that forces us to terminate the
         * HAQuorumService. This condition will be cured automatically (unless
         * the service is being shutdown), but only limited status information
         * can be provided while the HAQuorumService is not running.
         */
        final QuorumService<HAGlue> quorumService;
        {
            QuorumService<HAGlue> t;
            try {
                t = (QuorumService<HAGlue>) quorum.getClient();
            } catch (IllegalStateException ex) {
                // Note: Not available (quorum.start() not called).
                throw ex;
            }
            quorumService = t;
        }

        final HAGlue haGlue;
        try {
            haGlue = quorumService.getService(serviceUUID);
        } catch (IllegalArgumentException ex) {
            // No such service.
            throw ex;
        }

        /*
         * TODO The hostname and port are RMIs. Use a smart proxy for HAGlue. Or
         * consult a cache of existing ServiceScore objects. [The caller DOES
         * consult a cache so this is partly addressed.]
         */
        final String hostname;
        final int port;
        try {
            hostname = haGlue.getHostname();
            port = haGlue.getNSSPort();
        } catch (IOException ex) {
            // RMI error.
            throw ex;
        }

        // The default URL for that host.
        final String requestURI = "http://" + hostname + ":" + port
                + contextPath;

        return new ServiceScore(serviceUUID, hostname, requestURI);

    }

}
