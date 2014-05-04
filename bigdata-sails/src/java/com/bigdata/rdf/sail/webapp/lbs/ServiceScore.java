/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.lbs;

import java.io.IOException;
import java.util.UUID;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.jini.ha.HAJournal;
import com.bigdata.quorum.Quorum;
import com.bigdata.rdf.sail.webapp.IHARequestURIRewriter;

/**
 * Helper class caches metadata about an {@link HAGlue} service.
 * <p>
 * Note: This class is written fairly defensively. The fields can wind up being
 * left at their default values (typically <code>null</code>) if we are not able
 * to find the necessary information for the {@link HAGlue} service. Users of
 * this class must test for <code>null</code> values and skip over those
 * services since they have not been pre-resolved to a host and requestURL.
 */
public class ServiceScore {

    /**
     * The service {@link UUID} for the remote service.
     * 
     * @see HAGlue#getServiceUUID()
     */
    final private UUID serviceUUID;
    
//    /**
//     * The {@link HAGlue} interface for the remote service.
//     */
//    private HAGlue haGlue;
    
    /**
     * The hostname for the remote service.
     * 
     * @see HAGlue#getHostname()
     */
    private String hostname;
    
    /**
     * The port for the NSS on the remote service.
     * 
     * @see HAGlue#getNSSPort()
     */
    private int port;
    
    /**
     * The constructed Request-URL for the root of the servlet context for the
     * NSS on the remote service -or- <code>null</code> if anything goes wrong.
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
     * The port for the NSS on the remote service.
     * 
     * @see HAGlue#getNSSPort()
     */
    public int getPort() {
       
        return port;
        
    }
    
    /**
     * The {@link #requestURI} for the root of the web application on the target
     * host. This is assigned IFF everything succeeds. This is what we will use
     * to proxy a request to the service having the {@link UUID} given to the
     * constructor.
     * <p>
     * Note: This needs to be a URL, not just a relative path. Otherwise you get
     * an NPE.
     * <p>
     * This is formed as:
     * 
     * <pre>
     * requestURL = &quot;http://&quot; + hostname + &quot;:&quot; + port + contextPath;
     * </pre>
     * 
     * The <code>hostname</code> is obtained from {@link HAGlue#getHostname()}.
     * <p>
     * The <code>port</code> is obtained from {@link HAGlue#getNSSPort()}.
     * 
     * TODO How do we configured the protocol for the remote NSS instance? This
     * code assumes that it is <code>http</code>, but <code>https</code> is also
     * possible. This could be handled by an {@link IHARequestURIRewriter} but
     * maybe the {@link HAGlue} interface should be declaring this too?
     */
    public String getRequestURI() {

        return requestURI;
        
    }
    
    @Override
    public String toString() {
        
        return getClass().getName() + "{serviceUUID=" + serviceUUID
                + ", hostname=" + hostname + ", port=" + port + ", requestURI="
                + requestURI + "}";
        
    }

    public ServiceScore(final IIndexManager indexManager,
            final String contextPath, final UUID serviceUUID) {

        if (serviceUUID == null)
            throw new IllegalArgumentException();

        this.serviceUUID = serviceUUID;

        final HAJournal journal = (HAJournal) indexManager;

        final Quorum<HAGlue, QuorumService<HAGlue>> quorum = journal
                .getQuorum();

        if (quorum == null) {
            // No quorum.
            return;
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
                return;
            }
            quorumService = t;
        }

        final HAGlue haGlue;
        try {
            haGlue = quorumService.getService(serviceUUID);
        } catch (IllegalArgumentException ex) {
            // No such service.
            return;
        }

        /*
         * TODO The hostname and port are RMIs. Use a smart proxy for HAGlue. Or
         * consult a cache of existing ServiceScore objects.
         */
        try {
            hostname = haGlue.getHostname();
            port = haGlue.getNSSPort();
        } catch (IOException ex) {
            // RMI error.
            return;
        }

        // The default URL for that host.
        requestURI = "http://" + hostname + ":" + port + contextPath;

    }

}