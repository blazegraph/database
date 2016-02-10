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
package com.bigdata.rdf.sail.webapp.lbs.policy;

import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.http.HttpServletRequest;

import com.bigdata.rdf.sail.webapp.lbs.AbstractLBSPolicy;
import com.bigdata.rdf.sail.webapp.lbs.ServiceScore;

/**
 * Policy implements a round-robin over the services that are joined with the
 * met quorum.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class RoundRobinLBSPolicy extends AbstractLBSPolicy {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    protected void toString(final StringBuilder sb) {

        super.toString(sb);
        
        sb.append(",nextService=" + nextService.get());

    }

    /**
     * {@inheritDoc}
     * <p>
     * This imposes a round-robin policy over the discovered services. If the
     * service is discovered and appears to be joined with the met quorum, then
     * the request can be proxied to that service.
     */
    @Override
    public String getReaderURI(final HttpServletRequest request) {

        final ServiceScore[] serviceScores = this.serviceTableRef.get();

        if (serviceScores == null) {

            // Nothing discovered. Can't proxy.
            return null;

        }

        /*
         * Choose a service.
         * 
         * Note: This is a round robin over the services. Any service that is
         * joined with the met quorum can be selected as a target for the read
         * request.
         * 
         * Note: The round-robin is atomic with respect to each request. The
         * request obtains a starting point in the serviceScores[] and then
         * finds the next service in that array using a round-robin. The
         * [nextService] starting point is not updated until the round-robin is
         * complete - this is necessary in order to avoid skipping over services
         * that are being checked by a concurrent request.
         * 
         * The [nextService] is updated only after the round-robin decision has
         * been made. As soon as it has been updated, a new round-robin decision
         * will be made with respect to the new value for [nextService] but any
         * in-flight decisions will be made against the value of [nextService]
         * that they observed on entry.
         */

        // The starting offset for the round-robin.
        final long startIndex = nextService.longValue();

        // The selected service.
        ServiceScore serviceScore = null;

        for (int i = 0; i < serviceScores.length; i++) {

            /*
             * Find the next host index.
             * 
             * Note: We need to ensure that the hostIndex stays in the legal
             * range, even with concurrent requests and when wrapping around
             * MAX_VALUE.
             */
            final int hostIndex = (int) Math
                    .abs(((i + startIndex) % serviceScores.length));

            serviceScore = serviceScores[hostIndex];

            if (serviceScore == null)
                continue;

            if (serviceScore.getHostname() == null) {
                // Can't use if no hostname.
                continue;
            }

            if (serviceScore.getRequestURI() == null) {
                // Can't use if no requestURL.
                continue;
            }

        }

        // Bump the nextService counter.
        nextService.incrementAndGet();

        if (serviceScore == null) {

            // No service. Can't proxy.
            return null;

        }

        // track #of requests to each service.
        serviceScore.nrequests.increment();
        
        return serviceScore.getRequestURI();

    }

    /**
     * Note: This could be a hot spot. We can have concurrent requests and we
     * need to increment this counter for each such request.
     */
    private final AtomicLong nextService = new AtomicLong(0L);

}
