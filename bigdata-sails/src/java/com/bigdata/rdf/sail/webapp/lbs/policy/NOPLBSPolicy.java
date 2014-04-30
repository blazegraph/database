/**
Copyright (C) SYSTAP, LLC 2006-2014.  All rights reserved.

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
package com.bigdata.rdf.sail.webapp.lbs.policy;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.rdf.sail.webapp.lbs.AbstractLBSPolicy;

/**
 * This policy proxies all requests for update operations to the leader but
 * forwards read requests to the local service. Thus, it does not provide a load
 * balancing strategy, but it does allow update requests to be directed to any
 * service in an non-HA aware manner. This policy can be combined with an
 * external round-robin strategy to load balance the read-requests over the
 * cluster.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO A service that is not joined with the met quorum can not answer
 *         a read-request. In order to be generally useful (and not just as a
 *         debugging policy), we need to proxy a read-request when this service
 *         is not joined with the met quorum. If there is no met quorum, then we
 *         can just forward the request to the local service and it will report
 *         the NoQuorum error.
 */
public class NOPLBSPolicy extends AbstractLBSPolicy {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public boolean service(final boolean isLeaderRequest,
            final HttpServletRequest request, final HttpServletResponse response)
            throws IOException, ServletException {

        if (!isLeaderRequest) {

            // Always handle read requests locally.
            HALoadBalancerServlet.forwardToThisService(isLeaderRequest,
                    request, response);

            // Request was handled.
            return true;

        }

        // Proxy update requests to the quorum leader.
        return super.service(isLeaderRequest, request, response);

    }

    /**
     * Note: This method is not invoked.
     */
    @Override
    public String getReaderURL(final HttpServletRequest req) {

        throw new UnsupportedOperationException();

    }

}