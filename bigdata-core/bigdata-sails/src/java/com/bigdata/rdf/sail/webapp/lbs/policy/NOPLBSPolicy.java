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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.rdf.sail.webapp.HALoadBalancerServlet;
import com.bigdata.rdf.sail.webapp.lbs.AbstractLBSPolicy;

/**
 * This policy proxies all requests for update operations to the leader but
 * forwards read requests to the local service. Thus, it DOES NOT provide a load
 * balancing strategy, but it DOES allow update requests to be directed to any
 * service in an non-HA aware manner. This policy can be combined with an
 * external round-robin strategy to load balance the read-requests over the
 * cluster.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class NOPLBSPolicy extends AbstractLBSPolicy {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    @Override
    public boolean service(//
            final boolean isLeaderRequest,//
            final HALoadBalancerServlet servlet,//
            final HttpServletRequest request, //
            final HttpServletResponse response//
    ) throws IOException, ServletException {

        if (!isLeaderRequest) {

            // Always handle read requests locally.
            servlet.forwardToLocalService(isLeaderRequest, request, response);

            // Request was handled.
            return true;

        }

        // Proxy update requests to the quorum leader.
        return super.service(isLeaderRequest, servlet, request, response);

    }

    /**
     * Note: This method is not invoked.
     */
    @Override
    public String getReaderURI(final HttpServletRequest req) {

        throw new UnsupportedOperationException();

    }

}
