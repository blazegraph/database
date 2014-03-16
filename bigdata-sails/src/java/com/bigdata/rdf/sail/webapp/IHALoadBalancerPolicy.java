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
package com.bigdata.rdf.sail.webapp;

import javax.servlet.http.HttpServletRequest;

/**
 * Load balancer policy interface.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see HALoadBalancerServlet
 * @see <a href="http://trac.bigdata.com/ticket/624">HA Load Balancer</a>
 */
public interface IHALoadBalancerPolicy {

    /**
     * Return the URL to which the request will be proxied. The returned URL
     * must include the protocol, hostname and port (if a non-default port will
     * be used) as well as the target request path.
     * 
     * @param req
     *            The request.
     * 
     * @return The URL.
     */
    String proxyTo(HttpServletRequest req);
    
}
