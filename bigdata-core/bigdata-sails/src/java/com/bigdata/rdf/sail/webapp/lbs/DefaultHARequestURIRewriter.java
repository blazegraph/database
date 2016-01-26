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

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import com.bigdata.journal.IIndexManager;

/**
 * Default implementation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DefaultHARequestURIRewriter implements IHARequestURIRewriter {

    /**
     * {@inheritDoc}
     * <p>
     * This implementation is a NOP.
     */
    @Override
    public void init(ServletConfig servletConfig, IIndexManager indexManager)
            throws ServletException {
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * This implementation is a NOP.
     */
    @Override
    public void destroy() {
        
    }

    @Override
    public StringBuilder rewriteURI(final boolean isLeaderRequest,
            final String full_prefix, final String originalRequestURL,
            final String proxyToRequestURL, final HttpServletRequest request) {

        final StringBuilder uri = new StringBuilder(proxyToRequestURL);

        if (proxyToRequestURL.endsWith("/"))
            uri.setLength(uri.length() - 1);

        final String rest = originalRequestURL.substring(full_prefix.length());

        if (!rest.startsWith("/"))
            uri.append("/");

        uri.append(rest);

        final String query = request.getQueryString();

        if (query != null)
            uri.append("?").append(query);

        return uri;

    }

}
