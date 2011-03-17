/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
/*
 * Created on Mar 9, 2011
 */

package com.bigdata.util.httpd;

import java.util.Collection;

import com.bigdata.util.httpd.NanoHTTPD.Request;
import com.bigdata.util.httpd.NanoHTTPD.Response;

/**
 * Interface allows for implementation of different handlers for "GET".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public interface HTTPGetHandler {

    /**
     * HTTP GET 
     * 
     * @param uri
     *            Percent-decoded URI without parameters, for example
     *            "/index.cgi"
     * @param method
     *            "GET", "POST" etc.
     * @param parms
     *            Parsed, percent decoded parameters from URI and, in
     *            case of POST, data. The keys are the parameter names.
     *            Each value is a {@link Collection} of {@link String}s
     *            containing the bindings for the named parameter. The
     *            order of the URL parameters is preserved.
     * @param header
     *            Header entries, percent decoded
     * 
     * @return HTTP response
     * 
     * @see Response
     */
    public Response doGet(final Request req)
            throws Exception;

}
