/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Apr 18, 2008
 */

package com.bigdata.util.httpd;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

/**
 * Overrides some methods on {@link NanoHTTPD} to (a) prevent serving files from
 * the local file system; and (b) to expose methods for handling GET, PUT, POST,
 * and DELETE requests - these methods all return a
 * {@link NanoHTTPD#HTTP_METHOD_NOT_ALLOWED} {@link Response} by default.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractHTTPD extends NanoHTTPD {
    
    public AbstractHTTPD(int port) throws IOException {
        
        super(port);

    }

    public Response serve(String uri, String method, Properties header,
            Map<String, Vector<String>> parms) {

        try {

            if ("GET".equalsIgnoreCase(method)) {

                return doGet(uri, method, header, parms);

            } else if ("POST".equalsIgnoreCase(method)) {

                return doGet(uri, method, header, parms);

            } else if ("PUT".equalsIgnoreCase(method)) {

                return doGet(uri, method, header, parms);

            } else if ("DELETE".equalsIgnoreCase(method)) {

                return doGet(uri, method, header, parms);

            } else {

                return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                        "" + method);

            }

        } catch (Exception e) {

            log.error(e.getMessage(), e);

            return new Response(HTTP_INTERNALERROR, MIME_TEXT_PLAIN, e
                    .getMessage());

        }

    }

    public Response doGet(String uri, String method, Properties header,
            Map<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

    public Response doPost(String uri, String method, Properties header,
            Map<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

    public Response doPut(String uri, String method, Properties header,
            Map<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

    public Response doDelete(String uri, String method, Properties header,
            Map<String, Vector<String>> parms) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN, ""
                + method);

    }

}
