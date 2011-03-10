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
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

import com.bigdata.util.httpd.NanoHTTPD.Response;

/**
 * Overrides some methods on {@link NanoHTTPD} to (a) prevent serving files from
 * the local file system; and (b) to expose methods for handling GET, PUT, POST,
 * and DELETE requests - these methods all return a
 * {@link NanoHTTPD#HTTP_METHOD_NOT_ALLOWED} {@link Response} by default.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractHTTPD extends NanoHTTPD implements HTTPGetHandler {

    final static private Logger log = Logger.getLogger(AbstractHTTPD.class);

    public AbstractHTTPD(final int port) throws IOException {
        
        super(port);

    }

    @Override
    public Response serve(final Request req) {

        if (log.isDebugEnabled()) {
            log.debug("uri=" + req.uri);
            log.debug("method=" + req.method);
            log.debug("headers=" + req.headers);
            log.debug("params=" + req.params);
        } else if (log.isInfoEnabled())
            log.info(req.method + " '" + req.uri + "' ");

        try {

            if (GET.equalsIgnoreCase(req.method)) {

                return doGet(req);

            } else if (POST.equalsIgnoreCase(req.method)) {

                return doPost(req);

            } else if (PUT.equalsIgnoreCase(req.method)) {

                return doPut(req);

            } else if (DELETE.equalsIgnoreCase(req.method)) {

                return doDelete(req);

            } else {

                return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                        "" + req.method);

            }

        } catch (Throwable e) {

            log.error(e.getMessage(), e);

            final StringWriter w = new StringWriter();
            
            e.printStackTrace(new PrintWriter(w));
            
            /*
             * Note: if you send an HTTP_INTERNALERROR (500) the browser won't
             * display the response body so you have to send an Ok with the text
             * of the error message....
             */

            return new Response(HTTP_OK, MIME_TEXT_PLAIN, w.toString());

        }

    }

    /**
     * Handle a GET request.
     * 
     * @param req
     *            The request
     * 
     * @return The response.
     * 
     * @throws Exception
     */
    public Response doGet(final Request req) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                req.method);

    }

    /**
     * Handle a POST request.
     * 
     * @param req
     *            The request
     * 
     * @return The response.
     * 
     * @throws Exception
     */
    public Response doPost(final Request req) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                req.method);

    }

    /**
     * Handle a PUT request.
     * 
     * @param req
     *            The request
     * 
     * @return The response.
     * 
     * @throws Exception
     */
    public Response doPut(final Request req) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                req.method);

    }

    /**
     * Handle a DELETE request.
     * 
     * @param req
     *            The request
     * 
     * @return The response.
     * 
     * @throws Exception
     */
    public Response doDelete(final Request req) throws Exception {

        return new Response(HTTP_METHOD_NOT_ALLOWED, MIME_TEXT_PLAIN,
                req.method);

    }

}
