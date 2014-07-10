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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.bigdata.rdf.task.AbstractApiTask;

/**
 * Abstract base class for REST API methods. This class is compatible with a
 * job-oriented concurrency control pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @param <T>
 * @see <a href="http://sourceforge.net/apps/trac/bigdata/ticket/753" > HA
 *      doLocalAbort() should interrupt NSS requests and AbstractTasks </a>
 * @see <a href="- http://sourceforge.net/apps/trac/bigdata/ticket/566" >
 *      Concurrent unisolated operations against multiple KBs </a>
 * 
 *      FIXME GROUP COMMIT: Define DropKBTask and CreateKBTask for use by (a)
 *      the multi-tenancy API; and (b) variants without servlet request and
 *      response parameters for use by the unit tests and the NSS during its
 *      default KB create logic. These latter tasks should be a base class of
 *      the RestApiTask that supports the same delegation pattern, but which
 *      does not require the http request and response parameters. Fix up the
 *      callers of CreateKBTask and tripleStore.destroy() to use these tasks.
 */
abstract class AbstractRestApiTask<T> extends AbstractApiTask<T> {

    /** The {@link HttpServletRequest}. */
    protected final HttpServletRequest req;
    /** The {@link HttpServletResponse}. */
    protected final HttpServletResponse resp;

    /**
     * @param req
     *            The {@link HttpServletRequest}.
     * @param resp
     *            The {@link HttpServletResponse}.
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp of the view of that KB instance.
     */
    protected AbstractRestApiTask(final HttpServletRequest req,
            final HttpServletResponse resp, final String namespace,
            final long timestamp) {
        super(namespace,timestamp);
        this.req = req;
        this.resp = resp;
    }

    protected void reportModifiedCount(final long nmodified, final long elapsed)
            throws IOException {

        BigdataRDFServlet.reportModifiedCount(resp, nmodified, elapsed);

    }

    abstract static class RestApiQueryTask<T> extends AbstractRestApiTask<T> {
        /**
         * 
         * @param req
         *            The {@link HttpServletRequest}.
         * @param resp
         *            The {@link HttpServletResponse}.
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a query connection.
         */
        public RestApiQueryTask(//
                final HttpServletRequest req,//
                final HttpServletResponse resp,//
                final String namespace, final long timestamp) {

            super(req, resp, namespace, timestamp);
            
        }

    }

    abstract static class RestApiMutationTask<T> extends AbstractRestApiTask<T> {
        /**
         * 
         * @param req
         *            The {@link HttpServletRequest}.
         * @param resp
         *            The {@link HttpServletResponse}.
         * @param namespace
         *            The namespace of the target KB instance.
         * @param timestamp
         *            The timestamp used to obtain a mutable connection.
         */
        public RestApiMutationTask(//
                final HttpServletRequest req,//
                final HttpServletResponse resp,//
                final String namespace, final long timestamp) {

            super(req, resp, namespace, timestamp);
            
        }

    }
    
}
