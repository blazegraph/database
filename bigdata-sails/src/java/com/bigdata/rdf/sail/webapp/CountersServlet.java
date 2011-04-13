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
 * Created on Apr 12, 2011
 */

package com.bigdata.rdf.sail.webapp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.counters.CounterSet;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.query.CounterSetSelector;
import com.bigdata.counters.query.URLQueryModel;
import com.bigdata.counters.render.IRenderer;
import com.bigdata.counters.render.RendererFactory;
import com.bigdata.rawstore.Bytes;
import com.bigdata.service.IEventReceivingService;
import com.bigdata.service.IService;

/**
 * Servlet for exposing performance counters.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 *          FIXME The state needs to be added to the servlet context
 *          (BigdataContext), ideally under named attribute which is specific to
 *          the performance counters. That state includes the time since the
 *          last rendering of the performance counter data (unless this can be
 *          done by a jetty configuration).
 *          <p>
 *          The {@link BigdataRDFContext} has all sorts of logic for starting and
 *          stopping statistics collection which belongs in the Journal or the
 *          DataService, not in the BigdataContext. [Or perhaps some of it is
 *          logic for the NanoSparqlServer's performance counter collection, in
 *          which case it might make sense where it is, but we need to separate
 *          the core bigdata layer from the SPARQL layer.]
 *          <p>
 *          The SPARQL layer needs to be separated from the core bigdata layer,
 *          with the BigdataContext moving into a servlet package in the bigdata
 *          module and the CountersServlet moving into a servlet package in the
 *          com.bigdata.counters package namespace.
 *          <p>
 *          The flot resources need to be retrievable.
 *          <p>
 *          The event reporting needs to be hooked via
 *          {@link IEventReceivingService}, at least for scale-out.
 * 
 */
public class CountersServlet extends BigdataServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(CountersServlet.class); 

    /**
     * 
     */
    public CountersServlet() {
    }

//    /**
//     * Access to the {@link CounterSet} exposed by this service.
//     */
//    private final ICounterSetAccess accessor;
//    
//    /**
//     * The service reference iff one one specified to the ctor (may be null).
//     */
//    private final IService service;
//
//    /**
//     * The minimum time before a client can force the re-materialization of the
//     * {@link CounterSet}. This is designed to limit the impact of the client on
//     * the service.
//     * 
//     * TODO Configuration parameter for {@link #minUpdateLatency}
//     */
//    private final long minUpdateLatency = 5000;
//    
//    /**
//     * The last materialized {@link CounterSet}.
//     */
//    private volatile CounterSet counterSet = null;
//
//    /**
//     * The timestamp of the last materialized {@link CounterSet}.
//     */
//    private volatile long lastTimestamp = 0L;

    /**
     * Performance counters
     * <pre>
     * GET /counters
     * </pre>
     */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(
                2 * Bytes.kilobyte32);

        final InputStream is;

//        /*
//         * If the request uri is one of the pre-declared resources then we send
//         * that resource.
//         */
//        final DeclaredResource decl = allowedClassPathResources.get(req.uri);
//
//        if (decl != null) {
//
//            // send that resource.
//            return sendClasspathResource(decl);
//
//        }

        /*
         * Materialization the CounterSet iff necessary or stale.
         * 
         * Note: This bit needs to be single threaded to avoid concurrent
         * requests causing concurrent materialization of the counter set.
         */
//        final ICounterSelector counterSelector;
//        synchronized(this) {
//            
//            final long now = System.currentTimeMillis();
//
//            final long elapsed = now - lastTimestamp;
//
//            if (counterSet == null || elapsed > minUpdateLatency/* ms */) {
//
//                counterSet = accessor.getCounters();
//                
//            }
//
//            counterSelector = new CounterSetSelector(counterSet);
//
//        }

        // FIXME Hook this how?
        final IService service = null;
        
        // FIXME Hook this how? Extension for SPARQL end point when it exists?
        final CounterSet counterSet = ((ICounterSetAccess) getIndexManager())
                .getCounters();

        final CounterSetSelector counterSelector = new CounterSetSelector(
                counterSet);

        /*
         * Obtain a renderer.
         * 
         * @todo This really should pass in the Accept header and our own list
         * of preferences and do CONNEG for (X)HMTL vs XML vs text/plain.
         * 
         * @todo if controller state error then send HTTP_BAD_REQUEST
         * 
         * @todo Write XSL and stylesheet for interactive browsing of the
         * CounterSet XML?
         */
        final String mimeType = MIME_TEXT_HTML;
        final IRenderer renderer;
        {

            // build model of the controller state.
            final URLQueryModel model = URLQueryModel.getInstance(service,
                    req, resp);

            renderer = RendererFactory.get(model, counterSelector, mimeType);
            
        }

        /*
         * Render the counters as specified by the query for the negotiated MIME
         * type.
         */
        {

            final OutputStreamWriter w = new OutputStreamWriter(baos);

            // render the view.
            renderer.render(w);

            w.flush();

            is = new ByteArrayInputStream(baos.toByteArray());
            
        }
        
        resp.setStatus(HTTP_OK);
        
        resp.setContentType(mimeType + "; charset='"
                + charset + "'");

        /*
         * Sets the cache behavior -- the data should be good for up to 60
         * seconds unless you change the query parameters. These cache control
         * parameters SHOULD indicate that the response is valid for 60 seconds,
         * that the client must revalidate, and that the response is cachable
         * even if the client was authenticated.
         */
        resp.addHeader("Cache-Control", "max-age=60, must-revalidate, public");
        // to disable caching.
        // r.addHeader("Cache-Control", "no-cache");

        copyStream(is,resp.getOutputStream());

    }
    
}
