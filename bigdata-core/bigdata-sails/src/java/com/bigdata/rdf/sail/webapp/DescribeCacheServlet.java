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
package com.bigdata.rdf.sail.webapp;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.Graph;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterRegistry;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sparql.ast.cache.CacheConnectionFactory;
import com.bigdata.rdf.sparql.ast.cache.ICacheConnection;
import com.bigdata.rdf.sparql.ast.cache.IDescribeCache;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A maintained cache for DESCRIBE of URIs.
 * <p>
 * In general, URIs may be identified either by a bare URI (in which case the
 * URI must be some extension of the SPARQL endpoint path) or by a SPARQL
 * DESCRIBE query ( <code>endpoint?query=DESCRIBE uri(,uri)*</code>).
 * <p>
 * The {@link DescribeCacheServlet} will recognize and perform the DESCRIBE of
 * cached resources where those resources are attached to a request attribute
 * 
 * TODO Http cache control. Different strategies can make sense depending on the
 * scalability of the application and the tolerance for stale data. We can use
 * an expire for improved scalability with caching into the network, but
 * invalidation notices can not propagate beyond the DESCRIBE cache to the
 * network. We can use E-Tags with Must-Validate to provide timely invalidation.
 * If we support faceting (by schema or source) then we need to provide E-Tags
 * for each schema/source so the client can inspect/require certain facets.
 * 
 * TODO VoID for forward/reverse link set and attribute sketch.
 * 
 * TODO Conneg for the actual representation, but internally use an efficient
 * representation that can provide summaries (a SKETCH) through a jump table
 * with statistics for each predicate type.
 * 
 * TODO Options for DESCRIBE filters by schema to get only properties and links
 * for a given schema namespace?
 * 
 * TODO Describe caching in an open web context requires maintaining metadata
 * about the web resources that have been queried for a given URI. This should
 * include at least the URI itself and could include well known aggregators that
 * might have data for that URI.
 * 
 * TODO Take advantage of the known materialization performed by a DESCRIBE
 * query when running queries (materialized star-join). Also, store IVs in the
 * Graph as well as Values. We need both to do efficient star-joins (enter by IV
 * and have IV on output).
 * 
 * TODO Hash partitioned DESCRIBE fabric. The partitioned map is easy enough and
 * could be installed at each DS, CS, etc. node. However, the distributed
 * invalidating scheme is slightly trickier. We would need to install this
 * servlet at each endpoint exposed to mutation, which is not actually all that
 * difficult. There needs to be configuration information for either each
 * namespace or for the webapp that specifies how to locate and maintain the
 * cache.
 * 
 * TODO Expose (and declare through VoID) a simple URI lookup service that is
 * powered by this cache and turns into a DESCRIBE query if there is a cache
 * miss (or turn it into a DESCRIBE query and let that route to the cache
 * first).  VoID has the concept of this kind of "lookup" service.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584"> DESCRIBE
 *      CACHE </a>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class DescribeCacheServlet extends BigdataRDFServlet {

    static private final transient Logger log = Logger
            .getLogger(DescribeCacheServlet.class);
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * The name of a request attribute whose bound value is a {@link Set} of
     * {@link URI}s to be described by the {@link DescribeCacheServlet}.
     */
    static final transient String ATTR_DESCRIBE_URIS = "describeUris";
    
    public DescribeCacheServlet() {
        
    }
    
    /**
     * GET returns the DESCRIBE of the resource.
     * 
     * FIXME DESCRIBE: TX ISOLATION for request but ensure that cache is not
     * negatively effected by that isolation (i.e., how does the cache index
     * based on time tx view).
     */
    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        /*
         * 1. Check the request path for a linked data GET on a resource.
         * If found, then add that URI to the request attribute.
         * 
         * 2. Else, if the request is a SPARQL DESCRIBE, then extract the URIs
         * to be described and attach them as request attributes (on a set).
         * A single Graph will be returned in this case. The client will have
         * to inspect the Graph to decide which URIs were found and which were
         * not.
         * 
         * 3. Check the request attribute for a set of URIs to be DESCRIBEd.
         */
        
        @SuppressWarnings("unchecked")
        final Set<URI> externalURIs = (Set<URI>) req.getAttribute(ATTR_DESCRIBE_URIS);

        if (externalURIs == null) {

            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Request attribute not found: " + ATTR_DESCRIBE_URIS);
            
            return;
            
        }

        final int nvalues = externalURIs.size();

        if (nvalues == 0) {

            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN, "No URIs");

            return;

        }

        final BigdataRDFContext context = getBigdataRDFContext();

        final QueryEngine queryEngine = QueryEngineFactory.getInstance()
                .getQueryController(context.getIndexManager());

        // Iff enabled.
        final ICacheConnection cacheConn = CacheConnectionFactory
                .getExistingCacheConnection(queryEngine);

        final String namespace = getNamespace(req);

        final long timestamp = getTimestamp(req);

        final IDescribeCache describeCache = cacheConn == null ? null
                : cacheConn.getDescribeCache(namespace, timestamp);

        if (describeCache == null) {

            /*
             * DESCRIBE cache is not enabled.
             * 
             * Note: DO NOT commit the response. The DESCRIBE of the resource
             * can be generated by running a SPARQL query.
             */
            
            resp.setStatus(HTTP_NOTFOUND);
            
            return;
            
        }
        
        final AbstractTripleStore tripleStore = context.getTripleStore(
                namespace, timestamp);

        if (tripleStore == null) {

            /*
             * There is no such triple/quad store instance.
             */
            
            buildAndCommitNamespaceNotFoundResponse(req, resp);

            return;
        
        }
        
        /*
         * Ensure that URIs are BigdatURIs for this namespace.
         */
        final Set<BigdataURI> internalURIs = new LinkedHashSet<BigdataURI>();
        {

            final BigdataValueFactory valueFactory = tripleStore
                    .getValueFactory();

            for (URI uri : externalURIs) {

                internalURIs.add(valueFactory.asValue(uri));

            }
            
        }
        
        /*
         * Resolve URIs to IVs.
         */
        {

            final BigdataValue[] values = internalURIs
                    .toArray(new BigdataValue[nvalues]);

            final long numNotFound = tripleStore.getLexiconRelation().addTerms(
                    values, nvalues, true/* readOnly */);

            if (log.isInfoEnabled())
                log.info("Not found: " + numNotFound + " out of "
                        + values.length);
            
        }
        
        /*
         * Build up the response graph.
         * 
         * TODO If the describe would be very large, then provide the summary
         * rather than delivering all the data. This will require a blobs aware
         * handling of the Values in the HTree.
         * 
         * TODO Support SKETCH (VoID request) option here.
         */
        Graph g = null;
        {

            for (BigdataURI uri : internalURIs) {

                final IV<?, ?> iv = uri.getIV();

                final Graph x = describeCache.lookup(iv);

                if (x != null && g == null) {

                    if (nvalues == 1) {
                    
                        // Only describing ONE (1) resource.
                        g = x;

                    } else {

                        // Collect the DESCRIBE of all graphs.
                        g = new GraphImpl();

                        // Combine the resource descriptions together.
                        g.addAll(x);

                    }

                }

            }
        
            if (g == null) {

                /*
                 * None of the URIs was found.
                 * 
                 * Note: We can only send the NOT_FOUND status and commit the
                 * response if the cache is complete. Otherwise, we might set
                 * the status code but we SHOULD NOT commit the response since
                 * the DESCRIBE of the resource can be generated by running a
                 * SPARQL query.
                 */

                // Not in the cache.  Note: Response is NOT committed.
                resp.setStatus(HTTP_NOTFOUND);
//                buildResponse(resp, HTTP_NOTFOUND, MIME_TEXT_PLAIN);

                return;

            }
        }

        /*
         * CONNEG
         */
        final RDFFormat format;
        {
            /*
             * CONNEG for the MIME type.
             * 
             * Note: An attempt to CONNEG for a MIME type which can not be
             * used with a given type of query will result in a response
             * using a default MIME Type for that query.
             */
            final String acceptStr = req.getHeader("Accept");

            final ConnegUtil util = new ConnegUtil(acceptStr);

            format = util.getRDFFormat(RDFFormat.RDFXML);
        }

        /*
         * Generate response.
         */
        try {
           
            final String mimeType = format.getDefaultMIMEType();
            
            resp.setContentType(mimeType);

            if (isAttachment(mimeType)) {
                /*
                 * Mark this as an attachment (rather than inline). This is
                 * just a hint to the user agent. How the user agent handles
                 * this hint is up to it.
                 */
                resp.setHeader("Content-disposition",
                        "attachment; filename=query" + UUID.randomUUID()
                                + "." + format.getDefaultFileExtension());
            }

            if (format.hasCharset()) {

                // Note: Binary encodings do not specify charset.
                resp.setCharacterEncoding(format.getCharset().name());

            }
            
            final OutputStream os = resp.getOutputStream();

            final RDFWriter w = RDFWriterRegistry.getInstance().get(format)
                    .getWriter(os);

            w.startRDF();

            for (Statement s : g)
                w.handleStatement(s);
            
            w.endRDF();
            os.flush();

        } catch (Throwable e) {

            BigdataRDFServlet.launderThrowable(e, resp,
                    "DESCRIBE: uris=" + internalURIs);
            
        }

    }

}
