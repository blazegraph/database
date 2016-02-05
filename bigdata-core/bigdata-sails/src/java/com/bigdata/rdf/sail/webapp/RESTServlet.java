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
import java.util.Collections;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.sail.webapp.client.MiniMime;

/**
 * Default dispatch pattern for a core REST API.
 * 
 * @author Martyn Cutcher
 */
public class RESTServlet extends BigdataRDFServlet {

    private static final transient Logger log = Logger
            .getLogger(RESTServlet.class);

    static {
//        // pull a Tinkerpop interface into the class loader
//        log.info(com.tinkerpop.blueprints.Graph.class.getName());
        
//        log.info(org.apache.avro.Schema.class.getName());
    }
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    /*
     * The delegates to which we dispatch the various requests.
     */
    private QueryServlet m_queryServlet;
    private InsertServlet m_insertServlet;
    private DeleteServlet m_deleteServlet;
    private UpdateServlet m_updateServlet;
    private WorkbenchServlet m_workbenchServlet;
    private BlueprintsServletProxy m_blueprintsServlet;
    private MapgraphServletProxy m_mapgraphServlet;
    
    /**
     * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/584">
     *      DESCRIBE CACHE </a>
     */
    private DescribeCacheServlet m_describeServlet;
    
    public RESTServlet() {
    }

    /**
     * Overridden to create and initialize the delegate {@link Servlet}
     * instances.
     */
    @Override
    public void init() throws ServletException {
        
        super.init();
        
        m_queryServlet = new QueryServlet();
        m_insertServlet = new InsertServlet();
        m_updateServlet = new UpdateServlet();
        m_deleteServlet = new DeleteServlet();
        m_describeServlet = new DescribeCacheServlet();
        m_workbenchServlet = new WorkbenchServlet();
        
        {
            final String provider = this
    				.getInitParameter(ConfigParams.BLUEPRINTS_SERVLET_PROVIDER); 
           
    		final String blueprintsProvider = provider == null
    				|| provider.equals("") ? ConfigParams.DEFAULT_BLUEPRINTS_SERVLET_PROVIDER
    				: provider;
            
            m_blueprintsServlet = BlueprintsServletProxy.BlueprintsServletFactory
                    .getInstance(blueprintsProvider);
        }

        {
            final String provider = this
                    .getInitParameter(ConfigParams.MAPGRAPH_SERVLET_PROVIDER); 
           
            final String mapgraphProvider = provider == null
                    || provider.equals("") ? ConfigParams.DEFAULT_MAPGRAPH_SERVLET_PROVIDER
                    : provider;
            
            m_mapgraphServlet = new MapgraphServletProxy.MapgraphServletFactory()
                .getInstance(mapgraphProvider);
        }

        m_queryServlet.init(getServletConfig());
        m_insertServlet.init(getServletConfig());
        m_updateServlet.init(getServletConfig());
        m_deleteServlet.init(getServletConfig());
        m_describeServlet.init(getServletConfig());
        m_workbenchServlet.init(getServletConfig());
        m_blueprintsServlet.init(getServletConfig());
        m_mapgraphServlet.init(getServletConfig());
        
    }
    
    /**
     * Overridden to destroy the delegate {@link Servlet} instances.
     */
    @Override
    public void destroy() {
        
        if (m_queryServlet != null) {
            m_queryServlet.destroy();
            m_queryServlet = null;
        }
        
        if (m_insertServlet != null) {
            m_insertServlet.destroy();
            m_insertServlet = null;
        }
        
        if (m_updateServlet != null) {
            m_updateServlet.destroy();
            m_updateServlet = null;
        }
        
        if (m_deleteServlet != null) {
            m_deleteServlet.destroy();
            m_deleteServlet = null;
        }

        if (m_describeServlet != null) {
            m_describeServlet.destroy();
            m_describeServlet = null;
        }

        if (m_workbenchServlet != null) {
            m_workbenchServlet.destroy();
            m_workbenchServlet = null;
        }

        if (m_blueprintsServlet != null) {
            m_blueprintsServlet.destroy();
            m_blueprintsServlet = null;
        }

        if (m_mapgraphServlet != null) {
            m_mapgraphServlet.destroy();
            m_mapgraphServlet = null;
        }

        super.destroy();
        
    }

    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (log.isInfoEnabled())
            log.info(req.toString());
        
        /*
         * Look for linked data GET requests.
         * 
         * Note: URIs ending in /sparql are *assumed* to be SPARQL end points on
         * this server. A GET against a SPARQL end point is a SERVICE
         * DESCRIPTION request (not a DESCRIBE) and will be handled by the
         * QueryServlet.
         */

        final String pathInfo = req.getPathInfo();

        if (pathInfo != null && !pathInfo.endsWith("/sparql")) {

            final URI uri = new URIImpl(req.getRequestURL().toString());

            if (m_describeServlet != null) {

                /*
                 * Test the DESCRIBE cache. 
                 */
                
                req.setAttribute(DescribeCacheServlet.ATTR_DESCRIBE_URIS,
                        Collections.singleton(uri));

                m_describeServlet.doGet(req, resp);

                if (resp.isCommitted()) {
                 
                    // Cache hit.
                    return;
                    
                }

            }

            // Set this up as a DESCRIBE query.
            req.setAttribute(QueryServlet.ATTR_QUERY,
                    "DESCRIBE <" + uri.stringValue() + ">");
            
            // Handle the linked data GET as a DESCRIBE query.
            m_queryServlet.doSparqlQuery(req, resp);
            
            return;

        }

        /*
         * Otherwise, GET against the SPARQL endpoint is only allowed with query
         * requests, so delegate to the QueryServlet.
         */
        
        m_queryServlet.doGet(req, resp);
        
    }

    /**
     * A query can be submitted with a POST if a query parameter is provided.
     * Otherwise delegate to the {@link InsertServlet} or {@link DeleteServlet}
     * as appropriate.
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
    	
        if (log.isInfoEnabled())
            log.info(req.toString());

        if (req.getParameter(QueryServlet.ATTR_QUERY) != null
                || req.getParameter(QueryServlet.ATTR_UPDATE) != null
                || req.getParameter(QueryServlet.ATTR_UUID) != null
                || req.getParameter(QueryServlet.ATTR_ESTCARD) != null
                || req.getParameter(QueryServlet.ATTR_HASSTMT) != null
                || req.getParameter(QueryServlet.ATTR_GETSTMTS) != null
                || req.getParameter(QueryServlet.ATTR_CONTEXTS) != null
                // the two cases below were added to fix bug trac 711
                || hasMimeType(req, BigdataRDFServlet.MIME_SPARQL_UPDATE)
                || hasMimeType(req, BigdataRDFServlet.MIME_SPARQL_QUERY)
                ) {

            // SPARQL QUERY -or- SPARQL UPDATE via POST
            m_queryServlet.doPost(req, resp);

        } else if (req.getParameter("updatePost") != null) {

           // UPDATE via POST w/ Multi-part Body.
           m_updateServlet.doPost(req, resp);

        } else if (req.getParameter("delete") != null) {
            
            // DELETE via POST w/ Body.
            m_deleteServlet.doPost(req, resp);

        } else if (req.getParameter(StatusServlet.CANCEL_QUERY) != null) {

            StatusServlet.doCancelQuery(req, resp, getIndexManager(),
                    getBigdataRDFContext());

            buildAndCommitResponse(resp, HTTP_OK, MIME_TEXT_PLAIN, "");

        } else if (req.getParameter(WorkbenchServlet.ATTR_WORKBENCH) != null) {
        	
        	m_workbenchServlet.doPost(req, resp);
        	
        } else if (req.getParameter(BlueprintsServletProxy.ATTR_BLUEPRINTS) != null) {
            
            m_blueprintsServlet.doPostRequest(req, resp);
            
        } else if (req.getParameter(MapgraphServletProxy.ATTR_MAPGRAPH) != null) {
            
            m_mapgraphServlet.doPostRequest(req, resp);
            
        } else if (req.getParameter("uri") != null) {

            // INSERT via w/ URIs
            m_insertServlet.doPost(req, resp);
	        
		} else {

		    // INSERT via POST w/ Body
		    m_insertServlet.doPost(req, resp);
		    
		}

	}

    static boolean hasMimeType(final HttpServletRequest req,
            final String mimeType) {

        final String contentType = req.getContentType();
        
        return contentType != null
                && mimeType.equals(new MiniMime(contentType).getMimeType());
        
	}

    /**
     * A PUT request always delegates to the {@link UpdateServlet}.
     * <p>
     * Note: The semantics of PUT are "DELETE+INSERT" for the API. PUT is not
     * support for just "INSERT". Use POST instead for that purpose.
     */
    @Override
    protected void doPut(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (log.isInfoEnabled())
            log.info(req.toString());

        m_updateServlet.doPut(req, resp);

    }

    /**
     * Delegate to the {@link DeleteServlet}.
     */
    @Override
    protected void doDelete(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        if (log.isInfoEnabled())
            log.info(req.toString());

        m_deleteServlet.doDelete(req, resp);

    }

}
