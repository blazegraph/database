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
package com.bigdata.blueprints.webapp;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.bigdata.blueprints.BigdataGraphBulkLoad;
import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.AbstractRestApiTask;
import com.bigdata.rdf.sail.webapp.BigdataRDFServlet;
import com.bigdata.rdf.sail.webapp.BlueprintsServletProxy;
import com.bigdata.rdf.sail.webapp.client.MiniMime;
import com.tinkerpop.blueprints.util.io.graphml.GraphMLReader;

/**
 * Helper servlet for the blueprints layer.
 */
public class BlueprintsServlet extends BlueprintsServletProxy {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    
    static private final transient Logger log = Logger.getLogger(BlueprintsServlet.class); 

    static public final List<String> mimeTypes = Arrays.asList(new String[] {
        "application/graphml+xml"    
    }) ;
    
    public BlueprintsServlet() {

    }

    /**
     * Post a GraphML file to the blueprints layer.
     */
    @Override
    protected void doPost(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        
        final String contentType = req.getContentType();

        if (log.isInfoEnabled())
            log.info("Request body: " + contentType);

        final String mimeType = new MiniMime(contentType).getMimeType().toLowerCase();

        if (!mimeTypes.contains(mimeType)) {

            buildAndCommitResponse(resp, HTTP_BADREQUEST, MIME_TEXT_PLAIN,
                    "Content-Type not recognized as graph data: " + contentType);

            return;

        }
        
        try {
            
            submitApiTask(
                    new BlueprintsPostTask(req, resp, getNamespace(req),
                    		ITx.UNISOLATED)).get();

        } catch (Throwable t) {

            BigdataRDFServlet.launderThrowable(t, resp, "");
            
        }
        
    }

    private static class BlueprintsPostTask extends AbstractRestApiTask<Void> {

        public BlueprintsPostTask(HttpServletRequest req,
                HttpServletResponse resp, String namespace, long timestamp) {

            super(req, resp, namespace, timestamp);

        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public Void call() throws Exception {

            final long begin = System.currentTimeMillis();
            
            BigdataSailRepositoryConnection conn = null;
            boolean success = false;
            try {

                conn = getConnection();
                
                final BigdataGraphBulkLoad graph = new BigdataGraphBulkLoad(conn);

                GraphMLReader.inputGraph(graph, req.getInputStream());
                
                graph.commit();
                
                success = true;
                
                final long nmodified = graph.getMutationCountLastCommit();

                final long elapsed = System.currentTimeMillis() - begin;
                
                reportModifiedCount(nmodified, elapsed);

                // Done.
                return null;

            } finally {

                if (conn != null) {

                    if (!success)
                        conn.rollback();

                    conn.close();

                }

            }

        }

    }

    /**
     * 
     * Convenience method to access doPost from a public method.
     * 
     * @param req
     * @param resp
     * @throws IOException 
     */
	public void doPostRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException {

		doPost(req, resp);
		
	}

}
