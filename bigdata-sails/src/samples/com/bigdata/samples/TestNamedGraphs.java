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
 * Created on Sep 18, 2009
 */

package com.bigdata.samples;

import java.util.HashSet;
import java.util.Properties;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.DatasetImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;

import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestNamedGraphs extends SampleCode {

    /**
     * Load all data from some directory.
     * 
     * @param dir
     * 
     * @throws Exception
     */
    public void test() throws Exception {

        final Properties properties = loadProperties("quads.properties");

        BigdataSail sail = new BigdataSail(properties);
        BigdataSailRepository repo = new BigdataSailRepository(sail);
        repo.initialize();

        try {

//            final RepositoryConnection cxn = repo.getConnection();
            final RepositoryConnection cxn = repo.getQueryConnection();
            
            // fast range count!
            long stmtCount = sail.getDatabase().getStatementCount();
            System.err.println("Statement Count: " + stmtCount);

            RepositoryResult<Resource> graphs = cxn.getContextIDs();
            HashSet<URI> ngs = new HashSet<URI>();
            while (graphs.hasNext()) {
                Resource g = graphs.next();
                if (g instanceof URI) {
                    ngs.add((URI) g);
                }
            }
            System.err.println("graphCount: " + ngs.size());
            TupleQuery actorQuery = cxn
                    .prepareTupleQuery(
                            QueryLanguage.SPARQL,
                            "SELECT "
                                    + "DISTINCT"
                                    + " ?actID ?actor  WHERE {"
                                    + " ?movie a <http://cambridgesemantics.com/ontologies/2009/08/Film#Movie>. "
                                    + " ?movie <http://cambridgesemantics.com/ontologies/2009/08/Film#performance> ?actID ."
                                    + " ?actID <http://cambridgesemantics.com/ontologies/2009/08/Film#mpName> ?actor ."
                                    + " ?actID a <http://cambridgesemantics.com/ontologies/2009/08/Film#Actor>"
                                    + "}");
            DatasetImpl ds = new DatasetImpl();
            for (URI g : ngs) {
                ds.addDefaultGraph(g);
            }
            actorQuery.setDataset(ds);
            actorQuery.setIncludeInferred(false /* includeInferred */);
            long start = System.currentTimeMillis();
            TupleQueryResult rs2 = actorQuery.evaluate();
            int size = 0;

            while (rs2.hasNext()) {
                rs2.next();
                size++;
                if (size % 100 == 0) {
                    System.err.println(size);
                }
            }
            System.err.println("Query time/size:"
                    + (System.currentTimeMillis() - start) + "/" + size);

            cxn.close();

        } finally {

            sail.shutDown();

        }

    }

    /**
     * Loads a bunch of data from a zip file.
     * 
     * @param args
     *            The file name.
     * 
     * @throws Exception
     */
    public static void main(String[] args) {

        try {

            new TestNamedGraphs().test();

        } catch (Exception ex) {

            ex.printStackTrace(System.err);

        }

    }

}
