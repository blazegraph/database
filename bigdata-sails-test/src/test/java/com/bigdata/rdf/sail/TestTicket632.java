/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2011.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;

import com.bigdata.rdf.sail.BigdataSail.Options;
import com.bigdata.rdf.sparql.ast.eval.service.OpenrdfNativeMockServiceFactory;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;

/**
 * Test for an error that occurs when a SERVICE (OpenRdf Service) call uses
 * variables that are assigned as input bindings to the query that holds the
 * SERVICE call.
 * <p>
 * To run this test case, specify the following JVM property:
 * <code>-DtestClass=com.bigdata.rdf.sail.TestBigdataSailWithQuads</code>
 */
public class TestTicket632 extends QuadsTestCase {

    public TestTicket632() {
    }

    public TestTicket632(String arg0) {
        super(arg0);
    }

    public void testServiceWithBindingArg() throws Exception {
        final URI serviceURI = new URIImpl("http://www.bigdata.com/mockService/" + getName());
        //the service solutions don't matter cause the error is from before computing the service solutions
        final List<BindingSet> serviceSolutions = new LinkedList<BindingSet>();
        ServiceRegistry.getInstance().add(serviceURI, new OpenrdfNativeMockServiceFactory(serviceSolutions));
        final BigdataSail sail = getSail();
        try {
            executeQuery(serviceURI, new BigdataSailRepository(sail));
        } finally {            
            ServiceRegistry.getInstance().remove(serviceURI);
            sail.__tearDownUnitTest();
        }
    }

    private void executeQuery(final URI serviceUri, final SailRepository repo) throws RepositoryException, MalformedQueryException, QueryEvaluationException, RDFParseException, IOException, RDFHandlerException {
        try {
            repo.initialize();
            final RepositoryConnection conn = repo.getConnection();
            final ValueFactory vf = conn.getValueFactory();
            conn.setAutoCommit(false);
            try {
                final String query = "SELECT ?x { SERVICE <" + serviceUri.stringValue() + "> { ?x <u:1> ?bool1 } }";
                final TupleQuery q = conn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                q.setBinding("bool1", vf.createLiteral(true));
                final TupleQueryResult tqr = q.evaluate();
                try {
                    tqr.hasNext();
                } finally {
                    tqr.close();
                }
            } finally {
                conn.close();
            }
        } finally {
            repo.shutDown();
        }
    }
    
    @Override
    public Properties getProperties() {
        
        final Properties properties = getOurDelegate().getProperties();
        
        properties.setProperty(Options.NAMESPACE, "freshNamespace-"+UUID.randomUUID());
        
        return properties;
    }

}
