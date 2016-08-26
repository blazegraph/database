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
/*
 * Created on Mar 29, 2011
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.MapBindingSet;
import org.openrdf.repository.RepositoryException;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for the logic which rewrites a query, replacing {@link Value}
 * constants with {@link BigdataValue} constants which have been resolved
 * against the database.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestBigdataValueReplacer extends ProxyBigdataSailTestCase {

    /**
     * 
     */
    public TestBigdataValueReplacer() {
    }

    /**
     * @param name
     */
    public TestBigdataValueReplacer(String name) {
        super(name);
    }

    @Override
    public Properties getProperties() {
        
//        Logger.getLogger(BigdataValueReplacer.class).setLevel(Level.ALL);
        
        final Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * Unit test for bindings passed into a query which are not used by the
     * query.
     * 
     * @throws RepositoryException
     * @throws SailException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/271
     */
    public void test_dropUnusedBindings() throws RepositoryException,
            SailException, MalformedQueryException, QueryEvaluationException {

        final BigdataSail sail = getSail();

        try {

            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            final BigdataSailRepositoryConnection cxn = (BigdataSailRepositoryConnection) repo
                    .getConnection();
            try {

                cxn.setAutoCommit(false);

                /*
                 * Add a statement so the query does not get short circuited
                 * because some of the terms in the query are undefined in the
                 * database.
                 */
                cxn.add(new URIImpl("s:1"), new URIImpl("p:1"), new URIImpl(
                        "s:2"));

                final String query = "select ?a ?b WHERE {?a <p:1> ?b}";

                final TupleQuery q = cxn.prepareTupleQuery(
                        QueryLanguage.SPARQL, query);
                
                /*
                 * Setup some bindings.  
                 */
                // bind to a term in the database.
                q.setBinding("a", new URIImpl("s:1"));
                // bind to a term in the database.
                q.setBinding("b", new URIImpl("s:2"));
                // bind to a term NOT found in the database.
                q.setBinding("notused", new LiteralImpl("lit"));
                
                /*
                 * Evaluate the query and verify that the correct solution
                 * is produced.
                 */
                final Collection<BindingSet> expected = new LinkedList<BindingSet>();
                {
                    final MapBindingSet bset = new MapBindingSet();
                    bset.addBinding("a", new URIImpl("s:1"));
                    bset.addBinding("b", new URIImpl("s:2"));
                    expected.add(bset);
                }
                final TupleQueryResult result = q.evaluate();
                try {
                    compare(result, expected);
                } finally {
                    result.close();
                }

            } finally {
            
                cxn.close();
                
            }

        } finally {
            
            sail.__tearDownUnitTest();

        }

    }

}
