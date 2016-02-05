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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestOrderBy extends ProxyBigdataSailTestCase {

    @Override
    public Properties getProperties() {
        
        final Properties props = super.getProperties();

        props.setProperty(BigdataSail.Options.ISOLATABLE_INDICES, "true");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestOrderBy() {
    }

    /**
     * @param arg0
     */
    public TestOrderBy(String arg0) {
        super(arg0);
    }

    
    public void testOrderBy() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
//          This fails with BigData trunk of 21-07-2010
            final URI s1 = vf.createURI("s:1");
            final URI s2 = vf.createURI("s:2");
            final URI s3 = vf.createURI("s:3");
            final URI pred1 = vf.createURI("p:1");
            final URI pred2 = vf.createURI("p:2");
            cxn.add(s1, pred1, vf.createLiteral(3));
            cxn.add(s1, pred2, vf.createLiteral("a"));
            cxn.add(s2, pred1, vf.createLiteral(1));
            cxn.add(s2, pred2, vf.createLiteral("b"));
            cxn.add(s3, pred1, vf.createLiteral(2));
            cxn.add(s3, pred2, vf.createLiteral("c"));
            final TupleQuery tq = cxn.prepareTupleQuery(QueryLanguage.SPARQL, 
                    "SELECT ?s ?lit " +
                    "WHERE { " +
                    "  ?s <p:1> ?val. " +
                    "  ?s <p:2> ?lit " +
                    "} " +
                    "ORDER BY ?val"
                    );
            final TupleQueryResult result = tq.evaluate();
            try {
                assertTrue(result.hasNext());
                assertEquals(s2, result.next().getValue("s"));
                assertTrue(result.hasNext());
                assertEquals(s3, result.next().getValue("s"));
                assertTrue(result.hasNext());
                assertEquals(s1, result.next().getValue("s"));
                assertFalse(result.hasNext());
            } finally {
                result.close();
            }

        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
