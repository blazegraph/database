/**
Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Sep 16, 2009
 */

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import com.bigdata.rdf.store.BD;

/**
 * Unit tests the query hints aspect of the {@link BigdataSail} implementation.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestQueryHints extends QuadsTestCase {

    /**
     * 
     */
    public TestQueryHints() {
    }

    /**
     * @param arg0
     */
    public TestQueryHints(String arg0) {
        super(arg0);
    }

    /**
     * Tests adding query hints in SPARQL.
     * 
     * @throws Exception 
     */
    public void testQueryHints() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            URI a = new URIImpl("_:A");
            URI b = new URIImpl("_:B");
            URI c = new URIImpl("_:C");
/**/
            cxn.add(a, b, c);
/**/

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
                String query = 
                    "PREFIX "+BD.QUERY_HINTS_NAMESPACE+": " +
                    "  <http://www.bigdata.com/queryOption#com.bigdata.relation.rule.eval.DefaultRuleTaskFactory.nestedSubquery=true&com.bigdata.fullScanTreshold=1000> " +
                    "SELECT * " +
                    "WHERE { " +
                    "  <"+a+"> ?p ?o " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
    
                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(
                        new BindingImpl("p", b),
                        new BindingImpl("o", c)
                        ));
                
                compare(result, answer);

            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
