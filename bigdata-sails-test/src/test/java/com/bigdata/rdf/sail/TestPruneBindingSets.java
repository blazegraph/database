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

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestPruneBindingSets extends ProxyBigdataSailTestCase {

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestPruneBindingSets() {
    }

    /**
     * @param arg0
     */
    public TestPruneBindingSets(String arg0) {
        super(arg0);
    }

    /**
     * Tests adding query hints in SPARQL.
     * 
     * @throws Exception 
     */
    public void testPruneBindingSets() throws Exception {

        final BigdataSail sail = getSail();
        
        try {
    
            sail.initialize();
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            final BigdataSailRepositoryConnection cxn = 
                (BigdataSailRepositoryConnection) repo.getConnection();
            try {
            cxn.setAutoCommit(false);

            URI x = new URIImpl("_:X");
            URI a = new URIImpl("_:A");
            URI b = new URIImpl("_:B");
            URI c = new URIImpl("_:C");
            URI d = new URIImpl("_:D");
            URI e = new URIImpl("_:E");
/**/
            cxn.add(a, x, b);
            cxn.add(b, x, c);
            cxn.add(c, x, d);
            cxn.add(d, x, e);
/**/

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
//            cxn.flush();//commit();
            cxn.commit();
            
/**/            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }

            {
                
                String query = 
                    "select ?a " +
                    "WHERE { " +
                    "  ?a <"+x+"> ?b ." +
                    "  ?b <"+x+"> ?c ." +
                    "  ?c <"+x+"> ?d ." +
                    "  ?d <"+x+"> <"+e+"> ." +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("a", a)
                }));
                
                compare(result, solution);
                
            }
            } finally {
                
                cxn.close();
            }
            
        } finally {
            sail.__tearDownUnitTest();
        }

    }
    
}
