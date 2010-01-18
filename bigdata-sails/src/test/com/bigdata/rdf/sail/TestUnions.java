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

/**
 * Unit tests the UNION aspects of the {@link BigdataSail} implementation.
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestUnions extends QuadsTestCase {

    /**
     * 
     */
    public TestUnions() {
    }

    /**
     * @param arg0
     */
    public TestUnions(String arg0) {
        super(arg0);
    }

    /**
     * The dc10: namespace.
     */
    final String DC10 = "http://purl.org/dc/elements/1.0/";
    
    /**
     * The dc11: namespace.
     */
    final String DC11 = "http://purl.org/dc/elements/1.1/";
    
    /**
     * dc10:title
     */
    final URI DC10_TITLE = new URIImpl(DC10+"title"); 
    
    /**
     * dc10:creator
     */
    final URI DC10_CREATOR = new URIImpl(DC10+"creator"); 
    
    /**
     * dc11:title
     */
    final URI DC11_TITLE = new URIImpl(DC11+"title"); 
    
    /**
     * dc11:creator
     */
    final URI DC11_CREATOR = new URIImpl(DC11+"creator"); 
    
    /**
     * Tests mapping of UNIONS in SPARQL onto unions in bigdata rules.
     * 
     * @throws Exception 
     */
    public void testUnions() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final BNode a = new BNodeImpl("_:a");
            final BNode b = new BNodeImpl("_:b");
            final BNode c = new BNodeImpl("_:c");
/**/            
            cxn.add(
                    a,
                    DC10_TITLE,
                    new LiteralImpl("A")
                    );
            cxn.add(
                    a,
                    DC10_CREATOR,
                    new LiteralImpl("A")
                    );
            cxn.add(
                    b,
                    DC11_TITLE,
                    new LiteralImpl("B")
                    );
            cxn.add(
                    b,
                    DC11_CREATOR,
                    new LiteralImpl("B")
                    );
            cxn.add(
                    c,
                    DC10_TITLE,
                    new LiteralImpl("C")
                    );
            cxn.add(
                    c,
                    DC11_CREATOR,
                    new LiteralImpl("C")
                    );
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
                    "SELECT ?title ?creator " +
                    "WHERE { " +
                    "  { ?book <"+DC10_TITLE+"> ?title . " +
                    "    ?book <"+DC10_CREATOR+"> ?creator . " +
                    "  } " +
                    "  UNION " +
                    "  { ?book <"+DC11_TITLE+"> ?title ." +
                    "    ?book <"+DC11_CREATOR+"> ?creator . " +
                    "  } " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
                TupleQueryResult result = tupleQuery.evaluate();
    
                Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(
                        new BindingImpl("title", new LiteralImpl("A")),
                        new BindingImpl("creator", new LiteralImpl("A"))
                        ));
                answer.add(createBindingSet(
                        new BindingImpl("title", new LiteralImpl("B")),
                        new BindingImpl("creator", new LiteralImpl("B"))
                        ));
                
                compare(result, answer);

            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
