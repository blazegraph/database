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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.openrdf.OpenRDFException;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.rio.RDFFormat;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Test suite for ticket #1388: xsd:date function
 */
public class TestTicket1388 extends ProxyBigdataSailTestCase {

    public Properties getTriplesNoInference() {
        
        Properties props = super.getProperties();
        
        // triples with sids
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
        
        // no inference
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
    public TestTicket1388() {
    }

    /**
     * @param arg0
     */
    public TestTicket1388(String arg0) {
        super(arg0);
    }
    
    /**
     * When loading quads into a triple store, the context is striped away by
     * default.
     */
    public void testDateFunction() throws Exception {

        BigdataSailRepositoryConnection cxn = null;

        final BigdataSail sail = getSail(getTriplesNoInference());

        try {

            sail.initialize();
            
            final BigdataSailRepository repo = new BigdataSailRepository(sail);
            
            cxn = (BigdataSailRepositoryConnection) repo.getConnection();
            
            cxn.begin();
           
            try {
				InputStream is = TestTicket1388.class.getResourceAsStream("testTicket1388.n3");
				if (is == null) {
					throw new IOException("Could not locate resource: " + "testTicket1388.n3");
				}
				Reader reader = new InputStreamReader(new BufferedInputStream(is));
				try {
					cxn.add(reader, "", RDFFormat.N3);
				} finally {
					reader.close();
				}
				cxn.commit();
			} catch (OpenRDFException ex) {
				cxn.rollback();
				throw ex;
			}
            cxn.commit();

           final String query = "SELECT ?myDate (count(?doc) as ?countDoc) \n " + // 
       			"{ ?doc rdf:type <http://www.example.com/Document> . \n " + //
    			" ?doc <http://www.example.com/created> ?date . \n " + //
    			" filter((<http://www.w3.org/2001/XMLSchema#date>(?date)) >= \"2014-04-11\"^^<http://www.w3.org/2001/XMLSchema#date>) .\n " + //
    			" filter((<http://www.w3.org/2001/XMLSchema#date>(?date)) < \"2014-05-30\"^^<http://www.w3.org/2001/XMLSchema#date>) .\n " + //
    			" bind((<http://www.w3.org/2001/XMLSchema#date>(?date)) as ?myDate) .\n " + // 
    			"} group by ?myDate";
            

            final TupleQuery q = cxn.prepareTupleQuery(QueryLanguage.SPARQL,
						query);
			
			final TupleQueryResult tqr = q.evaluate();
			
			final Collection<BindingSet> solution = new LinkedList<BindingSet>();
			//[myDate="2014-04-11"^^<http://www.w3.org/2001/XMLSchema#date>;countDoc="3"^^<http://www.w3.org/2001/XMLSchema#integer>]
			//[myDate="2014-05-29"^^<http://www.w3.org/2001/XMLSchema#date>;countDoc="2"^^<http://www.w3.org/2001/XMLSchema#integer>]
            solution.add(createBindingSet(new Binding[] {
                new BindingImpl("myDate", new LiteralImpl("2014-04-11", XMLSchema.DATE)),
                new BindingImpl("countDoc", new LiteralImpl(Integer.toString(3), XMLSchema.INTEGER))
            }));
            solution.add(createBindingSet(new Binding[] {
                new BindingImpl("myDate", new LiteralImpl("2014-05-29", XMLSchema.DATE)),
                new BindingImpl("countDoc", new LiteralImpl(Integer.toString(2), XMLSchema.INTEGER))
            }));
                       
            compare(tqr, solution);
            
			tqr.close();
			
        } finally {
            if (cxn != null)
                cxn.close();
            sail.__tearDownUnitTest();
        }
    }

}
