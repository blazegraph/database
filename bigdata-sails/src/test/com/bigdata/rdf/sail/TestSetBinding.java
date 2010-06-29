/**
Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.io.InputStream;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Properties;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.rio.RDFFormat;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestSetBinding extends ProxyBigdataSailTestCase {

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "false");
        
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_TRANSITIVE_PROPERTY, "false"); 
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_SAMEAS_CLOSURE, "false"); 
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_SAMEAS_PROPERTIES, "false");
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_INVERSE_OF, "false");
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_EQUIVALENT_CLASS, "false"); 
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_EQUIVALENT_PROPERTY, "false");
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_OWL_HAS_VALUE, "false");
        props.setProperty(BigdataSail.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "false"); 

        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "true");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "true"); 
        
        props.setProperty(BigdataSail.Options.BLOOM_FILTER, "false");
        
        return props;
        
    }

    /**
     * 
     */
    public TestSetBinding() {
    }

    /**
     * @param arg0
     */
    public TestSetBinding(String arg0) {
        super(arg0);
    }

    public void testSetBinding() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        final ValueFactory vf = cxn.getValueFactory(); 
        
        try {
    
//          First step, load data.
            final String data = 
                "@prefix ns:<http://localhost/pets#>. " +
                "@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#>. " +
                "    ns:snowball rdfs:label \"Snowball\"; " +
                "                ns:weight \"10\". " +
                "    ns:buffy rdfs:label \"Buffy\"; " +
                "                ns:weight \"8\".";
            
            System.err.println("Loading data"); 
//            final ClassLoader cl = getClass().getClassLoader(); 
//            final InputStream is = cl.getResourceAsStream("data.ttl"); 
//            cxn.add(is, "", RDFFormat.TURTLE, new Resource[0]);
            cxn.add(new StringReader(data), "", RDFFormat.TURTLE, new Resource[0]);
            cxn.commit();
            
            RepositoryResult<Statement> stmts = 
                cxn.getStatements(null, null, null, false);
            while(stmts.hasNext()) {
                System.err.println(stmts.next());
            }
            
            // Second step, query data. Load query from resource and execute. 
            final String query = "PREFIX ns:<http://localhost/pets#> " 
                + "\nPREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>" 
                + "\nSELECT ?name ?weight WHERE {" 
                + "\n?uri rdfs:label ?name." + "\n?uri ns:weight ?weight." 
                + "\n}"; System.err.println("Executing query: " + query); 
                
            final TupleQuery q = cxn.prepareTupleQuery( QueryLanguage.SPARQL, query);
            
            Literal snowball = vf.createLiteral("Snowball");
            if (repo instanceof BigdataSailRepository) {
                long tid = repo.getDatabase().getTermId(snowball);
                ((BigdataLiteral) snowball).setTermId(tid);
                System.err.println(((BigdataLiteral) snowball).getTermId());
            }
            q.setBinding("name", snowball); 
            final TupleQueryResult res = q.evaluate(); 
            while (res.hasNext()) { 
                final BindingSet set = (BindingSet) res.next(); 
                System.err.println("Found: " + set.getValue("name") 
                        + " = " + set.getValue("weight")); 
            } 
            
            System.err.println("Done..."); 
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
