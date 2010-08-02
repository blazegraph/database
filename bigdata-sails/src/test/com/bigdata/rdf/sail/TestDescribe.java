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

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.BindingImpl;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestDescribe extends ProxyBigdataSailTestCase {

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
    public TestDescribe() {
    }

    /**
     * @param arg0
     */
    public TestDescribe(String arg0) {
        super(arg0);
    }

    public void testSingleDescribe() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            URI mike = new URIImpl("_:Mike");
            URI person = new URIImpl("_:Person");
            URI likes = new URIImpl("_:likes");
            URI rdf = new URIImpl("_:RDF");
            Literal label = new LiteralImpl("Mike");
/**/
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, likes, rdf);
            cxn.add(mike, RDFS.LABEL, label);
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
//                    "describe ?x " +
//                    "WHERE { " +
//                    "  ?x <"+RDF.TYPE+"> <"+person+"> . " +
//                    "}";
                   "describe <"+mike+">";
//                    "construct { " +
//                    "  <"+mike+"> ?p1 ?o . " +
//                    "  ?s ?p2 <"+mike+"> . " +
//                    "} " +
//                    "where { " +
//                    "  { <"+mike+"> ?p1 ?o . } " +
//                    "  UNION " +
//                    "  { ?s ?p2 <"+mike+"> . } " +
//                    "}";
                    
                    
                
/*                
                construct {
                    ?s ?p ?o .
                }
                where {
                    ?x type Person .
                    ?s ?p ?o .
                    FILTER(?s == ?x || ?o == ?x) .
                }
                
                construct { 
                    ?x ?p1 ?o . 
                    ?s ?p2 ?x . 
                }
                where {
                    { ?x type Person . ?x ?p1 ?o . }
                    union
                    { ?x type Person . ?s ?p2 ?x . }
                }
*/                
                final BigdataSailGraphQuery graphQuery = (BigdataSailGraphQuery) 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                System.err.println(tupleExpr);
                
                while(result.hasNext()) {
                    Statement s = result.next();
                    System.err.println(s);
                }
            }
            
            {
                
                String query = 
                    "construct { " +
                    "  ?x ?px1 ?ox . " +
                    "  ?sx ?px2 ?x . " +
                    "} " +
                    "WHERE { " +
                    "  ?x <"+RDF.TYPE+"> <"+person+"> . " +
                    "  OPTIONAL { ?x ?px1 ?ox . } . " +
                    "  OPTIONAL { ?sx ?px2 ?x . } . " +
                    "}";
                
/*                
                construct {
                    ?s ?p ?o .
                }
                where {
                    ?x type Person .
                    ?s ?p ?o .
                    FILTER(?s == ?x || ?o == ?x) .
                }
                
                construct { 
                    ?x ?p1 ?o . 
                    ?s ?p2 ?x . 
                }
                where {
                    { ?x type Person . ?x ?p1 ?o . }
                    union
                    { ?x type Person . ?s ?p2 ?x . }
                }
*/                
                final BigdataSailGraphQuery graphQuery = (BigdataSailGraphQuery) 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                System.err.println(tupleExpr);
                
                while(result.hasNext()) {
                    Statement s = result.next();
                    System.err.println(s);
                }
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testMultiDescribe() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            URI mike = new URIImpl("_:Mike");
            URI person = new URIImpl("_:Person");
            URI likes = new URIImpl("_:likes");
            URI rdf = new URIImpl("_:RDF");
            URI thing = new URIImpl("_:Thing");
            Literal l1 = new LiteralImpl("Mike");
            Literal l2 = new LiteralImpl("RDF");
/**/
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, RDFS.LABEL, l1);
            cxn.add(mike, likes, rdf);
            cxn.add(rdf, RDF.TYPE, thing);
            cxn.add(rdf, RDFS.LABEL, l2);
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
                    "describe ?x ?y " +
                    "WHERE { " +
                    "  ?x <"+likes+"> ?y . " +
                    "}";
                
/*                
                construct {
                    ?s ?p ?o .
                }
                where {
                    ?x likes ?y .
                    ?s ?p ?o .
                    FILTER(?s == ?x || ?o == ?x || ?s == ?y || ?o == ?y) .
                }
                
                construct { 
                    ?x ?px1 ?ox . 
                    ?sx ?px2 ?x . 
                    ?y ?py1 ?oy . 
                    ?sy ?py2 ?y . 
                }
                where {
                    ?x likes ?y . 
                    OPTIONAL { ?x ?px1 ?ox . } .
                    OPTIONAL { ?sx ?px2 ?x . } .
                    OPTIONAL { ?y ?py1 ?oy . } .
                    OPTIONAL { ?sy ?py2 ?y . } .
                }
*/                
                final BigdataSailGraphQuery graphQuery = (BigdataSailGraphQuery) 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                System.err.println(tupleExpr);
                
                while(result.hasNext()) {
                    Statement s = result.next();
                    System.err.println(s);
                }
            }
            
            {
                
                String query = 
//                    "construct {" +
//                    "  ?x ?px1 ?ox . " + 
//                    "  ?sx ?px2 ?x . " + 
//                    "  ?y ?py1 ?oy . " + 
//                    "  ?sy ?py2 ?y . " +
//                    "} " +
                    "SELECT * " +
                    "WHERE { " +
                    "  ?x <"+likes+"> ?y . " +
                    "  OPTIONAL { ?x ?px1 ?ox . } . " +
                    "  OPTIONAL { ?sx ?px2 ?x . } . " +
                    "  OPTIONAL { ?y ?py1 ?oy . } . " +
                    "  OPTIONAL { ?sy ?py2 ?y . } . " +
                    "}";
                
/*                
                construct {
                    ?s ?p ?o .
                }
                where {
                    ?x likes ?y .
                    ?s ?p ?o .
                    FILTER(?s == ?x || ?o == ?x || ?s == ?y || ?o == ?y) .
                }
                
                construct { 
                    ?x ?px1 ?ox . 
                    ?sx ?px2 ?x . 
                    ?y ?py1 ?oy . 
                    ?sy ?py2 ?y . 
                }
                where {
                    ?x likes ?y . 
                    OPTIONAL { ?x ?px1 ?ox . } .
                    OPTIONAL { ?sx ?px2 ?x . } .
                    OPTIONAL { ?y ?py1 ?oy . } .
                    OPTIONAL { ?sy ?py2 ?y . } .
                }
*/              
/*                
                final BigdataSailGraphQuery graphQuery = (BigdataSailGraphQuery) 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                System.err.println(tupleExpr);
                
                while(result.hasNext()) {
                    Statement s = result.next();
                    System.err.println(s);
                }
*/
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                final TupleQueryResult result = tupleQuery.evaluate();
                
                while(result.hasNext()) {
                    BindingSet bs = result.next();
                    System.err.println(bs);
                }
                
            }
            
            {
                
                String query = 
//                    "construct {" +
//                    "  ?x ?px1 ?ox . " + 
//                    "  ?sx ?px2 ?x . " + 
//                    "  ?y ?py1 ?oy . " + 
//                    "  ?sy ?py2 ?y . " +
//                    "} " +
                    "SELECT * " +
                    "WHERE { " +
                    "  { ?x <"+likes+"> ?y . ?x ?px1 ?ox . } " +
                    "  UNION " +
                    "  { ?x <"+likes+"> ?y . ?sx ?px2 ?x . } " +
                    "  UNION " +
                    "  { ?x <"+likes+"> ?y . ?y ?py1 ?oy . } " +
                    "  UNION " +
                    "  { ?x <"+likes+"> ?y . ?sy ?py2 ?y . } " +
                    "}";
                
/*                
                construct {
                    ?s ?p ?o .
                }
                where {
                    ?x likes ?y .
                    ?s ?p ?o .
                    FILTER(?s == ?x || ?o == ?x || ?s == ?y || ?o == ?y) .
                }
                
                construct { 
                    ?x ?px1 ?ox . 
                    ?sx ?px2 ?x . 
                    ?y ?py1 ?oy . 
                    ?sy ?py2 ?y . 
                }
                where {
                    ?x likes ?y . 
                    OPTIONAL { ?x ?px1 ?ox . } .
                    OPTIONAL { ?sx ?px2 ?x . } .
                    OPTIONAL { ?y ?py1 ?oy . } .
                    OPTIONAL { ?sy ?py2 ?y . } .
                }
*/
/*                
                final BigdataSailGraphQuery graphQuery = (BigdataSailGraphQuery) 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, query);
                GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                System.err.println(tupleExpr);
                
                while(result.hasNext()) {
                    Statement s = result.next();
                    System.err.println(s);
                }
*/
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                final TupleQueryResult result = tupleQuery.evaluate();
                
                while(result.hasNext()) {
                    BindingSet bs = result.next();
                    System.err.println(bs);
                }
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
