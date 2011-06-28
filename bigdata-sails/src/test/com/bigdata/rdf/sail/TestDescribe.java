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

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.algebra.TupleExpr;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 * 
 * FIXME These unit tests do not verify the expected result.
 */
public class TestDescribe extends ProxyBigdataSailTestCase {

	private static Logger log = Logger.getLogger(TestDescribe.class);
	
    @Override
    public Properties getProperties() {
        
        final Properties props = super.getProperties();
        
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
        try {
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final URI mike = new URIImpl(BD.NAMESPACE+"Mike");
            final URI bryan = new URIImpl(BD.NAMESPACE+"Bryan");
            final URI person = new URIImpl(BD.NAMESPACE+"Person");
            final URI likes = new URIImpl(BD.NAMESPACE+"likes");
            final URI rdf = new URIImpl(BD.NAMESPACE+"RDF");
            final URI rdfs = new URIImpl(BD.NAMESPACE+"RDFS");
            final Literal label1 = new LiteralImpl("Mike");
            final Literal label2 = new LiteralImpl("Bryan");
/**/
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, likes, rdf);
            cxn.add(mike, RDFS.LABEL, label1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, likes, rdfs);
            cxn.add(bryan, RDFS.LABEL, label2);
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
                
                final String query = 
                	"prefix bd: <"+BD.NAMESPACE+"> " +
                	"prefix rdf: <"+RDF.NAMESPACE+"> " +
                	"prefix rdfs: <"+RDFS.NAMESPACE+"> " +

                    "describe ?x " +
                    "WHERE { " +
//                    "  { " +
                    "  ?x rdf:type bd:Person . " +
                    "  ?x bd:likes bd:RDF " +
//                    "  } union { " +
//                    "  ?x rdf:type bd:Person . " +
//                    "  ?x bd:likes bd:RDFS " +
//                    "  } " +
                    "}";
//                   "describe <"+mike+">";
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
                final GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                if(log.isInfoEnabled())
                    log.info(tupleExpr);
                
                while(result.hasNext()) {
                    final Statement s = result.next();
                    if(log.isInfoEnabled())
                        log.info(s);
                }
            }
            
            {
                
                final String query = 
                    "construct { " + 
                    "  ?x ?p1 ?o . " + 
                    "  ?s ?p2 ?x . " + 
                    "} " +
                    "WHERE { " +
                    "  ?x <"+RDF.TYPE+"> <"+person+"> . " +
                    "  {" +
                    "    ?x ?p1 ?ox . " +
                    "  } UNION {" +
                    "    ?sx ?p2 ?x . " +
                    "  } " +
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
                final GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                if(log.isInfoEnabled())
                        log.info(tupleExpr);
                
                while(result.hasNext()) {
                    final Statement s = result.next();
                    if(log.isInfoEnabled())
                        log.info(s);
                }
                
            }
            
        } finally {
            
            cxn.close();
            
        }
        } finally {
            sail.__tearDownUnitTest();
        }

    }
    
    public void testMultiDescribe() throws Exception {

        final BigdataSail sail = getSail();
        try {
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final URI mike = new URIImpl("_:Mike");
            final URI person = new URIImpl("_:Person");
            final URI likes = new URIImpl("_:likes");
            final URI rdf = new URIImpl("_:RDF");
            final URI thing = new URIImpl("_:Thing");
            final Literal l1 = new LiteralImpl("Mike");
            final Literal l2 = new LiteralImpl("RDF");
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
                
                final String query = 
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
                final GraphQueryResult result = graphQuery.evaluate();
                
                final TupleExpr tupleExpr = graphQuery.getTupleExpr();
                if(log.isInfoEnabled())
                    log.info(tupleExpr);
                
                while(result.hasNext()) {
                    final Statement s = result.next();
                    if(log.isInfoEnabled())
                        log.info(s);
                }
            }
            
        } finally {
            cxn.close();
        }
        } finally {
            sail.__tearDownUnitTest();
        }

    }
    
}
