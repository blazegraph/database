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

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.query.parser.QueryParserUtil;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

import info.aduna.iteration.CloseableIteration;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestSingleTailRule extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestSingleTailRule.class);
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();
        
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.VOCABULARY_CLASS, NoVocabulary.class.getName());
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "true");
        
        return props;
        
    }

    /**
     * 
     */
    public TestSingleTailRule() {
    }

    /**
     * @param arg0
     */
    public TestSingleTailRule(String arg0) {
        super(arg0);
    }

    public void testSingleTail() throws Exception {

        final BigdataSail sail = getSail();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        repo.initialize();
        final BigdataSailRepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            URI mike = vf.createURI(ns+"Mike");
            URI likes = vf.createURI(ns+"likes");
            URI rdf = vf.createURI(ns+"RDF");
/**/
            cxn.setNamespace("ns", ns);
            
            testValueRoundTrip(cxn.getSailConnection(), mike, likes, rdf);
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }
            
        } finally {
            cxn.close();
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();
        }

    }
    
    public void testSingleTailSearch() throws Exception {

        final BigdataSail sail = getSail();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        repo.initialize();
        final BigdataSailRepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            final URI mike = vf.createURI(ns+"Mike");
//            URI likes = vf.createURI(ns+"likes");
//            URI rdf = vf.createURI(ns+"RDF");
            final Literal l1 = vf.createLiteral("Mike");
/**/
            cxn.setNamespace("ns", ns);

            cxn.add(mike, RDFS.LABEL, l1);
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }
            
            {
                
                final String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select ?s ?p ?o " +
                    "WHERE { " +
                    "  ?s ?p ?o . " +
                    "  filter(?p = <"+RDFS.LABEL+">) " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                final TupleQueryResult result = tupleQuery.evaluate();
                
//                while (result.hasNext()) {
//                    System.err.println(result.next());
//                }
 
                final Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", mike),
                    new BindingImpl("p", RDFS.LABEL),
                    new BindingImpl("o", l1),
                }));
                
                compare(result, solution);
                
            }
            
            {
                
                final String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select ?s " +
                    "WHERE { " +
                    "  ?s ns:search \"Mike\" . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                final TupleQueryResult result = tupleQuery.evaluate();
                
//                while (result.hasNext()) {
//                    System.err.println(result.next());
//                }
 
                final Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", l1),
                }));
                
                compare(result, solution);
                
            }
            
            
        } finally {
            cxn.close();
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();
        }

    }
    
    private void testValueRoundTrip(final SailConnection con, 
            final Resource subj, final URI pred, final Value obj)
        throws Exception
    {
        con.addStatement(subj, pred, obj);
        con.commit();
    
        CloseableIteration<? extends Statement, SailException> stIter = 
            con.getStatements(null, null, null, false);
    
        try {
            assertTrue(stIter.hasNext());
    
            Statement st = stIter.next();
            assertEquals(subj, st.getSubject());
            assertEquals(pred, st.getPredicate());
            assertEquals(obj, st.getObject());
            assertTrue(!stIter.hasNext());
        }
        finally {
            stIter.close();
        }
    
//        ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SERQL,
//                "SELECT S, P, O FROM {S} P {O} WHERE P = <" + pred.stringValue() + ">", null);
        ParsedTupleQuery tupleQuery = QueryParserUtil.parseTupleQuery(QueryLanguage.SPARQL,
                "SELECT ?S ?P ?O " +
                "WHERE { " +
                "  ?S ?P ?O . " +
                "  FILTER( ?P = <" + pred.stringValue() + "> ) " +
                "}", null);
    
        CloseableIteration<? extends BindingSet, QueryEvaluationException> iter;
        iter = con.evaluate(tupleQuery.getTupleExpr(), null, EmptyBindingSet.getInstance(), false);
    
        try {
            assertTrue(iter.hasNext());
    
            BindingSet bindings = iter.next();
            assertEquals(subj, bindings.getValue("S"));
            assertEquals(pred, bindings.getValue("P"));
            assertEquals(obj, bindings.getValue("O"));
            assertTrue(!iter.hasNext());
        }
        finally {
            iter.close();
        }
    }

    public void testOptionalFilter()
        throws Exception
    {
        final BigdataSail sail = getSail();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
//        final Sail sail = new MemoryStore();
//        final Repository repo = new SailRepository(sail);
        
        repo.initialize();
        final RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            URI s = vf.createURI("urn:test:s");
            URI p1 = vf.createURI("urn:test:p1");
            URI p2 = vf.createURI("urn:test:p2");
            Literal v1 = vf.createLiteral(1);
            Literal v2 = vf.createLiteral(2);
            Literal v3 = vf.createLiteral(3);
            cxn.add(s, p1, v1);
            cxn.add(s, p2, v2);
            cxn.add(s, p1, v3);
            cxn.commit();
            
            String qry = 
                "PREFIX :<urn:test:> " +
                "SELECT ?s ?v1 ?v2 " +
                "WHERE { " +
                "  ?s :p1 ?v1 . " +
                "  OPTIONAL {?s :p2 ?v2 FILTER(?v1 < 3) } " +
                "}";
            
            TupleQuery query = cxn.prepareTupleQuery(QueryLanguage.SPARQL, qry);
            TupleQueryResult result = query.evaluate();
            
//            while (result.hasNext()) {
//                System.err.println(result.next());
//            }
            
            Collection<BindingSet> solution = new LinkedList<BindingSet>();
            solution.add(createBindingSet(new Binding[] {
                new BindingImpl("s", s),
                new BindingImpl("v1", v1),
                new BindingImpl("v2", v2),
            }));
            solution.add(createBindingSet(new Binding[] {
                new BindingImpl("s", s),
                new BindingImpl("v1", v3),
            }));
            
            compare(result, solution);
            
        } finally {
            cxn.close();
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();
        }
            
    }

    
}
