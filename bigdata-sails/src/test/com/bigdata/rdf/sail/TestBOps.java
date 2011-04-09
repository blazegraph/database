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
import java.util.Properties;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.memory.MemoryStore;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestBOps extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestBOps.class);
    
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
    public TestBOps() {
    }

    /**
     * @param arg0
     */
    public TestBOps(String arg0) {
        super(arg0);
    }

    public void testSimpleJoin() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            URI mike = new URIImpl(ns+"Mike");
            URI bryan = new URIImpl(ns+"Bryan");
            URI person = new URIImpl(ns+"Person");
            URI likes = new URIImpl(ns+"likes");
            URI rdf = new URIImpl(ns+"RDF");
            Literal l1 = new LiteralImpl("Mike");
            Literal l2 = new LiteralImpl("Bryan");
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, likes, rdf);
            cxn.add(mike, RDFS.LABEL, l1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, likes, rdf);
            cxn.add(bryan, RDFS.LABEL, l2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            cxn.commit();//
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select * " +
                    "WHERE { " +
                    "  ?s rdf:type ns:Person . " +
                    "  ?s ns:likes ?likes . " +
                    "  ?s rdfs:label ?label . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
//                while (result.hasNext()) {
//                    System.err.println(result.next());
//                }
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", mike),
                    new BindingImpl("likes", rdf),
                    new BindingImpl("label", l1)
                }));
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", bryan),
                    new BindingImpl("likes", rdf),
                    new BindingImpl("label", l2)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testSimpleConstraint() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            URI jill = new URIImpl(ns+"Jill");
            URI jane = new URIImpl(ns+"Jane");
            URI person = new URIImpl(ns+"Person");
            URI age = new URIImpl(ns+"age");
            URI IQ = new URIImpl(ns+"IQ");
            Literal l1 = new LiteralImpl("Jill");
            Literal l2 = new LiteralImpl("Jane");
            Literal age1 = vf.createLiteral(20);
            Literal age2 = vf.createLiteral(30);
            Literal IQ1 = vf.createLiteral(130);
            Literal IQ2 = vf.createLiteral(140);
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(jill, RDF.TYPE, person);
            cxn.add(jill, RDFS.LABEL, l1);
            cxn.add(jill, age, age1);
            cxn.add(jill, IQ, IQ1);
            cxn.add(jane, RDF.TYPE, person);
            cxn.add(jane, RDFS.LABEL, l2);
            cxn.add(jane, age, age2);
            cxn.add(jane, IQ, IQ2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            cxn.commit();//
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select * " +
                    "WHERE { " +
                    "  ?s rdf:type ns:Person . " +
                    "  ?s ns:age ?age . " +
                    "  ?s ns:IQ ?iq . " +
                    "  ?s rdfs:label ?label . " +
                    "  FILTER( ?age < 25 && ?iq > 125 ) . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
//                while (result.hasNext()) {
//                    System.err.println(result.next());
//                }
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", jill),
                    new BindingImpl("age", age1),
                    new BindingImpl("iq", IQ1),
                    new BindingImpl("label", l1)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testSimpleOptional() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            final String ns = BD.NAMESPACE;
            
            URI mike = new URIImpl(ns+"Mike");
            URI bryan = new URIImpl(ns+"Bryan");
            URI person = new URIImpl(ns+"Person");
            URI likes = new URIImpl(ns+"likes");
            URI rdf = new URIImpl(ns+"RDF");
            Literal l1 = new LiteralImpl("Mike");
            Literal l2 = new LiteralImpl("Bryan");
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, likes, rdf);
            cxn.add(mike, RDFS.LABEL, l1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, likes, rdf);
//            cxn.add(bryan, RDFS.LABEL, l2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            cxn.commit();//
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select * " +
                    "WHERE { " +
                    "  ?s rdf:type ns:Person . " +
                    "  ?s ns:likes ?likes . " +
                    "  OPTIONAL { ?s rdfs:label ?label . } " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
//                while (result.hasNext()) {
//                    System.err.println(result.next());
//                }
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", mike),
                    new BindingImpl("likes", rdf),
                    new BindingImpl("label", l1)
                }));
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", bryan),
                    new BindingImpl("likes", rdf),
//                    new BindingImpl("label", l2)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testOrEquals() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
    
            final LexiconRelation lex = sail.getDatabase().getLexiconRelation();
            
            final String ns = BD.NAMESPACE;
            
            URI mike = new URIImpl(ns+"Mike");
            URI bryan = new URIImpl(ns+"Bryan");
            URI martyn = new URIImpl(ns+"Martyn");
            URI person = new URIImpl(ns+"Person");
            URI p = new URIImpl(ns+"p");
            Literal l1 = new LiteralImpl("Mike");
            Literal l2 = new LiteralImpl("Bryan");
            Literal l3 = new LiteralImpl("Martyn");
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(mike, RDF.TYPE, person);
            cxn.add(mike, RDFS.LABEL, l1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, RDFS.COMMENT, l2);
            cxn.add(martyn, RDF.TYPE, person);
            cxn.add(martyn, p, l3);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            cxn.commit();//
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
                String query = 
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select * " +
                    "WHERE { " +
                    "  ?s rdf:type ns:Person . " +
                    "  ?s ?p ?label . " +
                    "  FILTER ( ?p = rdfs:label || ?p = rdfs:comment ) . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
//                while (result.hasNext()) {
//                    System.err.println(result.next());
//                }
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", mike),
                    new BindingImpl("p", RDFS.LABEL),
                    new BindingImpl("label", l1)
                }));
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", bryan),
                    new BindingImpl("p", RDFS.COMMENT),
                    new BindingImpl("label", l2)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testHashJoin() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
    
            final LexiconRelation lex = sail.getDatabase().getLexiconRelation();
            
            final String ns = BD.NAMESPACE;
            
            URI mikeA = new URIImpl(ns+"MikeA");
            URI mikeB = new URIImpl(ns+"MikeB");
            URI bryan = new URIImpl(ns+"Bryan");
            URI martyn = new URIImpl(ns+"Martyn");
            URI person = new URIImpl(ns+"Person");
            URI name = new URIImpl(ns+"name");
            Literal l1 = new LiteralImpl("Mike");
            Literal l2 = new LiteralImpl("Bryan");
            Literal l3 = new LiteralImpl("Martyn");
/**/
            cxn.setNamespace("ns", ns);
            
            cxn.add(mikeA, RDF.TYPE, person);
            cxn.add(mikeA, name, l1);
            cxn.add(mikeB, RDF.TYPE, person);
            cxn.add(mikeB, name, l1);
            cxn.add(bryan, RDF.TYPE, person);
            cxn.add(bryan, name, l2);
            cxn.add(martyn, RDF.TYPE, person);
            cxn.add(martyn, name, l3);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            cxn.commit();//
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            {
                
//                String query = 
//                    "PREFIX "+QueryHints.PREFIX+": <"+QueryHints.NAMESPACE+QueryHints.HASH_JOIN+"=true> " +
//                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
//                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
//                    "PREFIX bds: <"+BD.SEARCH_NAMESPACE+"> " +
//                    "PREFIX ns: <"+ns+"> " +
//                    
//                    "select distinct ?s1 ?s2 " +
////                    "select distinct ?s1 " +
//                    "WHERE { " +
//                    "  ?o1 bds:search \"m*\" ." +
//                    "  ?s1 ns:name ?o1 . " +
//                    "  ?s1 rdf:type ns:Person . " +
//                    "  ?s1 ns:name ?name . " +
//                    "  OPTIONAL { " +
//                    "    ?o2 bds:search \"m*\" ." +
//                    "    ?s2 ns:name ?o2 . " +
//                    "    ?s2 rdf:type ns:Person . " +
//                    "    ?s2 ns:name ?name . " +
//                    "    filter(?s1 != ?s2) . " +
//                    "  } " +
////                    "  filter(!bound(?s2) || ?s1 != ?s2) . " +
//                    "}";
                
                String query = 
                    "PREFIX "+QueryHints.PREFIX+": <"+QueryHints.NAMESPACE+QueryHints.HASH_JOIN+"=true> " +
                    "PREFIX rdf: <"+RDF.NAMESPACE+"> " +
                    "PREFIX rdfs: <"+RDFS.NAMESPACE+"> " +
                    "PREFIX bds: <"+BD.SEARCH_NAMESPACE+"> " +
                    "PREFIX ns: <"+ns+"> " +
                    
                    "select distinct ?s1 ?s2 " +
                    "WHERE { " +
                    "  ?s1 rdf:type ns:Person . " +
                    "  ?s1 ns:name ?name . " +
                    "  OPTIONAL { " +
                    "    ?s2 rdf:type ns:Person . " +
                    "    ?s2 ns:name ?name . " +
                    "    filter(?s1 != ?s2) . " +
                    "  } " +
                    "}";

                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
                
                while (result.hasNext()) {
                    System.err.println(result.next());
                }
 
//                Collection<BindingSet> solution = new LinkedList<BindingSet>();
//                solution.add(createBindingSet(new Binding[] {
//                    new BindingImpl("s", mike),
//                    new BindingImpl("p", RDFS.LABEL),
//                    new BindingImpl("label", l1)
//                }));
//                solution.add(createBindingSet(new Binding[] {
//                    new BindingImpl("s", bryan),
//                    new BindingImpl("p", RDFS.COMMENT),
//                    new BindingImpl("label", l2)
//                }));
//                
//                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
