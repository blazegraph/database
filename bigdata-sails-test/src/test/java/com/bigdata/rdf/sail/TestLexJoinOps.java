/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2007.  All rights reserved.

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

package com.bigdata.rdf.sail;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryOptimizerEnum;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

public class TestLexJoinOps extends QuadsTestCase {

    private static final Logger log = Logger.getLogger(TestLexJoinOps.class);

    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

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
    public TestLexJoinOps() {
    }

    /**
     * @param arg0
     */
    public TestLexJoinOps(String arg0) {
        super(arg0);
    }
    
    public void testStr() throws Exception {
        
//        final Sail sail = new MemoryStore();
//        try {
//        sail.initialize();
//        final Repository repo = new SailRepository(sail);

        final BigdataSail sail = getSail();
        try {
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        
        final RepositoryConnection cxn = repo.getConnection();
        
        try {
            cxn.setAutoCommit(false);
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI X = vf.createURI(BD.NAMESPACE + "X");
            final URI dt = vf.createURI(BD.NAMESPACE + "myDatatype");
            final Literal _1 = vf.createLiteral("foo");
            // Since Sesame 2.8 upgrade, xsd:string is the same as plain literal
//            final Literal _2 = vf.createLiteral("foo", XSD.STRING);
            final Literal _3 = vf.createLiteral("foo", dt);
            final Literal _4 = vf.createLiteral("foo", "EN");
            final Literal _5 = vf.createLiteral(true);
            final Literal _6 = vf.createLiteral(1000l);
            
            /*
             * Create some statements.
             */
            cxn.add(X, RDF.TYPE, RDFS.RESOURCE);
            cxn.add(X, RDFS.LABEL, _1);
//            cxn.add(X, RDFS.LABEL, _2);
            cxn.add(X, RDFS.LABEL, _3);
            cxn.add(X, RDFS.LABEL, _4);
            cxn.add(X, RDFS.LABEL, _5);
            cxn.add(X, RDFS.LABEL, _6);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(((BigdataSailRepositoryConnection)cxn).getTripleStore().dumpStore());
            }
            
            {
                
                String query =
//                    QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
                    "prefix bd: <"+BD.NAMESPACE+"> " +
                    "prefix rdf: <"+RDF.NAMESPACE+"> " +
                    "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
                    
                    "select ?o " +
                    "where { " +
                    "  hint:Query hint:"+QueryHints.OPTIMIZER+" \""+QueryOptimizerEnum.None+"\"."+
                    "  ?s rdf:type rdfs:Resource . " +
                    "  ?s ?p ?o . " +
                    "  filter(str(?o) = \"foo\") " +
                    "}"; 
    
                final SailTupleQuery tupleQuery = (SailTupleQuery)
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(false /* includeInferred */);
               
                if (log.isInfoEnabled()) {
                    
                    log.info(query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    while (result.hasNext()) {
                        log.info(result.next());
                    }
                    
                }
                
                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
                answer.add(createBindingSet(
                        new BindingImpl("o", _1)
                        ));
//                answer.add(createBindingSet(
//                        new BindingImpl("o", _2)
//                        ));
                answer.add(createBindingSet(
                        new BindingImpl("o", _3)
                        ));
                answer.add(createBindingSet(
                        new BindingImpl("o", _4)
                        ));

                final TupleQueryResult result = tupleQuery.evaluate();
                compare(result, answer);

            }
            
        } finally {
            cxn.close();
        }
        } finally {
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();//shutDown();
        }

    }

    public void testRegex() throws Exception {
        
//      final Sail sail = new MemoryStore();
//      try {
//      sail.initialize();
//      final Repository repo = new SailRepository(sail);

      final BigdataSail sail = getSail();
      try {
      sail.initialize();
      final BigdataSailRepository repo = new BigdataSailRepository(sail);
      
      final RepositoryConnection cxn = repo.getConnection();
      
      try {
          cxn.setAutoCommit(false);
  
          final ValueFactory vf = sail.getValueFactory();

          /*
           * Create some terms.
           */
          final URI X = vf.createURI(BD.NAMESPACE + "X");
          final URI dt = vf.createURI(BD.NAMESPACE + "myDatatype");
          final Literal _1 = vf.createLiteral("foo");
          // Since Sesame 2.8 upgrade, xsd:string is the same as plain literal
//          final Literal _2 = vf.createLiteral("foo", XSD.STRING);
          final Literal _3 = vf.createLiteral("foo", dt);
          final Literal _4 = vf.createLiteral("foo", "EN");
          final Literal _5 = vf.createLiteral(true);
          final Literal _6 = vf.createLiteral(1000l);
          
          /*
           * Create some statements.
           */
          cxn.add(X, RDF.TYPE, RDFS.RESOURCE);
          cxn.add(X, RDFS.LABEL, _1);
//          cxn.add(X, RDFS.LABEL, _2);
          cxn.add(X, RDFS.LABEL, _3);
          cxn.add(X, RDFS.LABEL, _4);
          cxn.add(X, RDFS.LABEL, _5);
          cxn.add(X, RDFS.LABEL, _6);
          
          /*
           * Note: The either flush() or commit() is required to flush the
           * statement buffers to the database before executing any operations
           * that go around the sail.
           */
          cxn.commit();
          
          if (log.isInfoEnabled()) {
              log.info(((BigdataSailRepositoryConnection)cxn).getTripleStore().dumpStore());
          }
          
          {
              
              String query =
//                  QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
                  "prefix bd: <"+BD.NAMESPACE+"> " +
                  "prefix rdf: <"+RDF.NAMESPACE+"> " +
                  "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
                  
                  "select ?o " +
                  "where { " +
                  "  hint:Query hint:"+QueryHints.OPTIMIZER+" \""+QueryOptimizerEnum.None+"\"."+
                  "  ?s rdf:type rdfs:Resource . " +
                  "  ?s ?p ?o . " +
//                  "  filter(regex(str(?o), \"FOO\")) " +
                  "  filter(regex(str(?o), \"FOO\", \"i\")) " +
                  "}"; 
  
              final SailTupleQuery tupleQuery = (SailTupleQuery)
                  cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
              tupleQuery.setIncludeInferred(false /* includeInferred */);
             
              if (log.isInfoEnabled()) {
                  
                  log.info(query);
                  
                  final TupleQueryResult result = tupleQuery.evaluate();
                  while (result.hasNext()) {
                      log.info(result.next());
                  }
                  
              }
              
              final Collection<BindingSet> answer = new LinkedList<BindingSet>();
              answer.add(createBindingSet(
                      new BindingImpl("o", _1)
                      ));
//              answer.add(createBindingSet(
//                      new BindingImpl("o", _2)
//                      ));
              answer.add(createBindingSet(
                      new BindingImpl("o", _3)
                      ));
              answer.add(createBindingSet(
                      new BindingImpl("o", _4)
                      ));

              final TupleQueryResult result = tupleQuery.evaluate();
              compare(result, answer);

            }
          
        } finally {
            cxn.close();
        }
        } finally {
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();//shutDown();
        }

    }
    
    /*
     * PREFIX : <http://example.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?s WHERE {
    ?s :p ?v .
    FILTER(datatype(xsd:boolean(?v)) = xsd:boolean) .
}
     */

    public void testCastAndDatatype() throws Exception {
        
//      final Sail sail = new MemoryStore();
//      try {
//      sail.initialize();
//      final Repository repo = new SailRepository(sail);

      final BigdataSail sail = getSail();
      try {
      sail.initialize();
      final BigdataSailRepository repo = new BigdataSailRepository(sail);
      
      final RepositoryConnection cxn = repo.getConnection();
      
      try {
          cxn.setAutoCommit(false);
  
          final ValueFactory vf = sail.getValueFactory();

          /*
           * Create some terms.
           */
          final URI X = vf.createURI(BD.NAMESPACE + "X");
          final URI dt = vf.createURI(BD.NAMESPACE + "myDatatype");
          final Literal _1 = vf.createLiteral("foo");
          final Literal _2 = vf.createLiteral("foo", XSD.STRING);
          final Literal _3 = vf.createLiteral("foo", dt);
          final Literal _4 = vf.createLiteral("foo", "EN");
          final Literal _5 = vf.createLiteral(true);
          final Literal _6 = vf.createLiteral(1000l);
          
          /*
           * Create some statements.
           */
          cxn.add(X, RDF.TYPE, RDFS.RESOURCE);
          cxn.add(X, RDFS.LABEL, _1);
          cxn.add(X, RDFS.LABEL, _2);
          cxn.add(X, RDFS.LABEL, _3);
          cxn.add(X, RDFS.LABEL, _4);
          cxn.add(X, RDFS.LABEL, _5);
          cxn.add(X, RDFS.LABEL, _6);
          
          /*
           * Note: The either flush() or commit() is required to flush the
           * statement buffers to the database before executing any operations
           * that go around the sail.
           */
          cxn.commit();
          
          if (log.isInfoEnabled()) {
          	log.info(((BigdataSailRepositoryConnection)cxn).getTripleStore().dumpStore());
          }
          
          {
              
              String query =
//                  QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
                  "prefix bd: <"+BD.NAMESPACE+"> " +
                  "prefix rdf: <"+RDF.NAMESPACE+"> " +
                  "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
                  "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                  
                  "select ?o " +
                  "where { " +
                  "  hint:Query hint:"+QueryHints.OPTIMIZER+" \""+QueryOptimizerEnum.None+"\"."+
                  "  ?s rdf:type rdfs:Resource . " +
                  "  ?s ?p ?o . " +
                  "  FILTER(datatype(xsd:boolean(?o)) = xsd:boolean) . " +
                  "}"; 
  
              final SailTupleQuery tupleQuery = (SailTupleQuery)
                  cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
              tupleQuery.setIncludeInferred(false /* includeInferred */);
             
              if (log.isInfoEnabled()) {
                  
                  log.info(query);
                  
                  final TupleQueryResult result = tupleQuery.evaluate();
                  while (result.hasNext()) {
                      log.info(result.next());
                  }
                  
              }
              
              final Collection<BindingSet> answer = new LinkedList<BindingSet>();
              answer.add(createBindingSet(
                      new BindingImpl("o", _5)
                      ));
              answer.add(createBindingSet(
                      new BindingImpl("o", _6)
                      ));

              final TupleQueryResult result = tupleQuery.evaluate();
              compare(result, answer);

            }
          
        } finally {
            cxn.close();
        }
        } finally {
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();//shutDown();
        }

    }
    
    public void testLang() throws Exception {
        
//      final Sail sail = new MemoryStore();
//      try {
//      sail.initialize();
//      final Repository repo = new SailRepository(sail);

      final BigdataSail sail = getSail();
      try {
      sail.initialize();
      final BigdataSailRepository repo = new BigdataSailRepository(sail);
      
      final RepositoryConnection cxn = repo.getConnection();
      
      try {
          cxn.setAutoCommit(false);
  
          final ValueFactory vf = sail.getValueFactory();

          /*
           * Create some terms.
           */
          final URI X = vf.createURI(BD.NAMESPACE + "X");
          final URI Y = vf.createURI(BD.NAMESPACE + "Y");
          final Literal _1 = vf.createLiteral("That Seventies Show","en");
          final Literal _2 = vf.createLiteral("Cette S�rie des Ann�es Soixante-dix","fr");
          final Literal _3 = vf.createLiteral("Cette S�rie des Ann�es Septante","fr-BE");
          final Literal _4 = vf.createLiteral("Il Buono, il Bruto, il Cattivo");

          /*
           * Create some statements.
           */
          cxn.add(X, RDFS.LABEL, _1);
          cxn.add(X, RDFS.LABEL, _2);
          cxn.add(X, RDFS.LABEL, _3);
          cxn.add(Y, RDFS.LABEL, _4);
          
          /*
           * Note: The either flush() or commit() is required to flush the
           * statement buffers to the database before executing any operations
           * that go around the sail.
           */
          cxn.commit();
          
          if (log.isInfoEnabled()) {
              log.info(((BigdataSailRepositoryConnection)cxn).getTripleStore().dumpStore());
          }
          
          {
              
              String query =
//                  QueryOptimizerEnum.queryHint(QueryOptimizerEnum.None) +
                  "prefix bd: <"+BD.NAMESPACE+"> " +
                  "prefix rdf: <"+RDF.NAMESPACE+"> " +
                  "prefix rdfs: <"+RDFS.NAMESPACE+"> " +
                  "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +
                  
                  "select ?title " +
                  "where { " +
                  "  hint:Query hint:"+QueryHints.OPTIMIZER+" \""+QueryOptimizerEnum.None+"\"."+
                  "  ?s rdfs:label \"That Seventies Show\"@en . " +
                  "  ?s rdfs:label ?title . " +
                  "  FILTER langMatches( lang(?title), \"FR\" ) . " +
                  "}"; 
  
              final SailTupleQuery tupleQuery = (SailTupleQuery)
                  cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
              tupleQuery.setIncludeInferred(false /* includeInferred */);
             
              if (log.isInfoEnabled()) {
                  
                  log.info(query);
                  
                  final TupleQueryResult result = tupleQuery.evaluate();
                  while (result.hasNext()) {
                      log.info(result.next());
                  }
                  
              }
              
              final Collection<BindingSet> answer = new LinkedList<BindingSet>();
              answer.add(createBindingSet(
                      new BindingImpl("title", _2)
                      ));
              answer.add(createBindingSet(
                      new BindingImpl("title", _3)
                      ));

              final TupleQueryResult result = tupleQuery.evaluate();
              compare(result, answer);

            }
          
        } finally {
            cxn.close();
        }
        } finally {
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();//shutDown();
        }

    }
    
}
