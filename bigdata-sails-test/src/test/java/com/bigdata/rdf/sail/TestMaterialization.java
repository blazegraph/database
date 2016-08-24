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

package com.bigdata.rdf.sail;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.rdf.store.BD;

public class TestMaterialization extends ProxyBigdataSailTestCase {

    protected static final Logger log = Logger.getLogger(TestMaterialization.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
    /**
     * 
     */
    public TestMaterialization() {
    }

    /**
     * @param arg0
     */
    public TestMaterialization(String arg0) {
        super(arg0);
    }
    
    @Override
    public Properties getProperties() {
        
        Properties props = super.getProperties();

        props.setProperty(BigdataSail.Options.TEXT_INDEX, "true");
        
        return props;
        
    }
    
    public void testRegex() throws Exception {
        
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
          final Literal label = vf.createLiteral("John");
          
          /*
           * Create some statements.
           */
          cxn.add(X, RDF.TYPE, RDFS.RESOURCE);
          cxn.add(X, RDFS.LABEL, label);
          
          /*
           * Note: The either flush() or commit() is required to flush the
           * statement buffers to the database before executing any operations
           * that go around the sail.
           */
          cxn.commit();
          
          if (log.isInfoEnabled()) {
              log.info(((BigdataSailRepositoryConnection) cxn).getTripleStore().dumpStore());
          }
          
          {
              
              final String query =
                  "select * where { ?s ?p ?o . FILTER (regex(?o,\"John\",\"i\")) }";
  
              final SailTupleQuery tupleQuery = (SailTupleQuery)
                  cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
              tupleQuery.setIncludeInferred(true /* includeInferred */);
             
              if (log.isInfoEnabled()) {
                  
                  log.info(query);
                  
                  final TupleQueryResult result = tupleQuery.evaluate();
                  while (result.hasNext()) {
                      log.info(result.next());
                  }
                  
              }
              
//              final Collection<BindingSet> answer = new LinkedList<BindingSet>();
//              answer.add(createBindingSet(
//                      new BindingImpl("a", paul),
//                      new BindingImpl("b", mary)
//                      ));
//              answer.add(createBindingSet(
//                      new BindingImpl("a", brad),
//                      new BindingImpl("b", john)
//                      ));
//
//              final TupleQueryResult result = tupleQuery.evaluate();
//              compare(result, answer);

            }
          
        } finally {
            cxn.close();
        }
        } finally {
            if (sail instanceof BigdataSail)
                ((BigdataSail)sail).__tearDownUnitTest();//shutDown();
        }

    }
    
    public void testStr() throws Exception {
        
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
            final Literal label = vf.createLiteral("John");
            
            /*
             * Create some statements.
             */
            cxn.add(X, RDF.TYPE, RDFS.RESOURCE);
            cxn.add(X, RDFS.LABEL, label);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(((BigdataSailRepositoryConnection) cxn).getTripleStore().dumpStore());
            }
            
            {
                
                String query =
                    "select * where { ?s ?p ?o . FILTER (str(?o) = \""+RDF.PROPERTY+"\") }";
    
                final SailTupleQuery tupleQuery = (SailTupleQuery)
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
               
                if (log.isInfoEnabled()) {
                    
                    log.info(query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    if (result.hasNext()) {
                        while (result.hasNext()) 
                        	log.info(result.next());
                    } else {
                    	fail("expecting result from the vocab");
                    }
                    
                }
                
//                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                answer.add(createBindingSet(
//                        new BindingImpl("a", paul),
//                        new BindingImpl("b", mary)
//                        ));
//                answer.add(createBindingSet(
//                        new BindingImpl("a", brad),
//                        new BindingImpl("b", john)
//                        ));
  //
//                final TupleQueryResult result = tupleQuery.evaluate();
//                compare(result, answer);

              }
            
          } finally {
              cxn.close();
          }
          } finally {
              if (sail instanceof BigdataSail)
                  ((BigdataSail)sail).__tearDownUnitTest();//shutDown();
          }

      }
      
    public void testXsdStr() throws Exception {
        
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
            final Literal label = vf.createLiteral("John");
            
            /*
             * Create some statements.
             */
            cxn.add(X, RDF.TYPE, RDFS.RESOURCE);
            cxn.add(X, RDFS.LABEL, label);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            if (log.isInfoEnabled()) {
                log.info(((BigdataSailRepositoryConnection) cxn).getTripleStore().dumpStore());
            }
            
            {
                
                String query =
                    "select * where { ?s ?p ?o . FILTER (xsd:string(?o) = \""+RDF.PROPERTY+"\"^^xsd:string) }";
    
                final SailTupleQuery tupleQuery = (SailTupleQuery)
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                tupleQuery.setIncludeInferred(true /* includeInferred */);
               
                if (log.isInfoEnabled()) {
                    
                    log.info(query);
                    
                    final TupleQueryResult result = tupleQuery.evaluate();
                    if (result.hasNext()) {
                        while (result.hasNext()) 
                        	log.info(result.next());
                    } else {
                    	fail("expecting result from the vocab");
                    }
                    
                }
                
//                final Collection<BindingSet> answer = new LinkedList<BindingSet>();
//                answer.add(createBindingSet(
//                        new BindingImpl("a", paul),
//                        new BindingImpl("b", mary)
//                        ));
//                answer.add(createBindingSet(
//                        new BindingImpl("a", brad),
//                        new BindingImpl("b", john)
//                        ));
  //
//                final TupleQueryResult result = tupleQuery.evaluate();
//                compare(result, answer);

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
