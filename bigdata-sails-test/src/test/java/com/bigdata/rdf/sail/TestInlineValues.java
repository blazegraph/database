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
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
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
 */
public class TestInlineValues extends ProxyBigdataSailTestCase {

	protected static final Logger log = Logger.getLogger(TestInlineValues.class);
	
    @Override
    public Properties getProperties() {
        
        final Properties props = new Properties(super.getProperties());
        
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
    public TestInlineValues() {
    }

    /**
     * @param arg0
     */
    public TestInlineValues(String arg0) {
        super(arg0);
    }

    public void testInlineValuesLT() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            URI A = vf.createURI("_:A");
            URI B = vf.createURI("_:B");
            URI X = vf.createURI("_:X");
            URI AGE = vf.createURI("_:AGE");
            Literal _25 = vf.createLiteral(25);
            Literal _45 = vf.createLiteral(45);

            cxn.add(A, RDF.TYPE, X);
            cxn.add(B, RDF.TYPE, X);
            cxn.add(A, AGE, _25);
            cxn.add(B, AGE, _45);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }

            {
                
                String query = 
                    "select ?s ?age " +
                    "WHERE { " +
                    "  ?s <"+RDF.TYPE+"> <"+X+"> . " +
                    "  ?s <"+AGE+"> ?age . " +
                    "  FILTER( ?age < 35 ) . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", A),
                    new BindingImpl("age", _25)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testInlineValuesGT() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            URI A = vf.createURI("_:A");
            URI B = vf.createURI("_:B");
            URI X = vf.createURI("_:X");
            URI AGE = vf.createURI("_:AGE");
            Literal _25 = vf.createLiteral(25);
            Literal _45 = vf.createLiteral(45);

            cxn.add(A, RDF.TYPE, X);
            cxn.add(B, RDF.TYPE, X);
            cxn.add(A, AGE, _25);
            cxn.add(B, AGE, _45);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }

            {
                
                String query = 
                    "select ?s ?age " +
                    "WHERE { " +
                    "  ?s <"+RDF.TYPE+"> <"+X+"> . " +
                    "  ?s <"+AGE+"> ?age . " +
                    "  FILTER( ?age > 35 ) . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                TupleQueryResult result = tupleQuery.evaluate();
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", B),
                    new BindingImpl("age", _45)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    public void testIsLiteral() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();
            
            URI A = vf.createURI("_:A");
            URI B = vf.createURI("_:B");
            URI X = vf.createURI("_:X");
            URI AGE = vf.createURI("_:AGE");
            Literal _25 = vf.createLiteral(25);
            Literal _45 = vf.createLiteral(45);

            cxn.add(A, RDF.TYPE, X);
            cxn.add(B, RDF.TYPE, X);
            cxn.add(A, AGE, _25);
            cxn.add(B, AGE, _45);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (log.isInfoEnabled()) {
                log.info(cxn.getTripleStore().dumpStore());
            }

            {
                
                String query = 
                    "select ?s ?age " +
                    "WHERE { " +
                    "  ?s <"+RDF.TYPE+"> <"+X+"> . " +
                    "  ?s <"+AGE+"> ?age . " +
                    "  FILTER( isLiteral(?age) ) . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                
                if (log.isInfoEnabled()) {
                	final TupleQueryResult result = tupleQuery.evaluate();
                	log.info("results:");
                	if (!result.hasNext()) {
                		log.info("no results.");
                	}
                	while (result.hasNext()) {
                		log.info(result.next());
                	}
                }
                
                final TupleQueryResult result = tupleQuery.evaluate();
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", A),
                    new BindingImpl("age", _25)
                }));
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", B),
                    new BindingImpl("age", _45)
                }));
                
                compare(result, solution);
                
            }
            
            {
                
                String query = 
                    "select ?s ?age " +
                    "WHERE { " +
                    "  ?s <"+RDF.TYPE+"> <"+X+"> . " +
                    "  ?s <"+AGE+"> ?age . " +
                    "  FILTER( isLiteral("+_25.toString()+") ) . " +
                    "}";
                
                final TupleQuery tupleQuery = 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
                
                if (log.isInfoEnabled()) {
                	final TupleQueryResult result = tupleQuery.evaluate();
                	log.info("results:");
                	if (!result.hasNext()) {
                		log.info("no results.");
                	}
                	while (result.hasNext()) {
                		log.info(result.next());
                	}
                }
                
                final TupleQueryResult result = tupleQuery.evaluate();
 
                Collection<BindingSet> solution = new LinkedList<BindingSet>();
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", A),
                    new BindingImpl("age", _25)
                }));
                solution.add(createBindingSet(new Binding[] {
                    new BindingImpl("s", B),
                    new BindingImpl("age", _45)
                }));
                
                compare(result, solution);
                
            }
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
}
