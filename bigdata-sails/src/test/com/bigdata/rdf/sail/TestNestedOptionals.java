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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.impl.BindingImpl;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailTupleQuery;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.sop.SOp;
import com.bigdata.rdf.sail.sop.SOp2BOpUtility;
import com.bigdata.rdf.sail.sop.SOpTree;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroup;
import com.bigdata.rdf.sail.sop.SOpTree.SOpGroups;
import com.bigdata.rdf.sail.sop.SOpTreeBuilder;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BD;
import com.bigdata.rdf.vocab.NoVocabulary;

/**
 * Unit tests the optionals aspects of the {@link BigdataSail} implementation.
 * 
 * 
 * @author <a href="mailto:mrpersonick@users.sourceforge.net">Mike Personick</a>
 * @version $Id$
 */
public class TestNestedOptionals extends ProxyBigdataSailTestCase {

	/*
	 * TODO
	 * 
	 * 1. arrange sesame operator tree into bigdata predicates with optional
	 * group specified.
	 * 
	 * 2. re-work how constraints are attached - need to be attached to the
	 * first appearance of a variable within its group, not globally across all
	 * predicates (constraints are group-local)
	 * 
	 * 3. some constraints will be conditional routing op instead of an
	 * actual constraint on a predicate. this occurs when the variables in
	 * the constraint do not appear anywhere in the optional group
	 * 
	 * 4. need to punt the query if we can't evaluate a filter inside an
	 * optional group natively, need to recognize this
	 * 
	 * 5. 
	 */
	
    protected static final Logger log = Logger.getLogger(TestNestedOptionals.class);

    protected static final boolean INFO = log.isInfoEnabled();
    
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
    public TestNestedOptionals() {
    }

    /**
     * @param arg0
     */
    public TestNestedOptionals(String arg0) {
        super(arg0);
    }
    
    public void testSimplestNestedOptional() throws Exception {
    	
//        final Sail sail = new MemoryStore();
//        sail.initialize();
//        final Repository repo = new SailRepository(sail);

    	final BigdataSail sail = getSail();
    	sail.initialize();
    	final BigdataSailRepository repo = new BigdataSailRepository(sail);
    	
    	final RepositoryConnection cxn = repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI john = vf.createURI(BD.NAMESPACE + "john");
            final URI mary = vf.createURI(BD.NAMESPACE + "mary");
            final URI leon = vf.createURI(BD.NAMESPACE + "leon");
            final URI paul = vf.createURI(BD.NAMESPACE + "paul");
            final URI brad = vf.createURI(BD.NAMESPACE + "brad");
            final URI fred = vf.createURI(BD.NAMESPACE + "fred");
            final URI knows = vf.createURI(BD.NAMESPACE + "knows");

            /*
             * Create some statements.
             */
            cxn.add(paul, knows, mary);
            cxn.add(paul, knows, brad);
            
            cxn.add(john, knows, mary);
            cxn.add(john, knows, brad);

            cxn.add(mary, knows, brad);
            cxn.add(brad, knows, fred);
            cxn.add(brad, knows, leon);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.commit();
            
            {
            	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  OPTIONAL { " +
	                "    ?b bd:knows ?c . " +
	                "    ?c bd:knows ?d . " +
	                "  } " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	           
	            if (log.isInfoEnabled()) {
	            	
		            final BigdataSailTupleQuery bdTupleQuery =
		            	(BigdataSailTupleQuery) tupleQuery;
		            final QueryRoot root = (QueryRoot) bdTupleQuery.getTupleExpr();
		            final Projection p = (Projection) root.getArg();
		            final TupleExpr tupleExpr = p.getArg();
		            final SOpTreeBuilder stb = new SOpTreeBuilder();
		            final SOpTree tree = stb.collectSOps(tupleExpr);
	           
	                log.info(tree);
	            	log.info(query);

	            	final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	                
	            }
	            
	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", mary),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", leon)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", leon)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", leon)
	            		));
	            
	            final TupleQueryResult result = tupleQuery.evaluate();
                compare(result, answer);

            }
            
        } finally {
            cxn.close();
            sail.shutDown();
        }

    }

    public void testSimpleNestedOptionalWithFilter() throws Exception {
	    	
	//      final Sail sail = new MemoryStore();
	//      sail.initialize();
	//      final Repository repo = new SailRepository(sail);
	
	  	final BigdataSail sail = getSail();
	  	sail.initialize();
	  	final BigdataSailRepository repo = new BigdataSailRepository(sail);
	  	
	  	final RepositoryConnection cxn = repo.getConnection();
	        cxn.setAutoCommit(false);
	      
	    try {
	  
			final ValueFactory vf = sail.getValueFactory();

			/*
			 * Create some terms.
			 */
			final URI john = vf.createURI(BD.NAMESPACE + "john");
			final URI mary = vf.createURI(BD.NAMESPACE + "mary");
			final URI leon = vf.createURI(BD.NAMESPACE + "leon");
			final URI paul = vf.createURI(BD.NAMESPACE + "paul");
			final URI brad = vf.createURI(BD.NAMESPACE + "brad");
			final URI fred = vf.createURI(BD.NAMESPACE + "fred");
			final URI knows = vf.createURI(BD.NAMESPACE + "knows");

			/*
			 * Create some statements.
			 */
			cxn.add(paul, knows, mary);
			cxn.add(paul, knows, brad);

			cxn.add(john, knows, mary);
			cxn.add(john, knows, brad);

			cxn.add(mary, knows, brad);
			cxn.add(brad, knows, fred);
			cxn.add(brad, knows, leon);

			/*
			 * Note: The either flush() or commit() is required to flush the
			 * statement buffers to the database before executing any operations
			 * that go around the sail.
			 */
			cxn.commit();
	          
			{

	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  OPTIONAL { " +
	                "    ?b bd:knows ?c . " +
	                "    ?c bd:knows ?d . " +
	                "    filter(?d != bd:leon) " +
	                "  } " +
	                "}"; 

				final SailTupleQuery tupleQuery = (SailTupleQuery) cxn
						.prepareTupleQuery(QueryLanguage.SPARQL, query);
				tupleQuery.setIncludeInferred(false /* includeInferred */);

				if (log.isInfoEnabled()) {

					final BigdataSailTupleQuery bdTupleQuery = (BigdataSailTupleQuery) tupleQuery;
					final QueryRoot root = (QueryRoot) bdTupleQuery
							.getTupleExpr();
					final Projection p = (Projection) root.getArg();
					final TupleExpr tupleExpr = p.getArg();
					final SOpTreeBuilder stb = new SOpTreeBuilder();
					final SOpTree tree = stb.collectSOps(tupleExpr);

					log.info(tree);
					log.info(query);

					final TupleQueryResult result = tupleQuery.evaluate();
					while (result.hasNext()) {
						log.info(result.next());
					}

				}

	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", mary),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", leon)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", fred)
	            		));

				final TupleQueryResult result = tupleQuery.evaluate();
				compare(result, answer);

			}
	          
		} finally {
			cxn.close();
			sail.shutDown();
		}
	
	}

    public void testSimpleNestedOptionalWithConditional() throws Exception {
    	
//      final Sail sail = new MemoryStore();
//      sail.initialize();
//      final Repository repo = new SailRepository(sail);

  	final BigdataSail sail = getSail();
  	sail.initialize();
  	final BigdataSailRepository repo = new BigdataSailRepository(sail);
  	
  	final RepositoryConnection cxn = repo.getConnection();
      cxn.setAutoCommit(false);
      
      try {
  
          final ValueFactory vf = sail.getValueFactory();

          /*
           * Create some terms.
           */
          final URI john = vf.createURI(BD.NAMESPACE + "john");
          final URI mary = vf.createURI(BD.NAMESPACE + "mary");
          final URI leon = vf.createURI(BD.NAMESPACE + "leon");
          final URI paul = vf.createURI(BD.NAMESPACE + "paul");
          final URI brad = vf.createURI(BD.NAMESPACE + "brad");
          final URI fred = vf.createURI(BD.NAMESPACE + "fred");
          final URI knows = vf.createURI(BD.NAMESPACE + "knows");

          /*
           * Create some statements.
           */
          cxn.add(paul, knows, mary);
          cxn.add(paul, knows, brad);
          
          cxn.add(john, knows, mary);
          cxn.add(john, knows, brad);

          cxn.add(mary, knows, brad);
          cxn.add(brad, knows, fred);
          cxn.add(brad, knows, leon);

          /*
           * Note: The either flush() or commit() is required to flush the
           * statement buffers to the database before executing any operations
           * that go around the sail.
           */
          cxn.commit();
          
          {
          	
	            String query =
	            	"prefix bd: <"+BD.NAMESPACE+"> " +
	                "select * " +
	                "where { " +
	                "  ?a bd:knows ?b . " +
	                "  OPTIONAL { " +
	                "    ?b bd:knows ?c . " +
	                "    ?c bd:knows ?d . " +
	                "    filter(?a != bd:paul) " +
	                "  } " +
	                "}"; 
	
	            final SailTupleQuery tupleQuery = (SailTupleQuery)
	                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
	            tupleQuery.setIncludeInferred(false /* includeInferred */);
	           
	            if (log.isInfoEnabled()) {
	            	
		            final BigdataSailTupleQuery bdTupleQuery =
		            	(BigdataSailTupleQuery) tupleQuery;
		            final QueryRoot root = (QueryRoot) bdTupleQuery.getTupleExpr();
		            final Projection p = (Projection) root.getArg();
		            final TupleExpr tupleExpr = p.getArg();
		            final SOpTreeBuilder stb = new SOpTreeBuilder();
		            final SOpTree tree = stb.collectSOps(tupleExpr);
	           
	                log.info(tree);
	            	log.info(query);

	            	final TupleQueryResult result = tupleQuery.evaluate();
	                while (result.hasNext()) {
	                    log.info(result.next());
	                }
	                
	            }
	            
	            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", mary),
	            		new BindingImpl("b", brad)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", brad),
	            		new BindingImpl("b", leon)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", paul),
	            		new BindingImpl("b", mary)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", fred)
	            		));
	            answer.add(createBindingSet(
	            		new BindingImpl("a", john),
	            		new BindingImpl("b", mary),
	            		new BindingImpl("c", brad),
	            		new BindingImpl("d", leon)
	            		));
	            
				final TupleQueryResult result = tupleQuery.evaluate();
				compare(result, answer);

			}

		} finally {
			cxn.close();
			sail.shutDown();
		}

	}

    private void __testNestedOptionals1() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI T1 = vf.createURI(BD.NAMESPACE + "T1");
            final URI T2 = vf.createURI(BD.NAMESPACE + "T2");
            final URI T3 = vf.createURI(BD.NAMESPACE + "T3");
            final URI P1 = vf.createURI(BD.NAMESPACE + "P1");
            final URI P2 = vf.createURI(BD.NAMESPACE + "P2");
            final URI P3 = vf.createURI(BD.NAMESPACE + "P3");
            final URI a = vf.createURI(BD.NAMESPACE + "a");
            final URI b = vf.createURI(BD.NAMESPACE + "b");
            final Literal ap1 = vf.createLiteral(25);
            final Literal ap2 = vf.createLiteral(100);
            final Literal ap3 = vf.createLiteral(7465);
            final Literal bp1 = vf.createLiteral(40);
            final Literal bp2 = vf.createLiteral(250);

            /*
             * Create some statements.
             */
            cxn.add(a, RDF.TYPE, T1);
            cxn.add(a, RDF.TYPE, T2);
            cxn.add(a, P1, ap1);
            cxn.add(a, P2, ap2);
            cxn.add(a, P3, ap3);
            
            cxn.add(b, RDF.TYPE, T1);
            cxn.add(b, P1, bp1);
            cxn.add(b, P2, bp2);

            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (INFO) log.info("\n" + sail.getDatabase().dumpStore());

            // get P1 for all T1 and P2 only if also T2
            String query = 
                "prefix rdf: <"+RDF.NAMESPACE+">" +
                "prefix bd: <"+BD.NAMESPACE+">" +
                "select * " +
                "where { " +
                "  ?a bd:knows ?b . " +
                "  optional { " +
                "    ?b bd:knows ?c . " +
                "    ?c bd:knows ?d . " +
                "    filter (?b != bd:Mike) . " +
                "  } " +
                "}"; 

            final BigdataSailTupleQuery tupleQuery = (BigdataSailTupleQuery) 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(false /* includeInferred */);
            
            if (INFO) log.info(tupleQuery.getTupleExpr());
//              
//            if (INFO) {
//                final TupleQueryResult result = tupleQuery.evaluate();
//                while (result.hasNext()) {
//                    log.info(result.next());
//                }
//            }
            
            final QueryRoot root = (QueryRoot) tupleQuery.getTupleExpr();
            final Projection p = (Projection) root.getArg();
            final TupleExpr tupleExpr = p.getArg();
            final SOpTreeBuilder stb = new SOpTreeBuilder();
            final SOpTree tree = stb.collectSOps(tupleExpr);
            
            if (INFO) {
                System.err.println(query);
                for (SOp bop : tree) {
                    System.err.println(bop);    
                }
            }
              
            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet());
            
//            result = tupleQuery.evaluate();
//            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    private void _testNestedOptionals2() throws Exception {

        final BigdataSail sail = getSail();
        sail.initialize();
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        final BigdataSailRepositoryConnection cxn = 
            (BigdataSailRepositoryConnection) repo.getConnection();
        cxn.setAutoCommit(false);
        
        try {
    
            final ValueFactory vf = sail.getValueFactory();

            /*
             * Create some terms.
             */
            final URI T1 = vf.createURI(BD.NAMESPACE + "T1");
            final URI T2 = vf.createURI(BD.NAMESPACE + "T2");
            final URI T3 = vf.createURI(BD.NAMESPACE + "T3");
            final URI T4 = vf.createURI(BD.NAMESPACE + "T4");
            final URI T5 = vf.createURI(BD.NAMESPACE + "T5");
            final URI P0 = vf.createURI(BD.NAMESPACE + "P0");
            final URI P1 = vf.createURI(BD.NAMESPACE + "P1");
            final URI P2 = vf.createURI(BD.NAMESPACE + "P2");
            final URI P3 = vf.createURI(BD.NAMESPACE + "P3");
            final URI P4 = vf.createURI(BD.NAMESPACE + "P4");
            final URI P5 = vf.createURI(BD.NAMESPACE + "P5");
            final URI P6 = vf.createURI(BD.NAMESPACE + "P6");
            final URI a = vf.createURI(BD.NAMESPACE + "a");
            final URI b = vf.createURI(BD.NAMESPACE + "b");
            final URI c = vf.createURI(BD.NAMESPACE + "c");
            final URI ap0 = vf.createURI(BD.NAMESPACE + "ap0");
            final URI ap1 = vf.createURI(BD.NAMESPACE + "ap1");
            final URI ap2 = vf.createURI(BD.NAMESPACE + "ap2");
            final URI bp0 = vf.createURI(BD.NAMESPACE + "bp0");
            final URI bp1 = vf.createURI(BD.NAMESPACE + "bp1");
            final URI bp2 = vf.createURI(BD.NAMESPACE + "bp2");
            final URI bp3 = vf.createURI(BD.NAMESPACE + "bp3");
            final URI cp0 = vf.createURI(BD.NAMESPACE + "cp0");
            final URI cp1 = vf.createURI(BD.NAMESPACE + "cp1");
            final URI cp2 = vf.createURI(BD.NAMESPACE + "cp2");
            final URI cp3 = vf.createURI(BD.NAMESPACE + "cp3");

            /*
             * Create some statements.
             */
            cxn.add(a, RDF.TYPE, T1);
            cxn.add(a, RDF.TYPE, T2);
            cxn.add(a, RDF.TYPE, T3);
            cxn.add(a, P0, ap0);
            cxn.add(a, P1, ap1);
            cxn.add(a, P2, ap2);
            
            cxn.add(b, RDF.TYPE, T1);
            cxn.add(b, RDF.TYPE, T2);
            cxn.add(b, P0, bp0);
            cxn.add(b, P1, bp1);
            cxn.add(b, P2, bp2);
            cxn.add(b, P3, bp3);

            cxn.add(c, RDF.TYPE, T1);
            cxn.add(c, RDF.TYPE, T2);
            cxn.add(c, RDF.TYPE, T3);
            cxn.add(c, P0, cp0);
            cxn.add(c, P1, cp1);
            cxn.add(c, P2, cp2);
            cxn.add(c, P3, cp3);
            
            /*
             * Note: The either flush() or commit() is required to flush the
             * statement buffers to the database before executing any operations
             * that go around the sail.
             */
            cxn.flush();//commit();
            
            if (log.isInfoEnabled()) {
                log.info("\n" + sail.getDatabase().dumpStore());
            }

            // get P1 for all T1 and P2 only if also T2 and T3 also
            // also, optionally get P3 but only if type T2 and T3 also
            String query = 
                "select *\n" +
                "where {\n" +
                "  { " +
                "    ?s <"+RDF.TYPE+"> <"+T1+"> .\n" +                  		// g = 1, pg = 0
                
                "    { ?s <"+P0+"> ?p0 . }\n" +                    					// g = 1, pg = 0
                "    UNION { ?s <"+P1+"> ?p1 .} \n" +                    					// g = 1, pg = 0
                "    OPTIONAL {\n" +
                "      { " +
                "        ?s <"+RDF.TYPE+"> <"+T4+"> .\n" +       				// g = 3, pg = 2
                "        ?s <"+RDF.TYPE+"> <"+T5+"> .\n" +       				// g = 3, pg = 2
                "        ?s <"+P4+"> ?p4 .\n" +                  				// g = 3, pg = 2
                "        FILTER ( ?p4 > (?p1*?p0+10+20) || !bound(?p4)) .\n" +  // g = 3, pg = 2
                "        FILTER (!bound(?p4)) .\n" +							// g = 3, pg = 2
                "        OPTIONAL { ?s <"+P5+"> ?p5 . }\n" +     				// g = 4, pg = 3
                "        OPTIONAL { ?s <"+P6+"> ?p6 . }\n" +     				// g = 5, pg = 3
                "      } " +
                "      UNION " +												// g = 2, pg = 1
                "      { " +
                "        ?s <"+P2+"> ?p200 . " +								// g = 6, pg = 2
                "      }\n" + 
                "    }\n" +
                "    OPTIONAL {\n" +
                "      ?s <"+RDF.TYPE+"> <"+T2+"> .\n" +       					// g = 7, pg = 1
                "      ?s <"+RDF.TYPE+"> <"+T3+"> .\n" +      				 	// g = 7, pg = 1
                "      ?s <"+P2+"> ?p2 .\n" +                  					// g = 7, pg = 1
                "      OPTIONAL { ?s <"+P3+"> ?p3 . }\n" +     					// g = 8, pg = 7
                "    }\n" +
                "  } " +
                "  UNION " +													// g = 0
                "  {" +
                "    ?s <"+RDF.TYPE+"> <"+RDFS.RESOURCE+"> .\n" +       		// g = 2, pg = 0
                "  }\n" +
                "  UNION " +													// g = 0
                "  {" +
                "    ?s <"+RDF.TYPE+"> <"+RDF.PROPERTY+"> .\n" +       		    // g = 2, pg = 0
                "  }\n" +
                "}";
            
          final BigdataSailTupleQuery tupleQuery = (BigdataSailTupleQuery) 
                cxn.prepareTupleQuery(QueryLanguage.SPARQL, query);
            tupleQuery.setIncludeInferred(false /* includeInferred */);
            
            if (INFO) log.info(tupleQuery.getTupleExpr());
            
//            if (INFO) {
//                final TupleQueryResult result = tupleQuery.evaluate();
//                while (result.hasNext()) {
//                    log.info(result.next());
//                }
//            }
            
            final QueryRoot root = (QueryRoot) tupleQuery.getTupleExpr();
            final Projection p = (Projection) root.getArg();
            final TupleExpr tupleExpr = p.getArg();
            final SOpTreeBuilder stb = new SOpTreeBuilder();
            final SOpTree tree = stb.collectSOps(tupleExpr);
            
            if (INFO) {
                System.err.println(query);
                for (SOp bop : tree) {
                    System.err.println(bop);    
                }
                
                for (Map.Entry<Integer, SOpGroup> e : tree.allGroups.entrySet()) {
                	final SOpGroup g = e.getValue();
                	System.err.println(e.getKey() + ": g=" + g.getGroup() + " pg=" + g.getParentGroup());
                	for (SOp sop : e.getValue()) {
                		System.err.println("  " + sop);
                	}
                }
                for (Map.Entry<Integer, SOpGroup> e : tree.parents.entrySet()) {
                	System.err.println(e.getKey() + ": " + e.getValue().getGroup());
                }
                for (Map.Entry<Integer, SOpGroups> e : tree.children.entrySet()) {
                	final SOpGroups groups = e.getValue();
                	StringBuilder sb = new StringBuilder();
                	for (SOpGroup g : groups) {
                		sb.append(g.getGroup()).append(", ");
                	}
                	sb.setLength(sb.length()-2);
                	System.err.println(e.getKey() + ": {" + sb.toString() + "}");
                }
            }
            
            final Collection<BindingSet> answer = new LinkedList<BindingSet>();
            answer.add(createBindingSet());
            
//            final TupleQueryResult result = tupleQuery.evaluate();
//            compare(result, answer);
            
        } finally {
            cxn.close();
            sail.__tearDownUnitTest();
        }

    }
    
    
}
