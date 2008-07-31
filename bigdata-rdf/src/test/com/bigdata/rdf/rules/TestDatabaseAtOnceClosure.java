/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jul 14, 2008
 */

package com.bigdata.rdf.rules;

import java.io.IOException;
import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.rules.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.relation.rule.Program;
import com.bigdata.relation.rule.eval.ActionEnum;
import com.bigdata.relation.rule.eval.IJoinNexus;
import com.bigdata.relation.rule.eval.IJoinNexusFactory;

/**
 * Unit tests for database at once closure, fix point of a rule set (does not
 * test truth maintenance under assertion and retraction or the justifications).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDatabaseAtOnceClosure extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestDatabaseAtOnceClosure() {

    }

    /**
     * @param name
     */
    public TestDatabaseAtOnceClosure(String name) {

        super(name);

    }

    public void test_fixedPoint_Small_Full() throws SailException,
            RepositoryException, IOException, RDFParseException {

        final String file = "small.rdf";

        doFixedPointTest(file, ForwardClosureEnum.Full);

    }

    public void test_fixedPoint_Small_Fast() throws SailException,
            RepositoryException, IOException, RDFParseException {

        final String file = "small.rdf";

        doFixedPointTest(file, ForwardClosureEnum.Fast);

    }

    public void test_fixedPoint_SampleData_Full() throws SailException,
            RepositoryException, IOException, RDFParseException {

        final String file = "sample data.rdf";

        doFixedPointTest(file, ForwardClosureEnum.Full);

    }

    public void test_fixedPoint_SampleData_Fast() throws SailException,
            RepositoryException, IOException, RDFParseException {

        final String file = "sample data.rdf";

        doFixedPointTest(file, ForwardClosureEnum.Fast);

    }

    public void test_fixedPoint_TestOwlSameAs_Full() throws SailException,
        RepositoryException, IOException, RDFParseException {
    
        // final String file = "testOwlSameAs.rdf";
        final String file = "small owlSameAs.rdf";
        
        doFixedPointTest(file, ForwardClosureEnum.Full);
    
    }
    
    public void test_fixedPoint_TestOwlSameAs_Fast() throws SailException,
        RepositoryException, IOException, RDFParseException {
    
        final String file = "testOwlSameAs.rdf";
        
        doFixedPointTest(file, ForwardClosureEnum.Fast);
    
    }
    
    /**
     * Compares ground truth for the closure of the source RDF/XML file (as
     * computed by Sesame 2) against the closure as computed by bigdata.
     * 
     * @param file
     *            The RDF/XML file.
     * @param closureType
     *            The closure program to be applied by bigdata.
     * 
     * @throws RepositoryException
     * @throws RDFParseException
     * @throws IOException
     * @throws SailException
     */
    protected void doFixedPointTest(String file, ForwardClosureEnum closureType)
            throws RepositoryException, RDFParseException, IOException,
            SailException {

        /*
         * Used to compute the entailments with out own rules engine.
         */
        final AbstractTripleStore closure;
        {

            final Properties properties = getProperties();

            properties.setProperty(InferenceEngine.Options.RDFS_ONLY, "true");

            properties.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,
                    closureType.toString());

            /*
             * Don't compute closure in the data loader since it does TM, not
             * database at once closure.
             */
            properties.setProperty(DataLoader.Options.CLOSURE,
                    ClosureEnum.None.toString());
            
            // properties.setProperty(InferenceEngine.Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE, "true");

            /*
             * Note: using variant method so that we can override some
             * properties for this test in a proxy test suite.
             */
            closure = getStore(properties);
            
        }

        /*
         * Gets loaded with the entailments computed by Sesame 2.
         */
        final TempTripleStore groundTruth;
        {
         
            Properties properties = new Properties();
            
            properties.setProperty(InferenceEngine.Options.RDFS_ONLY, "true");

            groundTruth = new TempTripleStore(properties);
            
        }

        try {

        	{ // use the Sesame2 inferencer to get ground truth
        		
                int numSesame2Stmts = 0;
                
	            StatementBuffer buf = new StatementBuffer(groundTruth ,10);
	            
	            Repository sesame2 = new SailRepository(
	                    new ForwardChainingRDFSInferencer(
	                    new MemoryStore()));
	            
	            sesame2.initialize();
	            
	            RepositoryConnection cxn = sesame2.getConnection();
	            
	            cxn.setAutoCommit(false);
	            
	            try {
	            	
	            	cxn.add(getClass().getResourceAsStream(file), 
	            			"", RDFFormat.RDFXML);
	            	
	            	cxn.commit();
	            	
	            	RepositoryResult<Statement> stmts = 
	            		cxn.getStatements(null, null, null, true);
	            	
	            	while(stmts.hasNext()) {
	            		
	            		Statement stmt = stmts.next();
	            		
	            		buf.add(stmt.getSubject(), stmt.getPredicate(), 
	            				stmt.getObject());
                        
                        numSesame2Stmts++;
	            		
	            	}
	            	
	                buf.flush();
	                
	                if (log.isInfoEnabled()) {

                        log.info("\ngroundTruth:\n" + groundTruth.dumpStore());
                        
                    }
	                
                    log.error("# Sesame2 stmts: " + numSesame2Stmts);
                    
	                // make the data visible to a read-committed view.
	                groundTruth.commit();
	                
	            } finally {
	            	
	            	cxn.close();
	            	
	            }
        	
        	}
        	
        	{ 
                /*
                 * Loads the same data into the closure store and computes the
                 * closure.
                 */
        		
        		closure.getDataLoader().loadData(
        				getClass().getResourceAsStream(file), 
	            		"", RDFFormat.RDFXML);

//                closure.commit();
//                
                /*
                 * compute the database at once closure.
                 * 
                 * Note: You can run either the full closure or the fast closure
                 * method depending on how you setup the store. You can also use
                 * an explicit InferenceEngine ctor to setup for either closure
                 * method by overriding the appropriate property (it will be set
                 * by the proxy test case otherwise which does not give you much
                 * control).
                 */
                closure.getInferenceEngine()
                        .computeClosure(null/* focusStore */);
                
                if (log.isInfoEnabled()) {

                    log.info("\nclosure:\n" + closure.dumpStore());
                    
                }
                
                // -DdataLoader.closure=None
        	}
            
        	assertTrue(modelsEqual(groundTruth, closure));
            
        } finally {
            
            closure.closeAndDelete();
            
            groundTruth.closeAndDelete();
            
        }
        
    }

    /**
     * Example using only {@link RuleRdfs11} that requires multiple rounds to
     * compute the fix point closure of a simple data set. This example is based
     * on closure of the class hierarchy. Given
     * 
     * <pre>
     * a sco b
     * b sco c
     * c sco d
     * </pre>
     * 
     * round 1 adds
     * 
     * <pre>
     * a sco c
     * b sco d
     * </pre>
     * 
     * round 2 adds
     * 
     * <pre>
     * a sco d
     * </pre>
     * 
     * and that is the fixed point.
     * 
     * @throws Exception 
     */
    public void test_simpleFixPoint() throws Exception {
        
        final AbstractTripleStore store = getStore();
        
        try {
            
            final URI A = new URIImpl("http://www.bigdata.com/a");
            final URI B = new URIImpl("http://www.bigdata.com/b");
            final URI C = new URIImpl("http://www.bigdata.com/c");
            final URI D = new URIImpl("http://www.bigdata.com/d");
            final URI SCO = RDFS.SUBCLASSOF;
            
            final RDFSVocabulary vocab = new RDFSVocabulary(store);
            
            /*
             * Add the original statements.
             */
            {
                
                StatementBuffer buf = new StatementBuffer(store,10);
                
                buf.add(A, SCO, B);
                buf.add(B, SCO, C);
                buf.add(C, SCO, D);
                
                buf.flush();
                
                if (log.isInfoEnabled())
                    log.info("\n" + store.dumpStore());
                
//                // make the data visible to a read-committed view.
//                store.commit();
                
            }
            
            // only the three statements that we added explicitly.
            assertEquals(3L, store.getStatementCount());
            
            // setup program to run rdfs11 to fixed point.
            final Program program = new Program("rdfs11", false);
            
            program.addClosureOf(new RuleRdfs11(store.getSPORelation()
                    .getNamespace(), vocab));

            /*
             * Run the rule to fixed point on the data.
             */
            {
                
                final IJoinNexusFactory joinNexusFactory = store
						.newJoinNexusFactory(
								RuleContextEnum.DatabaseAtOnceClosure,
								ActionEnum.Insert, IJoinNexus.ALL, null/* filter */);
            
                final long mutationCount = joinNexusFactory.newInstance(
                        store.getIndexManager()).runMutation(program);

                assertEquals("mutationCount", 3, mutationCount);

                assertEquals("statementCount", 6, store.getStatementCount());
                
            }
            
            /*
             * Verify the entailments.
             */
            assertNotNull(store.getStatement(A, SCO, C));
            assertNotNull(store.getStatement(B, SCO, D));
            assertNotNull(store.getStatement(A, SCO, D));
            
        } finally {
            
            store.closeAndDelete();
            
        }
        
    }
    
}
