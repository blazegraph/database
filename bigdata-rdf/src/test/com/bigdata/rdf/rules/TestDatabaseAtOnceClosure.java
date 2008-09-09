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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;
import java.util.Properties;

import org.openrdf.model.Statement;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.axioms.RdfsAxioms;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.rio.StatementBuffer;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;
import com.bigdata.rdf.vocab.Vocabulary;
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

    public void test_fixedPoint_Small_Full() throws Exception {

        final String file = "small.rdf";

        doFixedPointTest(file, FullClosure.class);

    }

    public void test_fixedPoint_Small_Fast() throws Exception {

        final String file = "small.rdf";

        doFixedPointTest(file, FastClosure.class);

    }

    public void test_fixedPoint_SampleData_Full() throws Exception {

        final String file = "sample data.rdf";

        doFixedPointTest(file, FullClosure.class);

    }

    public void test_fixedPoint_SampleData_Fast() throws Exception {

        final String file = "sample data.rdf";

        doFixedPointTest(file, FastClosure.class);

    }

    public void test_fixedPoint_TestOwlSameAs_Full() throws Exception {
        
        // final String file = "testOwlSameAs.rdf";
        final String file = "small owlSameAs.rdf";
        
        doFixedPointTest(file, FullClosure.class);
    
    }
    
    public void test_fixedPoint_TestOwlSameAs_Fast() throws Exception {
        
        // final String file = "testOwlSameAs.rdf";
        final String file = "small owlSameAs.rdf";
        
        doFixedPointTest(file, FastClosure.class);
    
    }

    /*
     * Commented out because these are a bit slow for unit tests and have
     * never identified any problems.
     */
    
//    public void test_fixedPoint_LUBM_U1_As_Full() throws Exception {
//
//        final File[] files = readFiles(new File("../rdf-data/lehigh/U1"),
//                new FilenameFilter() {
//
//                    public boolean accept(File dir, String name) {
//                        return name.endsWith(".owl");
//                    }
//                });
//
//        doFixedPointTest(files, FullClosure.class);
//
//    }
//
//    public void test_fixedPoint_LUBM_U1_As_Fast() throws Exception {
//
//        final File[] files = readFiles(new File("../rdf-data/lehigh/U1"),
//                new FilenameFilter() {
//
//                    public boolean accept(File dir, String name) {
//                        return name.endsWith(".owl");
//                    }
//                });
//
//        doFixedPointTest(files, FastClosure.class);
//
//    }

    /**
     * Reads files matching the filter from the directory.
     * 
     * @param dir
     *            The directory.
     * @param filter
     *            The filter.
     */
    private File[] readFiles(File dir, FilenameFilter filter) {

        assertTrue("No such file or directory: " + dir, dir.exists());

        assertTrue("Not a directory: " + dir, dir.isDirectory());

        final File[] files = dir.listFiles(filter);

        assertNotNull("Could not read directory: " + dir, files);
        
        return files;
        
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
     * @throws Exception
     */
    protected void doFixedPointTest(String file,
            Class<? extends BaseClosure> closureClass) throws Exception {

        doFixedPointTest(new File[] { new File(file) }, closureClass);
        
    }

    /**
     * Compares ground truth for the closure of the source RDF/XML file(s) (as
     * computed by Sesame 2) against the closure as computed by bigdata.
     * 
     * @param files
     *            The RDF/XML files.
     * @param closureType
     *            The closure program to be applied by bigdata.
     * 
     * @throws Exception
     */
    protected void doFixedPointTest(File[] files,
                Class<? extends BaseClosure> closureClass) throws Exception {
            
        /*
         * Used to compute the entailments our own rules engine.
         */
        final AbstractTripleStore closure;
        {

            final Properties properties = getProperties();

            // restrict to RDFS only since that is what Sesame 2 will compute.
            properties
                    .setProperty(
                            com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS,
                            RdfsAxioms.class.getName());

            // use the specified closure algorithm.
            properties
                    .setProperty(
                            com.bigdata.rdf.store.AbstractTripleStore.Options.CLOSURE_CLASS,
                            closureClass.getName());

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
         
            final Properties properties = new Properties();
            
            properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS,
                    NoAxioms.class.getName());

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
	            	
                    for(File file : files) {
                        
                        InputStream is = null;

                        try {
                        
                            is = new FileInputStream(file);
                            
                        } catch (FileNotFoundException ex) {
                            
                            is = getClass()
                                    .getResourceAsStream(file.toString());
                            
                        }
                        
                        if(is == null) {
                            
                            fail("No such file or resource: "+file);
                            
                        }

                        cxn.add(is, file.toURI().toString()/* baseURI */,
                                RDFFormat.RDFXML);

                        if(log.isInfoEnabled()) {
                         
                            log.info("Loaded: "+file);
                            
                        }
                        
                    }
	            	
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
	                
	                if (log.isDebugEnabled()) {

                        log.debug("\ngroundTruth:\n" + groundTruth.dumpStore());
                        
                    }
	                
                    log.error("# Sesame2 stmts: " + numSesame2Stmts);
                    
//	                // make the data visible to a read-committed view.
//	                groundTruth.commit();
	                
	            } finally {
	            	
	            	cxn.close();
	            	
	            }
        	
        	}
        	
        	{ 
                /*
                 * Loads the same data into the closure store and computes the
                 * closure.
                 */
                
                for(File file : files) {
        		
                    InputStream is = null;

                    try {
                    
                        is = new FileInputStream(file);
                        
                    } catch (FileNotFoundException ex) {
                        
                        is = getClass()
                                .getResourceAsStream(file.toString());
                        
                    }
                    
                    if(is == null) {
                        
                        fail("No such file or resource: "+file);
                        
                    }

                    closure.getDataLoader().loadData(is,
                            file.toURI().toString()/* baseURL */,
                            RDFFormat.RDFXML);
                
                }

// closure.commit();
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
                
                if (log.isDebugEnabled()) {

                    log.debug("\nclosure:\n" + closure.dumpStore());
                    
                }
                
                // -DdataLoader.closure=None
        	}
            
            /*
             * Note: For this test the closure of the [groundTruth] was computed
             * by Sesame and just loaded into a bigdata triple store instance
             * while the closure of the [closure] graph was computed by bigdata
             * itself. In order to be able to compare these two graphs, we first
             * bulk export the [closure] graph (with its backchained
             * entailments) into a TempTripleStore and then compare that
             * TempTripleStore to the data from Sesame2.
             */
            final TempTripleStore tmp = bulkExport(closure);

            assertTrue(modelsEqual(groundTruth, tmp));
            
            tmp.closeAndDelete();
            
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
        
        final Properties properties = super.getProperties();
        
        // override the default axiom model.
        properties.setProperty(com.bigdata.rdf.store.AbstractTripleStore.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        
        final AbstractTripleStore store = getStore(properties);
        
        try {
            
            final BigdataValueFactory f = store.getValueFactory();
            
            final BigdataURI A = f.createURI("http://www.bigdata.com/a");
            final BigdataURI B = f.createURI("http://www.bigdata.com/b");
            final BigdataURI C = f.createURI("http://www.bigdata.com/c");
            final BigdataURI D = f.createURI("http://www.bigdata.com/d");
            final BigdataURI SCO = f.asValue(RDFS.SUBCLASSOF);
            
            final Vocabulary vocab = store.getVocabulary();

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
