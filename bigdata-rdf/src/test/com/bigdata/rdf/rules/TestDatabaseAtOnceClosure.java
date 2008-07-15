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
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
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

    /**
     * Unit test for correct fix point closure of a known data set.
     * 
     * FIXME Things to watch out for here include the choice of the read times
     * for the relation views and whether and if those read times are updated
     * correctly after each round of closure.
     */
    public void test_fixedPoint() throws SailException, RepositoryException, 
    	IOException, RDFParseException {
        
        final AbstractTripleStore closure = getStore();
        
        final TempTripleStore groundTruth = 
        	new TempTripleStore(new Properties());
        
        try {
        	
        	{ // use the Sesame2 inferencer to get ground truth
        		
	            StatementBuffer buf = new StatementBuffer(groundTruth ,10);
	            
	            Repository sesame2 = new SailRepository(
	                    new ForwardChainingRDFSInferencer(
	                    new MemoryStore()));
	            
	            sesame2.initialize();
	            
	            RepositoryConnection cxn = sesame2.getConnection();
	            
	            cxn.setAutoCommit(false);
	            
	            try {
	            	
	            	cxn.add(getClass().getResourceAsStream("sample data.rdf"), 
	            			"", RDFFormat.RDFXML);
	            	
	            	cxn.commit();
	            	
	            	RepositoryResult<Statement> stmts = 
	            		cxn.getStatements(null, null, null, true);
	            	
	            	while(stmts.hasNext()) {
	            		
	            		Statement stmt = stmts.next();
	            		
	            		buf.add(stmt.getSubject(), stmt.getPredicate(), 
	            				stmt.getObject());
	            		
	            	}
	            	
	                buf.flush();
	                
	                groundTruth.dumpStore();
	                
	                // make the data visible to a read-committed view.
	                groundTruth.commit();
	                
	            } finally {
	            	
	            	cxn.close();
	            	
	            }
        	
        	}
        	
        	{ // load the same data into the closure store
        		
        		
        		
        	}
            
        	assertTrue(modelsEqual(closure, groundTruth));
            
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
                
                // make the data visible to a read-committed view.
                store.commit();
                
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
                        .newJoinNexusFactory(ActionEnum.Insert, IJoinNexus.ALL,
                                null/*filter*/);
            
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
