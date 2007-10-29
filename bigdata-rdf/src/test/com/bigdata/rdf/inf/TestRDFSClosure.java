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
 * Created on Jan 26, 2007
 */

package com.bigdata.rdf.inf;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sesame.admin.RdfAdmin;
import org.openrdf.sesame.admin.StdOutAdminListener;
import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository;

import com.bigdata.rdf.inf.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.inf.InferenceEngine.Options;
import com.bigdata.rdf.sail.BigdataRdfRepository;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for full forward closure.
 * 
 * FIXME Run each of the closure tests against each supported configuration of
 * the inference engine. E.g., fastForwardClosure that backchains (?x rdf:type
 * rdfs:Resource) vs fullForwardClosure that backchains vs fastForwardClosure
 * that stores (?x rdf:type rdfs:Resource) vs ....
 * 
 * @todo run more small tests that focus on specific inferences.
 * 
 * @todo run tests of a variety of ontologies found "in the wild".
 * 
 * @todo verify that we correctly distinguish Axioms, Explicit, and Inferred
 *       statements. Axioms are checked against those defined by
 *       {@link RdfsAxioms}. Explicit statements are checked against the
 *       dataset w/o closure. The rest of the statements should be marked as
 *       Inferred. Note that an Axiom can be marked as Explicit when loading
 *       data, but that TM needs to convert the statement back to an Axiom if it
 *       is deleted. Also note that an inference that concludes a triple that is
 *       an axiom MUST be marked as an Axiom NOT Inferred.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRDFSClosure extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestRDFSClosure() {
    }

    /**
     * @param name
     */
    public TestRDFSClosure(String name) {
        super(name);
    }
    
    /**
     * The RDF/XML resource that will be used by the unit tests.
     */
    final String resource = "data/alibaba_v41.rdf";
    final String baseURL = "";
    
    /**
     * Unit test based on the test resource <code>testClosure01.nt</code>.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    public void testClosure01_full() throws SailInitializationException, IOException, UpdateException {
        
        Properties properties = new Properties(getProperties());

        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());
        
        assertCorrectClosure(properties,"testClosure01.nt", baseURL, RDFFormat.NTRIPLES);
        
    }

    public void testClosure01_fast() throws SailInitializationException, IOException, UpdateException {
        
        Properties properties = new Properties(getProperties());

//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());
        
        assertCorrectClosure(properties,"testClosure01.nt", baseURL, RDFFormat.NTRIPLES);
        
    }

    /**
     * Unit test based on the test resource <code>testClosure01.nt</code>.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    public void testAlibaba_v41_full() throws SailInitializationException, IOException, UpdateException {

        Properties properties = new Properties(getProperties());

        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());

        assertCorrectClosure(properties, "alibaba_v41.rdf", baseURL,
                RDFFormat.RDFXML);
        
    }

    public void testAlibaba_v41_fast() throws SailInitializationException, IOException, UpdateException {

        Properties properties = new Properties(getProperties());

//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());

        assertCorrectClosure(properties, "alibaba_v41.rdf", baseURL,
                RDFFormat.RDFXML);
        
    }

    /**
     * Read a local test resource and verify that the RDFS closure of that
     * resource is correct. Correctness is judged against the closure of the
     * same resource as computed by the Sesame platform.
     * 
     * @param properties Used to configure the {@link InferenceEngine}.
     * @param resource
     *            (MUST be in the same package as this test class).
     * @param baseURL
     * @param format
     * 
     * @throws IOException
     * @throws SailInitializationException
     * @throws UpdateException
     */
    protected void assertCorrectClosure(Properties properties, String resource,
            String baseURL, RDFFormat format) throws IOException,
            SailInitializationException,
            UpdateException {

        RdfRepository groundTruth = getGroundTruth(resource, baseURL, format);

        AbstractTripleStore store = getStore();

        try {

            store.loadData("/com/bigdata/rdf/inf/" + resource, baseURL, format,
                    true/* verify */, false/* commit */);

            /*
             * Automatically enabled for dumping small stores. 
             */
            final boolean dump = store.getStatementCount() < 200;
            
            if (dump) {
                System.err.println("told triples:");
                store.dumpStore();
            }

            InferenceEngine inf = new InferenceEngine(properties, store);

            inf.computeClosure();

            if (dump) {
                System.err.println("entailed:");
                store.dumpStore(false, true, false);
            }

            store.commit();

            assertTrue(modelsEqual(groundTruth, store));
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }
    
    /**
     * Read the resource into an {@link RdfSchemaRepository}. This
     * automatically computes the closure of the told triples.
     * <p>
     * Note: We treat the closure as computed by the {@link RdfSchemaRepository}
     * as if it were ground truth.
     * 
     * @return The {@link RdfSchemaRepository} with the loaded resource.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    protected RdfRepository getGroundTruth(String resource, String baseURL,
            RDFFormat format) throws SailInitializationException, IOException,
            UpdateException {

        RdfRepository repo = new org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository();

        repo.initialize(new HashMap());

        upload(repo, resource, baseURL, format);

        return repo;
        
    }
    
//    /**
//     * Test of full forward closure.
//     * 
//     * @throws IOException
//     * @throws UpdateException
//     * @throws SailInitializationException
//     * 
//     * @todo make sure that all entailments are either being computed (including
//     *       rdf:type rdfs:Resource) or handled by back-chaining.
//     */
//    public void testFullForwardClosure01() throws IOException, SailInitializationException, UpdateException {
//
//        AbstractTripleStore store = getStore();
//
//        try {
//
//            // @todo modify to load data into a temp store and then compute the
//            // incremental closure.
//
//            store.loadData(resource, baseURL, RDFFormat.RDFXML,
//                    true/* verifyData */, false/* commit */);
//
//            InferenceEngine inf = new InferenceEngine(store);
//    
//            inf.fullForwardClosure();
//            
//            store.commit();
//
//            assertTrue(modelsEqual(getGroundTruth(resource, baseURL,
//                    RDFFormat.RDFXML), store));
//            
//        } finally {
//        
//            store.closeAndDelete();
//            
//        }
//        
//    }
//
//    /**
//     * Test of fast forward closure.
//     * 
//     * @throws IOException
//     * @throws UpdateException
//     * @throws SailInitializationException
//     * 
//     * @todo make sure that all entailments are either being computed (including
//     *       rdf:type rdfs:Resource) or handled by back-chaining.
//     */
//    public void testFastForwardClosure01() throws IOException, SailInitializationException, UpdateException {
//
//        AbstractTripleStore store = getStore();
//        
//        try {
//            
//            // @todo modify to load data into a temp store and then compute the
//            // incremental closure.
//            
//            store.loadData(resource, baseURL, RDFFormat.RDFXML,
//                    true/* verifyData */, false/* commit */);
//
//            InferenceEngine inf = new InferenceEngine(store);
//    
//            inf.fastForwardClosure();
//            
//            store.commit();
//
//            assertTrue(modelsEqual(getGroundTruth(resource, baseURL,
//                    RDFFormat.RDFXML), store));
//
//        } finally {
//        
//            store.closeAndDelete();
//            
//        }
//        
//    }

    /**
     * Uploads an file into an {@link RdfRepository}.
     *
     * @see RdfAdmin
     * 
     * @todo refactor to base class.
     */
    protected void upload(RdfRepository repo, String resource, String baseURL,
            RDFFormat format)
            throws IOException, UpdateException {

        InputStream rdfStream = getClass().getResourceAsStream(resource);

        if (rdfStream == null) {

            // If we do not find as a Resource then try the file system.
            rdfStream = new BufferedInputStream(new FileInputStream(resource));

        }

        try {

            RdfAdmin admin = new RdfAdmin(repo);

            final boolean validate = true;

            admin.addRdfModel(rdfStream, baseURL, new StdOutAdminListener(),
                    format, validate);

        } finally {

            rdfStream.close();

        }

    }

    /*
     * compares two RDF models for equality.
     */

    /**
     * Wraps up the {@link AbstractTripleStore} as an {@link RdfRepository} to
     * facilitate using {@link #modelsEqual(RdfRepository, RdfRepository)} for
     * ground truth testing.
     */
    public boolean modelsEqual(RdfRepository expected,
            AbstractTripleStore actual) throws SailInitializationException {

        RdfRepository repo = new BigdataRdfRepository(actual);
        
        Properties properties = new Properties(getProperties());
        
        properties.setProperty(BigdataRdfRepository.Options.RDFS_CLOSURE,
                "" + true);
        
        repo.initialize( properties );
        
        return modelsEqual(expected,repo);
        
    }
    
    /**
     * Compares two RDF graphs for equality (same statements) - does NOT handle
     * bnodes, which much be treated as variables for RDF semantics.
     * 
     * @todo Sesame probably bundles this logic in a manner that does handle
     *       bnodes.
     * 
     * @param expected
     * 
     * @param actual
     * 
     * @return true if all statements in the expected graph are in the actual
     *         graph and if the actual graph does not contain any statements
     *         that are not also in the expected graph.
     */
    public static boolean modelsEqual(RdfRepository expected,
            RdfRepository actual) {

        Collection<Statement> testRepoStmts = getStatements(expected);

        Collection<Statement> closureRepoStmts = getStatements(actual);

        return compare(testRepoStmts, closureRepoStmts);

    }

    private static ValueFactory simpleFactory = new ValueFactoryImpl();

    private static Collection<Statement> getStatements(RdfRepository repo) {

        Collection<Statement> c = new HashSet<Statement>();

        StatementIterator statIter = repo.getStatements(null, null, null);

        while (statIter.hasNext()) {

            Statement stmt = statIter.next();

            stmt = makeSimple(stmt);

            c.add(stmt);

        }

        statIter.close();

        return c;

    }

    private static Statement makeSimple(Statement stmt) {

        Resource s = stmt.getSubject();
        URI p = stmt.getPredicate();
        Value o = stmt.getObject();

        s = (Resource) makeSimple(s);

        p = (URI) makeSimple(p);

        o = makeSimple(o);

        stmt = simpleFactory.createStatement(s, p, o);

        return stmt;

    }

    private static Value makeSimple(Value v) {

        if (v instanceof URI) {

            v = simpleFactory.createURI(((URI) v).getURI());

        } else if (v instanceof Literal) {

            String label = ((Literal) v).getLabel();
            String language = ((Literal) v).getLanguage();
            URI datatype = ((Literal) v).getDatatype();

            if (datatype != null) {

                v = simpleFactory.createLiteral(label, datatype);

            } else if (language != null) {

                v = simpleFactory.createLiteral(label, language);

            } else {

                v = simpleFactory.createLiteral(label);

            }

        } else {

            v = simpleFactory.createBNode(((BNode) v).getID());

        }

        return v;

    }

    private static boolean compare(Collection<Statement> expectedRepo,
            Collection<Statement> actualRepo) {

        boolean sameStatements = true;

        log("size of 'expected' repository: " + expectedRepo.size());
        log("size of 'actual'   repository: " + actualRepo.size());

        for (Iterator<Statement> it = actualRepo.iterator(); it.hasNext();) {

            Statement stmt = it.next();

            if (!expectedRepo.contains(stmt)) {

                sameStatements = false;

                log("Not expecting: " + stmt);

            }

        }

        log("all the statements in actual in expected? " + sameStatements);

        for (Iterator<Statement> it = expectedRepo.iterator(); it.hasNext();) {

            Statement stmt = it.next();

            if (!actualRepo.contains(stmt)) {

                sameStatements = false;

                log("    Expecting: " + stmt);

            }

        }

        return expectedRepo.size() == actualRepo.size() && sameStatements;

    }

    private static void log(String s) {

        log.info(s);

    }

}
