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

import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sailimpl.memory.RdfSchemaRepository;

import com.bigdata.rdf.inf.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.inf.InferenceEngine.Options;
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

//        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");
        
        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());
        
        assertCorrectClosure(properties,"testClosure01.nt", baseURL, RDFFormat.NTRIPLES);
        
    }

    public void testClosure01_fast() throws SailInitializationException, IOException, UpdateException {
        
        Properties properties = new Properties(getProperties());

//      properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");

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

//      properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");

        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());

        assertCorrectClosure(properties, "alibaba_v41.rdf", baseURL,
                RDFFormat.RDFXML);
        
    }

    public void testAlibaba_v41_fast() throws SailInitializationException, IOException, UpdateException {

        Properties properties = new Properties(getProperties());

//      properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");

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

            assertTrue(modelsEqual(groundTruth, inf));
            
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

}
