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

package com.bigdata.rdf.sail;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.openrdf.sesame.admin.UpdateException;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;
import org.openrdf.sesame.sail.SailInitializationException;

import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.inf.InferenceEngine.Options;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

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
    
    private final String prefix = "/com/bigdata/rdf/inf/";
    
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
        
        assertCorrectClosure(properties,prefix+"testClosure01.nt", ""/*baseURL*/, RDFFormat.NTRIPLES);
        
    }

    public void testClosure01_fast() throws SailInitializationException, IOException, UpdateException {
        
        Properties properties = new Properties(getProperties());

//      properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");

//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());
        
        assertCorrectClosure(properties,prefix+"testClosure01.nt", ""/*baseURL*/, RDFFormat.NTRIPLES);
        
    }

    /**
     * Unit test based on the test resource <code>testClosure01.nt</code>.
     * 
     * @throws SailInitializationException
     * @throws IOException
     * @throws UpdateException
     */
    public void testAlibaba_v41_full() throws SailInitializationException, IOException, UpdateException {

        String resource = "../rdf-data/alibaba_v41.rdf";
        
        if(!new File(resource).exists()) {
         
            System.err.println("Resource not found: "+resource+", test="+getName()+" skipped.");

            return;
            
        }

        Properties properties = new Properties(getProperties());

//      properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");

        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());

        assertCorrectClosure(properties, resource, ""/* baseURL */,
                RDFFormat.RDFXML);
        
    }

    public void testAlibaba_v41_fast() throws SailInitializationException, IOException, UpdateException {

        String resource = "../rdf-data/alibaba_v41.rdf";
        
        if(!new File(resource).exists()) {
         
            System.err.println("Resource not found: "+resource+", test="+getName()+" skipped.");

            return;
            
        }
            
        Properties properties = new Properties(getProperties());

//      properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"true");
        properties.setProperty(Options.FORWARD_CHAIN_RDF_TYPE_RDFS_RESOURCE,"false");

//        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Full.toString());
        properties.setProperty(Options.FORWARD_CLOSURE, ForwardClosureEnum.Fast.toString());

        assertCorrectClosure(properties, resource, ""/* baseURL */,
                RDFFormat.RDFXML);

    }
    
    /**
     * Read a local test resource and verify that the RDFS closure of that
     * resource is correct. Correctness is judged against the closure of the
     * same resource as computed by the Sesame platform.
     * 
     * @param properties
     *            Used to configure the {@link DataLoader} and the
     *            {@link InferenceEngine}.
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

            /*
             * Note: overrides properties to make sure that entailments are NOT computed
             * on load since we want to close the database itself not the loaded data
             * set against the database!
             */
              
            properties.setProperty(DataLoader.Options.CLOSURE, ClosureEnum.None.toString());

            /*
             * Note: overrides properties to make sure that the OWL axioms are
             * not defined since they are not going to be in the graph produced
             * by Sesame.
             */
            properties.setProperty(Options.RDFS_ONLY, "true");

            DataLoader dataLoader = new DataLoader(properties,store);
            
            dataLoader.loadData(resource, baseURL, format);

            /*
             * Automatically enabled for dumping small stores.
             */
            final boolean dump = store.getStatementCount() < 200;
            
            if (dump) {
                System.err.println("told triples:");
                store.dumpStore(true,false,false);
            }

            InferenceEngine inf = new InferenceEngine(properties, store);

            // close the database against itself.
            inf.computeClosure(null);

            if (dump) {
                System.err.println("entailed:");
                store.dumpStore(false, true, false);
            }

            assertTrue(modelsEqual(groundTruth, inf));
            
        } finally {
            
            store.closeAndDelete();
            
        }

    }

}
