/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Oct 31, 2007
 */

package com.bigdata.rdf.sail;

import java.io.File;
import java.util.Properties;

import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.sesame.sail.RdfRepository;

import com.bigdata.rdf.inf.InferenceEngine;
import com.bigdata.rdf.inf.InferenceEngine.ForwardClosureEnum;
import com.bigdata.rdf.inf.InferenceEngine.Options;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Test suite for RDFS incremental closure correctness (when you load data in a
 * series of batches into the store and compute the closure of the batch against
 * the store rather than re-closing the entire store).
 * 
 * @todo write tests for "Batch" style closure for the DataLoader.
 * 
 * @todo set closure options (fast, backchain vs store x type resource, etc).
 * 
 * @todo run more small tests that focus on specific inferences split across
 *       multiple data file loads.
 * 
 * @todo the wordnet tests blow out the code that verifies to see if the graphs
 *       are the same.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRDFSIncrementalClosure extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestRDFSIncrementalClosure() {
        super();
    }

    /**
     * @param name
     */
    public TestRDFSIncrementalClosure(String name) {
        super(name);
    }

    final private String prefix = "/com/bigdata/rdf/inf/";
    
    /**
     * Trivial example - good for debugging.
     */
    public void test_testIncrementalClosure01_full() throws Exception {

        String[] resource = new String[] {
                prefix+"testIncrementalClosure01-part01.nt",
                prefix+"testIncrementalClosure01-part02.nt" };

        String[] baseURL = new String[] { "", "" 
                };

        RDFFormat[] format = new RDFFormat[] {
                RDFFormat.NTRIPLES,
                RDFFormat.NTRIPLES
                };

        // protect the caller's properties from modification.
        Properties properties = new Properties(getProperties());

        properties.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,ForwardClosureEnum.Full.toString());
        
        assertCorrectClosure(properties,resource,baseURL,format);
                
    }

    /**
     * Trivial example - good for debugging.
     */
    public void test_testIncrementalClosure01_fast() throws Exception {

        String[] resource = new String[] {
                prefix+"testIncrementalClosure01-part01.nt",
                prefix+"testIncrementalClosure01-part02.nt" };

        String[] baseURL = new String[] { "", "" 
                };

        RDFFormat[] format = new RDFFormat[] {
                RDFFormat.NTRIPLES,
                RDFFormat.NTRIPLES
                };

        // protect the caller's properties from modification.
        Properties properties = new Properties(getProperties());

        properties.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,ForwardClosureEnum.Fast.toString());
        
        assertCorrectClosure(properties,resource,baseURL,format);
                
    }

    /**
     * Alibaba data + schema order (resources must exist).
     */
    public void test_alibabaDataSchema() throws Exception {

        String[] resource = new String[] {
                "data/alibaba_data.rdf",
                "data/alibaba_schema.rdf" };

        String[] baseURL = new String[] { "", "" 
                };

        RDFFormat[] format = new RDFFormat[] {
                RDFFormat.RDFXML,
                RDFFormat.RDFXML
                };

        Properties properties = getProperties();
        
        assertCorrectClosure(properties,resource,baseURL,format);
                
    }

    /**
     * Alibaba schema + data order (resources must exist).
     */
    public void test_alibabaSchemaData() throws Exception {

        String[] resource = new String[] {
                "data/alibaba_schema.rdf",
                "data/alibaba_data.rdf"
                };

        String[] baseURL = new String[] { "", "" 
                };

        RDFFormat[] format = new RDFFormat[] {
                RDFFormat.RDFXML,
                RDFFormat.RDFXML
                };

        Properties properties = getProperties();
        
        assertCorrectClosure(properties,resource,baseURL,format);
                
    }

//    /**
//     * Wordnet data + schema order (resources must exist).
//     */
//    public void test_wordnetDataSchema() throws Exception {
//
//        String[] resource = new String[] {
//                "data/wordnet_nouns-20010201.rdf",
//                "data/wordnet-20000620.rdfs" };
//
//        String[] baseURL = new String[] { "", "" 
//                };
//
//        RDFFormat[] format = new RDFFormat[] {
//                RDFFormat.RDFXML,
//                RDFFormat.RDFXML
//                };
//
//        Properties properties = getProperties();
//        
//        assertCorrectClosure(properties,resource,baseURL,format);
//                
//    }
//
//    /**
//     * Wordnet schema + data order (resources must exist).
//     */
//    public void test_wordnetSchemaData() throws Exception {
//
//        String[] resource = new String[] {
//                "data/wordnet_nouns-20010201.rdf",
//                "data/wordnet-20000620.rdfs" };
//
//        String[] baseURL = new String[] { "", "" 
//                };
//
//        RDFFormat[] format = new RDFFormat[] {
//                RDFFormat.RDFXML,
//                RDFFormat.RDFXML
//                };
//
//        Properties properties = getProperties();
//
//        assertCorrectClosure(properties,resource,baseURL,format);
//                
//    }

    /**
     * Test helper for RDFS incremental closure correctness.
     * 
     * @param properties
     * @param resource
     * @param baseURL
     * @param format
     * @throws Exception
     */
    public void assertCorrectClosure(Properties properties, String[] resource,
            String[] baseURL, RDFFormat[] format) throws Exception {
        
        for(String r : resource) {
            
            if(!new File(r).exists()) {
                
                System.err.println("Resource not found: "+r+", test="+getName()+" skipped.");
                
                return;
                
            }
            
        }
        
        RdfRepository groundTruth = getGroundTruth(resource, baseURL, format);

        AbstractTripleStore store = getStore();

        /*
         * Note: overrides properties to make sure that entailments are computed
         * on load (the temporary store against the database).
         */
        
        properties.setProperty(DataLoader.Options.CLOSURE,
                ClosureEnum.Incremental.toString());

        /*
         * Note: overrides properties to make sure that the OWL axioms are
         * not defined since they are not going to be in the graph produced
         * by Sesame.
         */
        properties.setProperty(Options.RDFS_ONLY, "true");

        DataLoader dataLoader = new DataLoader(properties, store);
        
        try {

            // load and close using an incremental approach.
            dataLoader.loadData(resource, baseURL, format);
            
            /*
             * Automatically enabled for dumping small stores. 
             */
            final boolean dump = store.getStatementCount() < 200;
            
            if (dump) {
                System.err.println("told triples:");
                store.dumpStore(true, false, false);
            }

            if (dump) {
                System.err.println("entailed:");
                store.dumpStore(false, true, false);
            }

            assertTrue("Closure does not agree",modelsEqual(groundTruth, dataLoader.getInferenceEngine()));

        } finally {

            store.closeAndDelete();
            
        }

    }

}
