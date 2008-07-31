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
 * Created on Jul 15, 2008
 */

package com.bigdata.rdf.rules;

import java.io.IOException;
import java.util.Properties;
import org.openrdf.rio.RDFFormat;
import org.openrdf.sail.SailException;
import com.bigdata.rdf.rio.LoadStats;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;
import com.bigdata.rdf.store.TempTripleStore;
import com.bigdata.rdf.store.DataLoader.ClosureEnum;

/**
 * Test suite comparing full fix point closure of RDFS entailments against the
 * fast closure program for some known data sets (does not test truth
 * maintenance under assertion and retraction or the justifications).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestCompareFullAndFastClosure extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestCompareFullAndFastClosure() {
    }

    /**
     * @param name
     */
    public TestCompareFullAndFastClosure(String name) {
        super(name);
    }

    public void test_compareEntailments() throws IOException, SailException {
        
        // String[] resource = new String[]{"/com/bigdata/rdf/rules/testOwlSameAs.rdf"};
        // String[] resource = new String[]{"/com/bigdata/rdf/rules/testOwlSameAs.rdf"};
        String[] resource = new String[]{"/com/bigdata/rdf/rules/small.rdf"};
        String[] baseURL = new String[]{""};
        RDFFormat[] format = new RDFFormat[]{RDFFormat.RDFXML};

        doCompareEntailments(resource, baseURL, format);
        
    }
    
    /**
     * 
     * Note: This always uses {@link TempTripleStore} rather than whatever is
     * configured since we need to create two stores and we need control over
     * the options for each store.
     * 
     * @param resource
     * @param baseURL
     * @param format
     * @throws IOException
     */
    protected void doCompareEntailments(String resource[], String baseURL[],
            RDFFormat[] format) throws IOException, SailException {

        final AbstractTripleStore store1;
        final AbstractTripleStore store2;
        
        Properties properties = new Properties(getProperties());
        
        // close each set of resources after it has been loaded.
        properties.setProperty(DataLoader.Options.CLOSURE,
                DataLoader.ClosureEnum.Batch.toString());

        properties.setProperty(DataLoader.Options.CLOSURE,
                DataLoader.ClosureEnum.Batch.toString());

        { // use the "full" forward closure.

            Properties tmp = new Properties(properties);

            tmp.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,
                    InferenceEngine.ForwardClosureEnum.Full.toString());
/*            
            tmp.setProperty(DataLoader.Options.CLOSURE,
                    ClosureEnum.None.toString());
*/
            store1 = new TempTripleStore(tmp);

        }

        { // use the "fast" forward closure.

            Properties tmp = new Properties(properties);

            tmp.setProperty(InferenceEngine.Options.FORWARD_CLOSURE,
                    InferenceEngine.ForwardClosureEnum.Fast.toString());
/*
            tmp.setProperty(DataLoader.Options.CLOSURE,
                    ClosureEnum.None.toString());
*/
            store2 = new TempTripleStore(tmp);

        }
        
        try {

            {

                LoadStats loadStats = store1.getDataLoader().loadData(resource,
                        baseURL, format);
                
                // store1.getInferenceEngine().computeClosure(null);
                
                System.err.println("Full forward closure: " + loadStats);
                
                System.err.println(store1.dumpStore(store1, true, true, false, true));
                
            }
            
            {

                LoadStats loadStats = store2.getDataLoader().loadData(resource,
                        baseURL, format);
                
                // store2.getInferenceEngine().computeClosure(null);
                
                System.err.println("Fast forward closure: " + loadStats);
                
                System.err.println(store2.dumpStore(store2, true, true, false, true));
                
            }
        
            assertTrue(modelsEqual(store1, store2));
            
        } finally {
            
            store1.closeAndDelete();
            store2.closeAndDelete();
            
        }
        
    }
    
}
