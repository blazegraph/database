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
 * Created on May 21, 2007
 */

package com.bigdata.rdf.scaleout;

import java.io.File;
import java.io.IOException;

import org.openrdf.sesame.constants.RDFFormat;

import com.bigdata.journal.BufferMode;
import com.bigdata.rdf.rio.PresortRioLoader;
import com.bigdata.rdf.rio.TestRioIntegration;
import com.bigdata.rdf.store.ScaleOutTripleStore;

/**
 * Note: The commit flag is ignored for the {@link ScaleOutTripleStore}.
 * 
 * @todo try with {@link BufferMode#Disk} when testing on a resource starved
 *       system (e.g., a laptop).
 * 
 * @todo there is no reason for the {@link PresortRioLoader} to do one operation
 *       per type of term (uri, bnode or literal).  That should just fall out of
 *       how we partition the indices.
 * 
 * @todo partition the terms index at least for literals (by type), URIs, and
 *       bnodes.
 * 
 * @todo partition the ids index every 1M ids.
 * 
 * @todo write a test of concurrent load rates using LUBM. This data set is good
 *       since it reuses the same ontology and will let us scale the #of
 *       concurrent clients and the #of files to be loaded to an arbitrary
 *       degree.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDistributedTripleStoreLoadRate extends AbstractDistributedTripleStoreTestCase {

    /**
     * 
     */
    public TestDistributedTripleStoreLoadRate() {
    }

    /**
     * @param arg0
     */
    public TestDistributedTripleStoreLoadRate(String arg0) {
        super(arg0);
    }

    public void test_loadNCIOncology() throws IOException {

        store.loadData(new File("data/nciOncology.owl"), "", RDFFormat.RDFXML,
                false, false /*commit*/);

    }

//    protected String[] testData = new String[] {
//            "data/nciOncology.owl" // nterms := 289844
////            "data/wordnet_nouns-20010201.rdf"
////            "data/taxonomy.rdf"
//            };

    /**
     * Runs the test RDF/XML load.
     */
    public static void main(String[] args) throws Exception {
        
        TestDistributedTripleStoreLoadRate test = new TestDistributedTripleStoreLoadRate("TestInsertRate");
        test.setUp();
        test.test_loadNCIOncology();
        test.tearDown();
            
    }

}
