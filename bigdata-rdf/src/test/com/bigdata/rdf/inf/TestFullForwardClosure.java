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

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.sesame.constants.RDFFormat;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.model.OptimizedValueFactory._URI;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * Test suite for full forward closure.
 * 
 * @todo compute the difference in the entailments generated for the same
 *       datasets between {@link InferenceEngine#fullForwardClosure()} and
 *       {@link InferenceEngine#fastForwardClosure()}
 * 
 * @todo query the database after closure for (?x rdf:type rdfs:Resource). We
 *       don't want it in there unless it was explicitly asserted.
 * 
 * @todo verify that we correctly distinguish Axioms, Explicit, and Inferred
 *       statements. Axioms are checked against those defined by
 *       {@link RdfsAxioms}. Explicit statements are checked against the
 *       dataset w/o closure. The rest of the statements should be marked as
 *       Inferred. Note that an Axiom can be marked as Explicit when loading
 *       data, but that TM needs to convert the statement back to an Axiom if it
 *       is deleted.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFullForwardClosure extends AbstractInferenceEngineTestCase {

    /**
     * 
     */
    public TestFullForwardClosure() {
    }

    /**
     * @param name
     */
    public TestFullForwardClosure(String name) {
        super(name);
    }
    
    /**
     * Test of full forward closure.
     * 
     * @throws IOException
     */
    public void testFullForwardClosure01() throws IOException {

        /*
         * @todo use a dataset that we can add to CVS for a performance test and
         * hand-crafted data sets to test the rule implementations.
         */

        /*
rule        ms  #entms  entms/ms
RuleRdf01   140 38  0
RuleRdfs02  0   0   0
RuleRdfs03  0   0   0
RuleRdfs05  0   592 0
RuleRdfs06  0   202 0
RuleRdfs07  110 97801   889
RuleRdfs08  0   330 0
RuleRdfs09  1687    31680   18
RuleRdfs10  0   330 0
RuleRdfs11  16  990 61
RuleRdfs12  0   0   0
RuleRdfs13  0   0   0

rule        ms  #entms  entms/ms
RuleRdf01   172 38  0
RuleRdfs02  0   0   0
RuleRdfs03  0   0   0
RuleRdfs05  0   592 0
RuleRdfs06  0   202 0
RuleRdfs07  109 97801   897
RuleRdfs08  0   330 0
RuleRdfs09  1687    31680   18
RuleRdfs10  0   330 0
RuleRdfs11  16  990 61
RuleRdfs12  0   0   0
RuleRdfs13  0   0   0
         */
        store.loadData(new File("data/alibaba_v41.rdf"), "", RDFFormat.RDFXML,
                false/* verifyData */, false/* commit */);

//        store.loadData(new File("data/nciOncology.owl"), "", RDFFormat.RDFXML,
//                false/* verifyData */, false/*commit*/);

        /*
         * Wordnet schema + nouns (two source files).
         */
//        store.loadData(new File("data/wordnet-20000620.rdfs"), "",
//                RDFFormat.RDFXML, false/* verifyData */, false/* commit */);
//        store.loadData(new File("data/wordnet_nouns-20010201.rdf"), "",
//                RDFFormat.RDFXML, false/* verifyData */, false/* commit */);
        
        inferenceEngine.fullForwardClosure();
        
        store.commit();
        
    }

    /**
     * Test of fast forward closure.
     * 
     * @throws IOException
     */
    public void testFastForwardClosure01() throws IOException {

        store.loadData(new File("data/alibaba_v41.rdf"), "", RDFFormat.RDFXML,
                false/* verifyData */, false/* commit */);

//        store.loadData(new File("data/nciOncology.owl"), "", RDFFormat.RDFXML,
//                false/* verifyData */, false/*commit*/);

//        store.dumpStore();
        
        /*
         * Wordnet schema + nouns (two source files).
         */
//        store.loadData(new File("data/wordnet-20000620.rdfs"), "",
//                RDFFormat.RDFXML, false/* verifyData */, false/* commit */);
//        store.loadData(new File("data/wordnet_nouns-20010201.rdf"), "",
//                RDFFormat.RDFXML, false/* verifyData */, false/* commit */);
        
        inferenceEngine.fastForwardClosure();
        
        store.commit();

    }

    /**
     * Unit test for
     * {@link InferenceEngine#getSubProperties(com.bigdata.rdf.store.AbstractTripleStore)}
     * 
     */
    public void test_getSubProperties() {
       
        URI A = new _URI("http://www.foo.org/A");
        URI B = new _URI("http://www.foo.org/B");
        URI C = new _URI("http://www.foo.org/C");

        URI rdfsSubPropertyOf = new _URI(RDFS.SUBPROPERTYOF);

        AbstractTripleStore database = store;

//        store.addRdfsAxioms(database);
        
        database.addStatement(A, rdfsSubPropertyOf, rdfsSubPropertyOf);
        database.addStatement(B, rdfsSubPropertyOf, A);

        assertTrue(database.containsStatement(A, rdfsSubPropertyOf, rdfsSubPropertyOf));
        assertTrue(database.containsStatement(B, rdfsSubPropertyOf, A));

        Set<Long> subProperties = inferenceEngine.getSubProperties(database);
        
        assertTrue(subProperties.contains(database.getTermId(rdfsSubPropertyOf)));
        assertTrue(subProperties.contains(database.getTermId(A)));
        assertTrue(subProperties.contains(database.getTermId(B)));

        assertEquals(3,subProperties.size());

        database.addStatement(C, A, A);
        
        assertTrue(database.containsStatement(C, A, A));

        subProperties = inferenceEngine.getSubProperties(database);
        
        assertTrue(subProperties.contains(database.getTermId(rdfsSubPropertyOf)));
        assertTrue(subProperties.contains(database.getTermId(A)));
        assertTrue(subProperties.contains(database.getTermId(B)));
        assertTrue(subProperties.contains(database.getTermId(C)));

        assertEquals(4,subProperties.size());

    }
    
}
