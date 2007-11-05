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
package com.bigdata.rdf.inf;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Aggregates test suites into increasing dependency order.
 *
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
    }

    /**
     * Returns a test that will run each of the implementation specific test
     * suites in turn.
     */
    public static Test suite()
    {

        TestSuite suite = new TestSuite("RDF(S)+ inference and truth maintenance");

        /*
         * test forward chainer rules.
         */
        
        // test suite for basic rule mechanisms.
        suite.addTestSuite( TestRule.class );
       
        // @todo SPOAssertionBuffer
        // @todo SPORetractionBuffer

        // test suite for writing, reading, chasing and retracting justifications.
        suite.addTestSuite(TestJustifications.class);
        
        // test suite for rdf1.
        suite.addTestSuite( TestRuleRdf01.class );

        // test that rdfs3 does not let literals into the subject.
        suite.addTestSuite( TestRuleRdfs03.class );
        
        // test suite for rdfs4.
        suite.addTestSuite( TestRuleRdfs04.class );

        // Note: rdfs 2, 3, 7, and 9 use the same base class.
        suite.addTestSuite( TestRuleRdfs07.class );

        // Note: rdfs 6, 8, 10, 12, and 13 use the same base clase.
        suite.addTestSuite( TestRuleRdfs10.class );

        // Note: rdfs 5 and 11 use the same base class.
        suite.addTestSuite( TestRuleRdfs11.class );
        
        // Note: fast closure rules using the same base class.
        suite.addTestSuite( TestRuleFastClosure_11_13.class );

        // Note: fast closure rules using the same base class.
        suite.addTestSuite( TestRuleFastClosure_3_5_6_7_9.class );

        // owl:sameAs rules.
        suite.addTestSuite( TestRuleOwlSameAs.class );

        // test owl:equivilantClass
        suite.addTestSuite( TestRuleOwlEquivalentClass.class );

        // test owl:equivilantProperty
        suite.addTestSuite( TestRuleOwlEquivalentProperty.class );

        /*
         * test backward chainer rules.
         */
        
        // test suite for backward chaining of (?x rdf:type rdfs:Resource).
        suite.addTestSuite( TestBackchainTypeResourceIterator.class );

        // @todo test suite for backward chaining of (?x owl:sameAs ?y).

        /*
         * test SLD / magic sets / semi-naive evaluation (constrained forward
         * chainer that may be used to prove whether or not statement(s) are
         * entailed by the database as opposed to generating entailments from
         * the data).
         */
        
        // @todo test suite for semi-naive evaluation (magic sets / SLD).
//        suite.addTestSuite( TestMagicSets.class);

        /*
         * test truth maintenance.
         */
        
        // test suite for RDFS closure correctness.
        suite.addTestSuite( TestRDFSClosure.class );

        // test suite for RDFS closure correctness with incremental load (TM).
        suite.addTestSuite( TestRDFSIncrementalClosure.class );

        // test suite for RDFS closure correctness with incremental delete (TM).
        suite.addTestSuite( TestRDFSTruthMaintenance.class );
        
        /*
         * @todo write a performance test of concurrent load and close rates
         * using LUBM. This data set is good since it reuses the same ontology
         * and will let us scale the #of concurrent clients and the #of files to
         * be loaded to an arbitrary degree.
         */
        
        return suite;
        
    }
    
}
