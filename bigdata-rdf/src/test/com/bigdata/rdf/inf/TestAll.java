/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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
        
        // test suite for the axiom models.
        suite.addTestSuite( TestAxiomModel.class );
        
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

        // compare two means of computing owl:sameAs for equivilence.
        suite.addTestSuite( TestCompareOwlSameAsEntailments.class );
        
        /*
         * test backward chainer rules.
         */
        
        // test suite for backward chaining of (?x rdf:type rdfs:Resource).
        suite.addTestSuite( TestBackchainTypeResourceIterator.class );

        // test suite for backward chaining of owl:sameAs {2,3}
        suite.addTestSuite( TestBackchainOwlSameAs.class );

        /*
         * @todo do some rigerous tests of the forward chainer to make sure that
         * it is properly computing the RDFS entailments. There are tests of
         * this in the bigdata-sails project, but they rely on testing against
         * the Sesame in-memory repository as ground truth.  It would be nice to
         * run some W3C test suites or the like here.
         */
        
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
        
        // test suite for basic TM mechansism encapsulated by this class.
        suite.addTestSuite( TestTMStatementBuffer.class );
        
        /*
         * @todo write a performance test of concurrent load and close rates
         * using LUBM. This data set is good since it reuses the same ontology
         * and will let us scale the #of concurrent clients and the #of files to
         * be loaded to an arbitrary degree.
         */
        
        return suite;
        
    }
    
}
