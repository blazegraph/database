/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.bigdata.rdf.lexicon;

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

        final TestSuite suite = new TestSuite("lexicon");

        /*
         * Non-proxy tests.
         */

        // TODO Unit tests for LexPredicate.
        
        // Unit tests for recognizing access patterns for the lexicon indices.
        suite.addTestSuite(TestLexAccessPatternEnum.class);

        /*
         * Tests related to BlobIVs and the BLOBS index.
         */
        
        // Unit tests for operations on the BLOBS index.
        suite.addTestSuite(TestBlobsIndex.class);

        // Unit tests for the TermsWriteTask.
        suite.addTestSuite(TestBlobsWriteTask.class);

        /*
         * Tests related TermIVs and the TERM2ID and TERMS indices.
         */
        
        suite.addTestSuite(TestLexiconKeyBuilder.class);
        
        suite.addTestSuite(TestId2TermTupleSerializer.class);

        // Test for encoding transform for TermIVs used in scale-out.
        suite.addTestSuite(TestTermIdEncoder.class);
        
        // integration tests for adding terms to the lexicon.
        suite.addTestSuite(TestAddTerms.class);
        
        // integration test suite for the vocabulary models.
        suite.addTestSuite(TestVocabulary.class);

        // test suite for the completion scan (prefix match for literals).
        suite.addTestSuite(TestCompletionScan.class);
        
        // test suite for the full-text indexer integration.
        suite.addTestSuite(TestFullTextIndex.class);

        // test suite for inlining
        suite.addTestSuite(TestInlining.class);

        // test suite for the IV cache, including serialization of cached vals.
        suite.addTestSuite(TestIVCache.class);

        // test suite for access paths reading on the TERMS index.
        suite.addTestSuite(TestAccessPaths.class);
        
        // test suite for subject-centric text index
        suite.addTestSuite(TestSubjectCentricFullTextIndex.class);
        
        return suite;
        
    }
    
}
