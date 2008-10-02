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
/*
 * Created on Aug 10, 2007
 */

package com.bigdata.sparse;

import junit.extensions.proxy.ProxyTestSuite;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.journal.AbstractIndexManagerTestCase;
import com.bigdata.journal.IIndexManager;
import com.bigdata.service.TestJournal;
import com.bigdata.service.TestLDS;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        super();
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

        TestSuite suite = new TestSuite("Sparse Row Store");

        // value encoding and decoding.
        suite.addTestSuite(TestValueType.class);
        
        // sparse property set object.
        suite.addTestSuite(TestTPS.class);

        // encoding and decoding of keys.
        suite.addTestSuite(TestKeyEncodeDecode.class);
        
        // test row store backed by a Journal.
        suite.addTest(proxySuite(new TestJournal("Journal row store"),"Journal"));

        // test row store backed by LDS.
        suite.addTest(proxySuite(new TestLDS("LDS row store"),"LDS"));

        /*
         * @todo test against EDS.
         * 
         * @todo test when the index is statically partitioned.
         * 
         * @todo test consistent across split/join operations.
         * 
         * 
         * 
         * @todo use of btree to support column store (in another package)?
         * 
         * @todo test version expiration based on age?
         * 
         * @todo test version expiration based on #of versions?
         */
        
        return suite;
        
    }
    
    /**
     * Create and populate a {@link ProxyTestSuite} with the unit tests that we
     * will run against any of the {@link IIndexManager} implementations.
     * 
     * @param delegate
     *            The delegate for the proxied unit tests.
     * @param name
     *            The name of the test suite.
     * @return The {@link ProxyTestSuite} populated with the unit tests.
     */
    protected static ProxyTestSuite proxySuite(
            AbstractIndexManagerTestCase<? extends IIndexManager> delegate, String name) {

        final ProxyTestSuite suite = new ProxyTestSuite(delegate, name);

        // sparse row store operations.
        suite.addTestSuite(TestSparseRowStore.class);
        
        return suite;
        
    }

}
