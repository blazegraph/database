/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 5, 2008
 */

package com.bigdata.btree.keys;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.bigdata.btree.TestBytesUtil;
import com.bigdata.btree.TestImmutableKeyBuffer;
import com.bigdata.btree.TestKeyBufferSearch;
import com.bigdata.btree.TestMutableKeyBuffer;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestAll extends TestCase {

    /**
     * 
     */
    public TestAll() {
        // TODO Auto-generated constructor stub
    }

    /**
     * @param arg0
     */
    public TestAll(String arg0) {
        super(arg0);
        // TODO Auto-generated constructor stub
    }

    public static Test suite()
    {

        TestSuite suite = new TestSuite("BTree keys");
        
        /*
         * test key encoding and comparison support.
         */
        
        // test methods that compute the successor for various data types.
        suite.addTestSuite( TestSuccessorUtil.class );

        // test key encoding operations.
        suite.addTestSuite(TestKeyBuilder.class);
        suite.addTestSuite(TestJDKUnicodeKeyBuilder.class);
        suite.addTestSuite(TestICUUnicodeKeyBuilder.class);

        return suite;
        
    }
    
}
