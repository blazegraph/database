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
 * Created on Apr 15, 2009
 */

package com.bigdata.service.ndx.pipeline;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.KVO;

/**
 * Test suite for {@link DefaultDuplicateRemover}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDefaultDuplicateRemover extends TestCase2 {

    /**
     * 
     */
    public TestDefaultDuplicateRemover() {
    }

    /**
     * @param arg0
     */
    public TestDefaultDuplicateRemover(String arg0) {
        super(arg0);
    }

    /**
     * Test of filter which removes identical writes (same key, same value). 
     */
    public void test_filter_keyAndVal() {

        final IDuplicateRemover<Object> fixture = new DefaultDuplicateRemover<Object>(
                false/* filterRefs */);

        final KVO<Object> t0 = new KVO<Object>(new byte[] {}, new byte[] {}, null); 
        final KVO<Object> t1 = new KVO<Object>(new byte[] {1}, new byte[] {1}, null);
        // t2 is dup of t1.
        final KVO<Object> t2 = new KVO<Object>(new byte[] {1}, new byte[] {1}, null);
        // t3 has the same key, but is not a dup
        final KVO<Object> t3 = new KVO<Object>(new byte[] {1}, new byte[] {2}, null);
        final KVO<Object> t4 = new KVO<Object>(new byte[] {1,2}, new byte[] {}, null);

        /*
         * Note: The array is given in ordered order to avoid sort() choosing
         * the ordering for us.
         */ 
        final KVO<Object>[] a = new KVO[] { t0, t1, t2, t3, t4 };

        // filter to remove duplicates.
        final KVO<Object>[] b = fixture.filter(a);

        // should have removed ONE duplicate.
        assertEquals("length", a.length - 1, b.length);

        assertTrue(t0 == b[0]);
        assertTrue(t1 == b[1]);
        assertTrue(t3 == b[2]);
        assertTrue(t4 == b[3]);

    }

    /**
     * Test of filter which removes writes having the same reference (the
     * reference test is used to avoid the key and value comparison).
     */
    public void test_filter_ref() {

        final IDuplicateRemover<Object> fixture = new DefaultDuplicateRemover<Object>(
                false/* filterRefs */);

        final Object o0 = new Object();
        final Object o1 = new Object();
        final Object o2 = new Object();
        final Object o3 = new Object();
        
        final KVO<Object> t0 = new KVO<Object>(new byte[] {}, new byte[] {}, o0); 
        final KVO<Object> t1 = new KVO<Object>(new byte[] {1}, new byte[] {1}, o1);
        // t2 is dup of t1.
        final KVO<Object> t2 = new KVO<Object>(new byte[] {1}, new byte[] {1}, o1);
        // t3 has the same key, but is not a dup
        final KVO<Object> t3 = new KVO<Object>(new byte[] {1}, new byte[] {2}, o2);
        final KVO<Object> t4 = new KVO<Object>(new byte[] {1,2}, new byte[] {}, o3);

        /*
         * Note: The array is given in ordered order to avoid sort() choosing
         * the ordering for us.
         */ 
        final KVO<Object>[] a = new KVO[] { t0, t1, t2, t3, t4 };

        // filter to remove duplicates.
        final KVO<Object>[] b = fixture.filter(a);

        // should have removed ONE duplicate.
        assertEquals("length", a.length - 1, b.length);

        assertTrue(t0 == b[0]);
        assertTrue(t1 == b[1]);
        assertTrue(t3 == b[2]);
        assertTrue(t4 == b[3]);

    }

}
