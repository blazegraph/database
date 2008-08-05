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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.lexicon;

import java.util.Arrays;
import java.util.Comparator;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.model.OptimizedValueFactory.TermIdComparator;
import com.bigdata.rdf.model.OptimizedValueFactory._Literal;
import com.bigdata.rdf.model.OptimizedValueFactory._Value;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestComparators extends TestCase2 {

    /**
     * 
     */
    public TestComparators() {
    }

    /**
     * @param name
     */
    public TestComparators(String name) {
        super(name);
    }

    public void test_termIdComparator() {

        final long lmin = Long.MIN_VALUE;
        final long lm1 = -1L;
        final long l0 = 0L;
        final long lp1 = 1L;
        final long lmax = Long.MAX_VALUE;

        final _Value vmin = new _Literal("a"); vmin.termId = lmin;
        final _Value vm1  = new _Literal("b"); vm1.termId = lm1;
        final _Value v0   = new _Literal("c"); v0.termId = l0;
        final _Value vp1  = new _Literal("d"); vp1.termId = lp1;
        final _Value vmax = new _Literal("e"); vmax.termId = lmax;

        // ids out of order.
        long[] actualIds = new long[] { lm1, lmax, l0, lp1, lmin };
        // ids in order.
        long[] expectedIds = new long[] { lmin, lm1, l0, lp1, lmax };
        
        // values out of order.
        _Value[] terms = new _Value[] { vmax, vm1, vmin, v0, vp1 };

        /*
         * Test conversion of longs to unsigned byte[]s using the KeyBuilder and
         * verify the order on those unsigned byte[]s.
         */
        {
            byte[][] keys = new byte[actualIds.length][];
            KeyBuilder keyBuilder = new KeyBuilder(8);
            for(int i=0; i<actualIds.length; i++) {
                keys[i] = keyBuilder.reset().append(actualIds[i]).getKey();
            }
            Arrays.sort(keys,UnsignedByteArrayComparator.INSTANCE);
            for(int i=0;i<actualIds.length;i++) {
                System.err.println(BytesUtil.toString(keys[i]));
                /*
                 * Decode and verify sorted into the expected order.
                 */
                assertEquals(expectedIds[i], KeyBuilder.decodeLong(keys[i],0));
            }
        }
        
        /*
         * Test unsigned long integer comparator.
         */

        System.err.println("unsorted ids  : "+Arrays.toString(actualIds));
        Arrays.sort(actualIds);
        System.err.println("sorted ids    : "+Arrays.toString(actualIds));
        System.err.println("expected ids  : "+Arrays.toString(expectedIds));
        assertEquals("ids order",expectedIds,actualIds);

        /*
         * Test the term identifier comparator.
         */
        
        Comparator<_Value> c = TermIdComparator.INSTANCE;

        System.err.println("unsorted terms: "+Arrays.toString(terms));
        Arrays.sort(terms,c);
        System.err.println("sorted terms  : "+Arrays.toString(terms));

        assertTrue("kmin<km1", c.compare(vmin, vm1) < 0);
        assertTrue("km1<k0", c.compare(vm1, v0) < 0);
        assertTrue("k0<kp1", c.compare(v0, vp1) < 0);
        assertTrue("kp1<kmax", c.compare(vp1, vmax) < 0);
        assertTrue("kmin<kmax", c.compare(vmin, vmax) < 0);

    }
    
}
