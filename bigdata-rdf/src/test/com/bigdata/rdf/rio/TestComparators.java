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
 * Created on Jan 29, 2007
 */

package com.bigdata.rdf.rio;

import java.util.Arrays;
import java.util.Comparator;

import junit.framework.TestCase2;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.btree.BytesUtil.UnsignedByteArrayComparator;
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
