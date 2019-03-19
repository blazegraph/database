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
/*
 * Created on Feb 15, 2012
 */

package com.bigdata.rdf.internal.encoder;

import java.util.Arrays;

import com.bigdata.bop.Constant;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.Var;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.rdf.internal.IV;

/**
 * Test suite for {@link IVBindingSetEncoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIVBindingSetEncoder extends AbstractBindingSetEncoderTestCase {

    /**
     * 
     */
    public TestIVBindingSetEncoder() {
    }

    public TestIVBindingSetEncoder(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
     
        super.setUp();
        
        // The encoder under test.
        encoder = new IVBindingSetEncoder(valueFactory, false/* filter */);
        
        // The decoder under test (same object as the encoder).
        decoder = (IVBindingSetEncoder) encoder;
        
        // The encoder/decoder does not support IVCache resolution.
        testCache = false;
        
    }

//    protected void tearDown() throws Exception {
//        
//        super.tearDown();
//        
//        // Clear references.
//        encoder.release();
//        encoder = null;
//        decoder = null;
//        
//    }

    
    
    /**
     * https://jira.blazegraph.com/browse/BLZG-4476:
     * Assert that encoding does not change when schema changes incrementally
     */
    public void test_schema_change_simple() {

        final IBindingSet bs1 = new ListBindingSet();
        bs1.set(Var.var("a"), new Constant<IV<?, ?>>(termId));

        final IBindingSet bs2 = new ListBindingSet();
        bs2.set(Var.var("a"), new Constant<IV<?, ?>>(termId2));
        bs2.set(Var.var("b"), new Constant<IV<?, ?>>(inlineIV1));

        // encode twice (and make sure we get the same result)
        final byte[] val1a = encoder.encodeSolution(bs1);
        final byte[] val1b = encoder.encodeSolution(bs1);

        assertEquals(val1a, val1b);

        // this one extends the schema
        encoder.encodeSolution(bs2);
        
        // re-encode binding set bs1, now over the new schema, and
        // make sure we still get the same result
        final byte[] val1c = encoder.encodeSolution(bs1);
        
        assertEquals(val1b, val1c);
        
        final IBindingSet decoded1 = 
                decoder.decodeSolution(val1a, 0/* off */,
                val1a.length/* len */, true/* resolveCachedValues */);
        
        assertEquals(bs1, decoded1);

    }
    
    /**
     * https://jira.blazegraph.com/browse/BLZG-4476:
     * Assert that encoding does not change when schema changes incrementally
     */

    public void test_schema_change_complex() {
        
        final IBindingSet bs1 = new ListBindingSet();
        bs1.set(Var.var("a"), new Constant<IV<?, ?>>(inlineIV1));

        // extends the schema
        final IBindingSet bs2 = new ListBindingSet();
        bs2.set(Var.var("a"), new Constant<IV<?, ?>>(inlineIV2));
        bs2.set(Var.var("b"), new Constant<IV<?, ?>>(inlineIV1));

        // must differ from bs1 (different variable)
        final IBindingSet bs3 = new ListBindingSet();
        bs3.set(Var.var("b"), new Constant<IV<?, ?>>(inlineIV1));

        // same as bs1
        final IBindingSet bs4 = new ListBindingSet();
        bs4.set(Var.var("a"), new Constant<IV<?, ?>>(inlineIV1));

        // extends the schema
        final IBindingSet bs5 = new ListBindingSet();
        bs5.set(Var.var("c"), new Constant<IV<?, ?>>(inlineIV1));

        // must differ from bs2
        final IBindingSet bs6 = new ListBindingSet();
        bs6.set(Var.var("a"), new Constant<IV<?, ?>>(inlineIV2));
        bs6.set(Var.var("c"), new Constant<IV<?, ?>>(inlineIV1));

        // same as bs3
        final IBindingSet bs7 = new ListBindingSet();
        bs7.set(Var.var("b"), new Constant<IV<?, ?>>(inlineIV1));
        
        // same as bs5
        final IBindingSet bs8 = new ListBindingSet();
        bs8.set(Var.var("c"), new Constant<IV<?, ?>>(inlineIV1));

        // same as bs6
        final IBindingSet bs9 = new ListBindingSet();
        bs9.set(Var.var("a"), new Constant<IV<?, ?>>(inlineIV2));
        bs9.set(Var.var("c"), new Constant<IV<?, ?>>(inlineIV1));
        
        // encode twice (and make sure we get the same result)
        final byte[] val1 = encoder.encodeSolution(bs1);
        final byte[] val2 = encoder.encodeSolution(bs2);
        final byte[] val3 = encoder.encodeSolution(bs3);
        final byte[] val4 = encoder.encodeSolution(bs4);
        final byte[] val5 = encoder.encodeSolution(bs5);
        final byte[] val6 = encoder.encodeSolution(bs6);
        final byte[] val7 = encoder.encodeSolution(bs7);
        final byte[] val8 = encoder.encodeSolution(bs8);
        final byte[] val9 = encoder.encodeSolution(bs9);

        assertFalse(Arrays.equals(val1, val3)); // bs1 and bs3 encode different variables
        assertEquals(bs1, bs4); // equal binding set
        assertFalse(Arrays.equals(val2, val6)); // bs2 and bs6 encode different variables
        assertEquals(val3, val7); // equal binding set
        assertEquals(val5, val8); // equal binding set
        assertEquals(val6, val9); // equal binding set

        final IBindingSet decoded1 = 
                decoder.decodeSolution(val1, 0/* off */,
                val1.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded2 = 
                decoder.decodeSolution(val2, 0/* off */,
                val2.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded3 = 
                decoder.decodeSolution(val3, 0/* off */,
                val3.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded4 = 
                decoder.decodeSolution(val4, 0/* off */,
                val4.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded5 = 
                decoder.decodeSolution(val5, 0/* off */,
                val5.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded6 = 
                decoder.decodeSolution(val6, 0/* off */,
                val6.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded7 = 
                decoder.decodeSolution(val7, 0/* off */,
                val7.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded8 = 
                decoder.decodeSolution(val8, 0/* off */,
                val8.length/* len */, true/* resolveCachedValues */);
        final IBindingSet decoded9 = 
                decoder.decodeSolution(val9, 0/* off */,
                val9.length/* len */, true/* resolveCachedValues */);
        
        assertEquals(bs1, decoded1);
        assertEquals(bs2, decoded2);
        assertEquals(bs3, decoded3);
        assertEquals(bs4, decoded4);
        assertEquals(bs5, decoded5);
        assertEquals(bs6, decoded6);
        assertEquals(bs7, decoded7);
        assertEquals(bs8, decoded8);
        assertEquals(bs9, decoded9);
        
    }
}
