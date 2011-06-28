/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Jun 7, 2011
 */

package com.bigdata.rdf.internal;

import java.math.BigInteger;
import java.util.Arrays;

import junit.framework.TestCase2;

import org.semanticweb.yars.nx.dt.numeric.XSDUnsignedByte;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Test suite for the xsd:unsigned IV which are internally represented by Java
 * primitives (xsd:unsignedByte, xsd:unsignedShort, xsd:unsignedInt, and
 * xsd:unsignedLong). There is a {@link DTE} for each of these cases, e.g.,
 * {@link DTE#XSDUnsignedByte}. In each case, the corresponding signed primitive
 * is used to encode the {@link IV} so we represent the unsigned byte in one
 * byte, just as we represent the signed byte.
 * <p>
 * These IVs differ in that they must use a widening promotion when they expose
 * the unsigned value to callers. This is done in a manner similar to
 * {@link KeyBuilder}. First we use a widening promotion, e.g., from byte to
 * short. Then we apply the rules to convert from an signed short into an
 * unsigned short. The unsigned short is reported to the caller. The edge case
 * is when we must promote a long. In this case, we promote it to a
 * {@link BigInteger} and then operate on that.
 * <p>
 * These {@link IV}s also use a widening promotion when autoboxing the Java
 * primitive value in order to report it's unsigned value correctly. This the
 * Java numeric class used as the generic type parameter for
 * {@link XSDUnsignedByte} is {@link Short} and not {@link Byte}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnsignedIVs extends TestCase2 {

    /**
     * 
     */
    public TestUnsignedIVs() {
    }

    /**
     * @param name
     */
    public TestUnsignedIVs(String name) {
        super(name);
    }

    /**
     * xsd:unsignedByte.
     * <p>
     * Points of interest in the value space as reported by {@link KeyBuilder}.
     * 
     * <pre>
     * kmin(-128)=[0]
     * km1(-1)=[127]
     * k0(0)=[128]
     * kp1(1)=[129]
     * kmax(127)=[255]
     * </pre>
     * 
     * FIXME The guts of all this should be concentrated somewhere. The data
     * from KeyBuilder is useful but not complete (it does not show us the
     * unsigned version of the value, which is after all what we are after).
     * There are lots of little edges for when something is interpreted as
     * signed and unsigned and especially the ordering implied by those things
     * using different comparators.
     * 
     * TODO It might be more efficient to promote() in the xsd:unsigned
     * constructors or to use set the promote()'d value on a transient cache so
     * we can reuse it.
     */
    public void test_xsd_unsignedByte() {

        /*
         * Setup values of interest. These appear in their order when
         * interpreted as unsigned data.
         */

        final XSDUnsignedByteIV<BigdataLiteral> MIN_VALUE = new XSDUnsignedByteIV<BigdataLiteral>(
                Byte.MIN_VALUE);

        final XSDUnsignedByteIV<BigdataLiteral> MINUS_ONE = new XSDUnsignedByteIV<BigdataLiteral>(
                (byte) -1);

        final XSDUnsignedByteIV<BigdataLiteral> ZERO = new XSDUnsignedByteIV<BigdataLiteral>(
                (byte) 0);

        final XSDUnsignedByteIV<BigdataLiteral> ONE = new XSDUnsignedByteIV<BigdataLiteral>(
                (byte) 1);

        final XSDUnsignedByteIV<BigdataLiteral> MAX_VALUE = new XSDUnsignedByteIV<BigdataLiteral>(
                Byte.MAX_VALUE);

        // Verify IV self-reports as unsigned.
        assertTrue(MIN_VALUE.isUnsignedNumeric());

        // Verify boolean conversion (there are fence posts here).
        assertTrue(MIN_VALUE.booleanValue());
        assertTrue(MINUS_ONE.booleanValue());
        assertFalse(ZERO.booleanValue());
        assertTrue(ONE.booleanValue());
        assertTrue(MAX_VALUE.booleanValue());
        
        /*
         * Test promote().
         */
        assertEquals((short) 0, MIN_VALUE.promote());
        assertEquals((short) 127, MINUS_ONE.promote());
        assertEquals((short) 128, ZERO.promote());
        assertEquals((short) 129, ONE.promote());
        assertEquals((short) 255, MAX_VALUE.promote());

        /*
         * TODO Check more promotions?
         */
        
        /*
         * Test encoding/decoding.
         * 
         * Note: The values in this array are given in their natural _unsigned_
         * order. After the encode/decode test we will sort this array using the
         * comparator for the IV. If the IV comparator semantics are correct the
         * array SHOULD NOT be reordered by the sort.
         */
        
        final IV<?, ?>[] e = {//
                MIN_VALUE,
                MINUS_ONE,
                ZERO,
                ONE,
                MAX_VALUE,
        };

        TestEncodeDecodeKeys.doEncodeDecodeTest(e);

        /*
         * Test ordering.
         */
        final IV[] a = e.clone();

        Arrays.sort(a);

        for (int i = 0; i < a.length; i++) {

            final IV expected = e[i];

            final IV actual = a[i];
            
            if (!expected.equals(actual)) {

                fail("expected[" + i + "]=" + expected + ", but actual[" + i
                        + "]=" + actual);

            }

        }
        
    }

    /**
     * Unit test for xsd:unsignedShort.
     * <p>
     * Points of interest in the value space as reported by {@link KeyBuilder}.
     * 
     * <pre>
     * kmin(-32768)=[0, 0]
     * km1(-1)=[127, 255]
     * k0(0)=[128, 0]
     * kp1(1)=[128, 1]
     * kmax(32767)=[255, 255]
     * </pre>
     */
    public void test_xsd_unsignedShort() {
        
        /*
         * Setup values of interest. These appear in their order when
         * interpreted as unsigned data.
         */

        final XSDUnsignedShortIV<BigdataLiteral> MIN_VALUE = new XSDUnsignedShortIV<BigdataLiteral>(
                Short.MIN_VALUE);

        final XSDUnsignedShortIV<BigdataLiteral> MINUS_ONE = new XSDUnsignedShortIV<BigdataLiteral>(
                (short) -1);

        final XSDUnsignedShortIV<BigdataLiteral> ZERO = new XSDUnsignedShortIV<BigdataLiteral>(
                (short) 0);

        final XSDUnsignedShortIV<BigdataLiteral> ONE = new XSDUnsignedShortIV<BigdataLiteral>(
                (short) 1);

        final XSDUnsignedShortIV<BigdataLiteral> MAX_VALUE = new XSDUnsignedShortIV<BigdataLiteral>(
                Short.MAX_VALUE);

        // Verify IV self-reports as unsigned.
        assertTrue(MIN_VALUE.isUnsignedNumeric());
        
        // Verify boolean conversion (there are fence posts here).
        assertTrue(MIN_VALUE.booleanValue());
        assertTrue(MINUS_ONE.booleanValue());
        assertFalse(ZERO.booleanValue());
        assertTrue(ONE.booleanValue());
        assertTrue(MAX_VALUE.booleanValue());

        /*
         * Test promote().
         */
        assertEquals(0, MIN_VALUE.promote());
        assertEquals(32767, MINUS_ONE.promote());
        assertEquals(32768, ZERO.promote());
        assertEquals(32769, ONE.promote());
        assertEquals(65535, MAX_VALUE.promote());

        /*
         * Test encoding/decoding.
         * 
         * Note: The values in this array are given in their natural _unsigned_
         * order. After the encode/decode test we will sort this array using the
         * comparator for the IV. If the IV comparator semantics are correct the
         * array SHOULD NOT be reordered by the sort.
         */
        
        final IV<?, ?>[] e = {//
                MIN_VALUE,
                MINUS_ONE,
                ZERO,
                ONE,
                MAX_VALUE,
        };

        TestEncodeDecodeKeys.doEncodeDecodeTest(e);

        /*
         * Test ordering.
         */
        final IV[] a = e.clone();

        Arrays.sort(a);

        for (int i = 0; i < a.length; i++) {

            final IV expected = e[i];

            final IV actual = a[i];
            
            if (!expected.equals(actual)) {

                fail("expected[" + i + "]=" + expected + ", but actual[" + i
                        + "]=" + actual);

            }

        }

    }
    
}
