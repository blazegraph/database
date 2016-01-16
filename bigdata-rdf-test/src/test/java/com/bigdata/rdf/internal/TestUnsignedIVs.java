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
 * Created on Jun 7, 2011
 */

package com.bigdata.rdf.internal;

import java.math.BigInteger;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedByteIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedIntIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedLongIV;
import com.bigdata.rdf.internal.impl.literal.XSDUnsignedShortIV;
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
     * kmin(-128)=[0] // unsigned zero
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

        /*
         * Test promote().
         */
        assertEquals((short) 0, MIN_VALUE.promote());
        assertEquals((short) 127, MINUS_ONE.promote());
        assertEquals((short) 128, ZERO.promote());
        assertEquals((short) 129, ONE.promote());
        assertEquals((short) 255, MAX_VALUE.promote());

        // Verify boolean conversion (there are fence posts here).
        assertFalse(MIN_VALUE.booleanValue());
        assertTrue(MINUS_ONE.booleanValue());
        assertTrue(ZERO.booleanValue());
        assertTrue(ONE.booleanValue());
        assertTrue(MAX_VALUE.booleanValue());
        
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
        
        TestEncodeDecodeKeys.doComparatorTest(e);
        
    }

    /**
     * Unit test for xsd:unsignedShort.
     * <p>
     * Points of interest in the value space as reported by {@link KeyBuilder}.
     * 
     * <pre>
     * kmin(-32768)=[0, 0] // unsigned zero.
     * km1(-1)=[127, 255]
     * k0(0)=[128, 0]
     * kp1(1)=[128, 1]
     * kmax(32767)=[255, 255]
     * </pre>
     */
    public void test_xsd_unsignedShort() {
        
        /*
         * Setup values of interest. These appear in their given order when
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
        
        /*
         * Test promote().
         */
        assertEquals(0, MIN_VALUE.promote());
        assertEquals(Short.MAX_VALUE, MINUS_ONE.promote());
        assertEquals(((int) Short.MAX_VALUE) + 1/* 32768 */, ZERO.promote());
        assertEquals(((int) Short.MAX_VALUE) + 2/* 32769 */, ONE.promote());
        assertEquals(0xffff/*2^16-1 */, MAX_VALUE.promote());

        assertEquals(0x00000000, MIN_VALUE.promote());
        assertEquals(0x00007fff/*Short.MAX_VALUE*/, MINUS_ONE.promote());
        assertEquals(0x00008000/*((int) Short.MAX_VALUE) + 1 := 32768 */, ZERO.promote());
        assertEquals(0x00008001/*((int) Short.MAX_VALUE) + 2 := 32769 */, ONE.promote());
        assertEquals(0x0000ffff/*2^16-1 */, MAX_VALUE.promote());

        // Verify boolean conversion (there are fence posts here).
        assertFalse(MIN_VALUE.booleanValue());
        assertTrue(MINUS_ONE.booleanValue());
        assertTrue(ZERO.booleanValue());
        assertTrue(ONE.booleanValue());
        assertTrue(MAX_VALUE.booleanValue());

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

        TestEncodeDecodeKeys.doComparatorTest(e);

    }
    
    public void test_xsd_unsignedInt() {
        
        /*
         * Setup values of interest. These appear in their order when
         * interpreted as unsigned data.
         */

        final XSDUnsignedIntIV<BigdataLiteral> MIN_VALUE = new XSDUnsignedIntIV<BigdataLiteral>(
                Integer.MIN_VALUE);

        final XSDUnsignedIntIV<BigdataLiteral> MINUS_ONE = new XSDUnsignedIntIV<BigdataLiteral>(
                -1);

        final XSDUnsignedIntIV<BigdataLiteral> ZERO = new XSDUnsignedIntIV<BigdataLiteral>(
                0);

        final XSDUnsignedIntIV<BigdataLiteral> ONE = new XSDUnsignedIntIV<BigdataLiteral>(
                1);

        final XSDUnsignedIntIV<BigdataLiteral> MAX_VALUE = new XSDUnsignedIntIV<BigdataLiteral>(
                Integer.MAX_VALUE);

        // Verify IV self-reports as unsigned.
        assertTrue(MIN_VALUE.isUnsignedNumeric());
        
        /*
         * Test promote().
         */
        assertEquals(0L, MIN_VALUE.promote());
        assertEquals((long) Integer.MAX_VALUE, MINUS_ONE.promote());
        assertEquals(((long) Integer.MAX_VALUE) + 1, ZERO.promote());
        assertEquals(((long) Integer.MAX_VALUE) + 2, ONE.promote());
        assertEquals(0xffffffffL, MAX_VALUE.promote());
        //
        assertEquals(0x00000000L, MIN_VALUE.promote());
        assertEquals(0x7fffffffL, MINUS_ONE.promote());
        assertEquals(0x80000000L, ZERO.promote());
        assertEquals(0x80000001L, ONE.promote());
        assertEquals(0xffffffffL, MAX_VALUE.promote());

        // Verify boolean conversion (there are fence posts here).
        assertFalse(MIN_VALUE.booleanValue());
        assertTrue(MINUS_ONE.booleanValue());
        assertTrue(ZERO.booleanValue());
        assertTrue(ONE.booleanValue());
        assertTrue(MAX_VALUE.booleanValue());

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

        TestEncodeDecodeKeys.doComparatorTest(e);

    }
    
    public void test_xsd_unsignedLong() {
        
        /*
         * Setup values of interest. These appear in their order when
         * interpreted as unsigned data.
         */

        final XSDUnsignedLongIV<BigdataLiteral> MIN_VALUE = new XSDUnsignedLongIV<BigdataLiteral>(
                Long.MIN_VALUE);

        final XSDUnsignedLongIV<BigdataLiteral> MINUS_ONE = new XSDUnsignedLongIV<BigdataLiteral>(
                -1L);

        final XSDUnsignedLongIV<BigdataLiteral> ZERO = new XSDUnsignedLongIV<BigdataLiteral>(
                0L);

        final XSDUnsignedLongIV<BigdataLiteral> ONE = new XSDUnsignedLongIV<BigdataLiteral>(
                1L);

        final XSDUnsignedLongIV<BigdataLiteral> MAX_VALUE = new XSDUnsignedLongIV<BigdataLiteral>(
                Long.MAX_VALUE);

        // Verify IV self-reports as unsigned.
        assertTrue(MIN_VALUE.isUnsignedNumeric());
        
        /*
         * Test promote().
         */
        final BigInteger adjust = BigInteger.valueOf(Long.MIN_VALUE).negate();
        assertEquals(BigInteger.valueOf(0x0000000000000000L), MIN_VALUE.promote());
        assertEquals(BigInteger.valueOf(-1).add(adjust), MINUS_ONE.promote());
        assertEquals(adjust, ZERO.promote());
        assertEquals(BigInteger.valueOf(1).add(adjust), ONE.promote());
        assertEquals(BigInteger.valueOf(Long.MAX_VALUE).add(adjust), MAX_VALUE.promote());

        // Verify boolean conversion (there are fence posts here).
        assertFalse(MIN_VALUE.booleanValue());
        assertTrue(MINUS_ONE.booleanValue());
        assertTrue(ZERO.booleanValue());
        assertTrue(ONE.booleanValue());
        assertTrue(MAX_VALUE.booleanValue());

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

        TestEncodeDecodeKeys.doComparatorTest(e);

    }
    
}
