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
 * Created on Jun 17, 2011
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.impl.literal.XSDDecimalIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Unit tests for {@link XSDDecimalIV}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEncodeDecodeXSDDecimalIVs extends
        AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public TestEncodeDecodeXSDDecimalIVs() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeXSDDecimalIVs(String name) {
        super(name);
    }

    /**
     * Unit test of {@link XSDDecimalIV} with positive and negative
     * {@link BigDecimal}s having varying digits after the decimals.
     */
    public void test_encodeDecode_XSDDecimal_pos_and_neg_varying_digits() {

        final BigDecimal p1 = new BigDecimal("1.5");
        final BigDecimal p2 = new BigDecimal("1.51");
        final BigDecimal m1 = new BigDecimal("-1.5");
        final BigDecimal m2 = new BigDecimal("-1.51");

        final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(p1),//
                new XSDDecimalIV<BigdataLiteral>(p2),//
                new XSDDecimalIV<BigdataLiteral>(m1),//
                new XSDDecimalIV<BigdataLiteral>(m2),//
                };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }


    /**
     * Unit test of {@link XSDDecimalIV} with trailing zeros. The encoded and
     * decoded values are consistent, but trailing zeros are removed by the
     * {@link XSDDecimalIV} encoding.
     */
    public void test_encodeDecode_XSDDecimal_trailingZeros() {

        final BigDecimal p1 = new BigDecimal("1.50");
        final BigDecimal p2 = new BigDecimal("1.500");

        final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(p1),//
                new XSDDecimalIV<BigdataLiteral>(p2),//
                };

//        for(IV t : e) {
//            System.err.println(t.toString()+" : "+((XSDDecimalIV) t).getLabel());
//        }
        
        final IV<?,?>[] a = doEncodeDecodeTest(e);

//        for(IV t : a) {
//            System.err.println(t.toString()+" : "+((XSDDecimalIV) t).getLabel());
//        }

        doComparatorTest(e);

        doComparatorTest(a);

        final IV<?, ?>[] b = new IV[] { e[0], e[1], a[0], a[1] };

        doComparatorTest(b);

    }

    /**
     * Unit test demonstrates that precision is not preserved by the encoding.
     * Thus, ZEROs are encoded in the same manner regardless of their precision
     * (this is true of other values with trailing zeros after the decimal point
     * as well).
     */
    public void test_BigDecimal_zeroPrecisionNotPreserved() {

        final IKeyBuilder keyBuilder = new KeyBuilder();

        // Three ZEROs with different precision.
        final BigDecimal z0 = new BigDecimal("0");
        final BigDecimal z1 = new BigDecimal("0.0");
        final BigDecimal z2 = new BigDecimal("0.00");
        
        final IV<?,?> v0 = new XSDDecimalIV<BigdataLiteral>(z0);
        final IV<?,?> v1 = new XSDDecimalIV<BigdataLiteral>(z1);
        final IV<?,?> v2 = new XSDDecimalIV<BigdataLiteral>(z2);

        // Encode each of those BigDecimal values.
        final byte[] b0 = IVUtility.encode(keyBuilder.reset(), v0).getKey();
        final byte[] b1 = IVUtility.encode(keyBuilder.reset(), v1).getKey();
        final byte[] b2 = IVUtility.encode(keyBuilder.reset(), v2).getKey();

        // The encoded representations are the same.
        assertEquals(b0, b1);
        assertEquals(b0, b2);
        
    }

    public void test_BigDecimal_500() {

        final IKeyBuilder keyBuilder = new KeyBuilder();

//        final BigDecimal v = new BigDecimal("500"); // NB: Decodes as 5E+2!
        final BigDecimal v = new BigDecimal("5E+2");
        
        final IV<?,?> iv = new XSDDecimalIV<BigdataLiteral>(v);

        final byte[] b0 = IVUtility.encode(keyBuilder.reset(), iv).getKey();

        final IV<?,?> iv2 = IVUtility.decode(b0);

        final byte[] b2 = IVUtility.encode(keyBuilder.reset(), iv2).getKey();

        // The decoded IV compares as equals()
        assertEquals(iv, iv2);

        // The encoded representations are the same.
        assertEquals(b0, b2);

    }

    /**
     * Unit tests using xsd:decimal.
     */
    public void test_encodeDecode_XSDDecimal() {

        final BigDecimal z1 = new BigDecimal("0");
//        final BigDecimal z1 = new BigDecimal("0.0"); // NB: Will decode as "0"
//        final BigDecimal negz1 = new BigDecimal("-0.0"); // NB: Will decode as "0". 
//        final BigDecimal z2 = new BigDecimal("0.00"); // NB: Will decode as "0".
        final BigDecimal p1 = new BigDecimal("0.01");
        final BigDecimal negp1 = new BigDecimal("-0.01");
//        final BigDecimal z3 = new BigDecimal("0000.00"); // NB: Will decode as "0".
        final BigDecimal m1 = new BigDecimal("1.5");
        final BigDecimal m2 = new BigDecimal("-1.51");
        final BigDecimal m5 = new BigDecimal("5");
//        final BigDecimal m53 = new BigDecimal("5.000"); // NB: Will decode as "5".
        final BigDecimal m500 = new BigDecimal("5E+2"); // 
//        final BigDecimal m500 = new BigDecimal("500"); // NB: Will decode as "5E+2"!
//        final BigDecimal m500x = new BigDecimal("00500"); // NB: Will decode as "500".
//        final BigDecimal m5003 = new BigDecimal("500.000"); // NB: Will decode as "500".
        final BigDecimal v1 = new BigDecimal("383.00000000000001");
        final BigDecimal v2 = new BigDecimal("383.00000000000002");

        final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(z1),//
//                new XSDDecimalIV<BigdataLiteral>(negz1),//
//                new XSDDecimalIV<BigdataLiteral>(z2),//
                new XSDDecimalIV<BigdataLiteral>(p1),//
                new XSDDecimalIV<BigdataLiteral>(negp1),//
//                new XSDDecimalIV<BigdataLiteral>(z3),//
                new XSDDecimalIV<BigdataLiteral>(m1),//
                new XSDDecimalIV<BigdataLiteral>(m2),//
                new XSDDecimalIV<BigdataLiteral>(m5),//
//                new XSDDecimalIV<BigdataLiteral>(m53),//
                new XSDDecimalIV<BigdataLiteral>(m500),//
//                new XSDDecimalIV<BigdataLiteral>(m5003),//
                new XSDDecimalIV<BigdataLiteral>(v1),//
                new XSDDecimalIV<BigdataLiteral>(v2),//
                };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

//    /**
//     * Unit test for {@link XSDDecimalIV}.
//     */
//    public void test_encodeDecode_XSDDecimal_2() {
//
//        final IV<?,?>[] e = new IV[] {
//            
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0)),//
//            
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-123450)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-99)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-9)),//
//            
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(8.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(255.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(256.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(512.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1028.001)),//
//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-8.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-255.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-256.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-512.0001)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1028.001)),//
//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MIN_VALUE)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MAX_VALUE)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MIN_VALUE - 1)),//
//            new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(Double.MAX_VALUE + 1)),//
//            };
//
//        doEncodeDecodeTest(e);
//        
//        doComparatorTest(e);
//
//    }
    
    /**
     * Unit test for {@link XSDDecimalIV}.
     */
    public void test_encodeDecode_XSDDecimal_3() {

      final IV<?,?>[] e = new IV[] {
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(2.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(1.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(0.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-2.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(-1.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(10.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(11.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(258.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(259.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(3.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(259.01)),//
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(383.01)),
                new XSDDecimalIV<BigdataLiteral>(BigDecimal.valueOf(383.02)),//
      };

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

    /**
     * Stress test for {@link XSDDecimalIV}.
     */
    public void test_encodeDecode_XSDDecimal_stressTest() {

        final Random r = new Random();

        final List<IV<?,?>> a = new LinkedList<IV<?,?>>();
        
        for (int i = 0; i < 100; i++) {
            
            final BigDecimal t1 = BigDecimal.valueOf(r.nextDouble());
            
            final BigDecimal v2 = BigDecimal.valueOf(Math.abs(r.nextDouble()));
            
            final BigDecimal v4 = BigDecimal.valueOf(r.nextDouble());
            
            // x LT t1
            final BigDecimal t2 = t1.subtract(v2);
            final BigDecimal t4 = t1.subtract(BigDecimal.valueOf(5));
            final BigDecimal t5 = t1.subtract(BigDecimal.valueOf(9));

            // t1 LT x
            final BigDecimal t3 = t1.add(v2);
            final BigDecimal t6 = t1.add(BigDecimal.valueOf(5));
            final BigDecimal t7 = t1.add(BigDecimal.valueOf(9));

            a.add(new XSDDecimalIV<BigdataLiteral>(t1));
            a.add(new XSDDecimalIV<BigdataLiteral>(v2));
            a.add(new XSDDecimalIV<BigdataLiteral>(v4));
            a.add(new XSDDecimalIV<BigdataLiteral>(t2));
            a.add(new XSDDecimalIV<BigdataLiteral>(t4));
            a.add(new XSDDecimalIV<BigdataLiteral>(t5));
            a.add(new XSDDecimalIV<BigdataLiteral>(t3));
            a.add(new XSDDecimalIV<BigdataLiteral>(t6));
            a.add(new XSDDecimalIV<BigdataLiteral>(t7));
            
        }
        
        final IV<?, ?>[] e = a.toArray(new IV[0]);

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

}
