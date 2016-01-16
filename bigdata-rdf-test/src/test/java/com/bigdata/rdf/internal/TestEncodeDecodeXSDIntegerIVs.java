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

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataLiteral;

/**
 * Unit tests for {@link XSDIntegerIV}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEncodeDecodeXSDIntegerIVs extends
        AbstractEncodeDecodeKeysTestCase {

    /**
     * 
     */
    public TestEncodeDecodeXSDIntegerIVs() {
    }

    /**
     * @param name
     */
    public TestEncodeDecodeXSDIntegerIVs(String name) {
        super(name);
    }

    /** Unit test for {@link XSDIntegerIV}. */
    public void test_encodeDecode_XSDInteger() {

        final IV<?, ?>[] e = {//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(-1L)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(0L)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(1L)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MAX_VALUE)),//
                new XSDIntegerIV<BigdataLiteral>(BigInteger.valueOf(Long.MIN_VALUE)),//
        };

        doEncodeDecodeTest(e);
        
        doComparatorTest(e);
        
    }

    /**
     * Unit test for {@link XSDIntegerIV} with positive and negative
     * {@link BigInteger}s having a common prefix with varying digits after the
     * prefix.
     */
    public void test_encodeDecode_XSDInteger_pos_and_neg_varying_digits() {

        final BigInteger p1 = new BigInteger("15");
        final BigInteger p2 = new BigInteger("151");
        final BigInteger m1 = new BigInteger("-15");
        final BigInteger m2 = new BigInteger("-151");

        final IV<?,?>[] e = new IV[] {
                new XSDIntegerIV<BigdataLiteral>(p1),//
                new XSDIntegerIV<BigdataLiteral>(p2),//
                new XSDIntegerIV<BigdataLiteral>(m1),//
                new XSDIntegerIV<BigdataLiteral>(m2),//
                };
        
        doEncodeDecodeTest(e);
        
        doComparatorTest(e);

    }

    /**
     * Stress test for {@link XSDIntegerIV}.
     */
    public void test_encodeDecode_XSDInteger_stressTest() {

        final Random r = new Random();

        final List<IV<?,?>> a = new LinkedList<IV<?,?>>();
        
        for (int i = 0; i < 100; i++) {
            
            final BigInteger t1 = BigInteger.valueOf(r.nextLong());
            
            final BigInteger v2 = BigInteger.valueOf(Math.abs(r.nextLong()));
            
            final BigInteger v4 = BigInteger.valueOf(r.nextLong());
            
            // x LT t1
            final BigInteger t2 = t1.subtract(v2);
            final BigInteger t4 = t1.subtract(BigInteger.valueOf(5));
            final BigInteger t5 = t1.subtract(BigInteger.valueOf(9));

            // t1 LT x
            final BigInteger t3 = t1.add(v2);
            final BigInteger t6 = t1.add(BigInteger.valueOf(5));
            final BigInteger t7 = t1.add(BigInteger.valueOf(9));

            a.add(new XSDIntegerIV<BigdataLiteral>(t1));
            a.add(new XSDIntegerIV<BigdataLiteral>(v2));
            a.add(new XSDIntegerIV<BigdataLiteral>(v4));
            a.add(new XSDIntegerIV<BigdataLiteral>(t2));
            a.add(new XSDIntegerIV<BigdataLiteral>(t4));
            a.add(new XSDIntegerIV<BigdataLiteral>(t5));
            a.add(new XSDIntegerIV<BigdataLiteral>(t3));
            a.add(new XSDIntegerIV<BigdataLiteral>(t6));
            a.add(new XSDIntegerIV<BigdataLiteral>(t7));
            
        }
        
        final IV<?, ?>[] e = a.toArray(new IV[0]);

        doEncodeDecodeTest(e);

        doComparatorTest(e);

    }

}
