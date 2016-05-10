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
 * Created on Sep 21, 2015
 */
package com.bigdata.rdf.internal;

import junit.framework.TestCase2;

import com.bigdata.service.geospatial.ZOrderRangeScanUtil;

/**
 * Test for utility functionalities required for zOrder index construction,
 * such as {@link ZOrderRangeScanUtil}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestZOrderRangeScanUtility extends TestCase2 {

   /**
    * We implement the test case from Wikipedia: https://en.wikipedia.org/wiki/Z-order_curve,
    * implementing bigmin calculation in a two-dimensional setting.
    */
   public void testBigMinCalculation2Dim() {
      
      final byte[] searchMinZOrder = { Byte.valueOf("00001100",2) /* 12 */ };
      final byte[] searchMaxZOrder = { Byte.valueOf("00101101",2) /* 45 */ };
      
      final ZOrderRangeScanUtil rangeScanUtil = 
         new ZOrderRangeScanUtil(searchMinZOrder, searchMaxZOrder, 2 /* numDimensions */);

      // original scenario
      final byte[] divRecord1 = { Byte.valueOf("00010011", 2) /* 19 */ };
      final byte[] bigMinExp1 = { Byte.valueOf("00100100", 2) /* 36 */ };
      final byte[] bigMinAct1 = rangeScanUtil.calculateBigMin(divRecord1);
      assertEquals(bigMinExp1, bigMinAct1);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord1));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct1));

      // modified (choosing another value leading to the same bigmin)
      final byte[] divRecord2 = { Byte.valueOf("00100000", 2) /* 16 */ };
      final byte[] bigMinExp2 = { Byte.valueOf("00100100", 2) /* 36 */ };
      final byte[] bigMinAct2 = rangeScanUtil.calculateBigMin(divRecord2);
      assertEquals(bigMinExp2, bigMinAct2);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord2));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct2));
 
      // modified (choosing another value leading to the same bigmin)
      final byte[] divRecord3 = { Byte.valueOf("00011111", 2) /* 31 */ };
      final byte[] bigMinExp3 = { Byte.valueOf("00100100", 2) /* 36 */ };
      final byte[] bigMinAct3 = rangeScanUtil.calculateBigMin(divRecord3);
      assertEquals(bigMinExp3, bigMinAct3);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord3));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct3));
 
      // modified (choosing another value leading to the same bigmin)
      final byte[] divRecord4 = { Byte.valueOf("00100011", 2) /* 35 */ };
      final byte[] bigMinExp4 = { Byte.valueOf("00100100", 2) /* 36 */ };
      final byte[] bigMinAct4 = rangeScanUtil.calculateBigMin(divRecord4);
      assertEquals(bigMinExp4, bigMinAct4);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord4));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct4));
 
      // other scenario, leading to bigMin 44
      final byte[] divRecord5 = { Byte.valueOf("00101000", 2) /* 40 */ };
      final byte[] bigMinExp5 = { Byte.valueOf("00101100", 2) /* 44 */ };
      final byte[] bigMinAct5 = rangeScanUtil.calculateBigMin(divRecord5);
      assertEquals(bigMinExp5, bigMinAct5);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord5));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct5));

      // other scenario, leading to bigMin 44
      final byte[] divRecord6 = { Byte.valueOf("00101001", 2) /* 41 */ };
      final byte[] bigMinExp6 = { Byte.valueOf("00101100", 2) /* 44 */ };
      final byte[] bigMinAct6 = rangeScanUtil.calculateBigMin(divRecord6);
      assertEquals(bigMinExp6, bigMinAct6);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord6));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct6));

      // other scenario, leading to bigMin 44
      final byte[] divRecord7 = { Byte.valueOf("00101010", 2) /* 42 */ };
      final byte[] bigMinExp7 = { Byte.valueOf("00101100", 2) /* 44 */ };
      final byte[] bigMinAct7 = rangeScanUtil.calculateBigMin(divRecord7);
      assertEquals(bigMinExp7, bigMinAct7);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord7));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct7));

      // other scenario, leading to bigMin 44
      final byte[] divRecord8 = { Byte.valueOf("00101011", 2) /* 43 */ };
      final byte[] bigMinExp8 = { Byte.valueOf("00101100", 2) /* 44 */ };
      final byte[] bigMinAct8 = rangeScanUtil.calculateBigMin(divRecord8);
      assertEquals(bigMinExp8, bigMinAct8);
      assertFalse(rangeScanUtil.isInSearchRange(divRecord8));
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct8));

   }
   
   
   /**
    * Test for BigMin calculation in a three-dimensional setting.
    */
   public void testBigMinCalculation3Dim() {
      
      final byte[] searchMinZOrder = /* 19 */
         { Byte.valueOf("00000000",2), Byte.valueOf("00000000",2), Byte.valueOf("00010011",2) };
      final byte[] searchMaxZOrder = /* 23 */
         { Byte.valueOf("00000000",2), Byte.valueOf("00000000",2), Byte.valueOf("00010111",2) };

      final ZOrderRangeScanUtil rangeScanUtil = 
            new ZOrderRangeScanUtil(searchMinZOrder, searchMaxZOrder, 3 /* numDimensions */);

      // the following record is not in range for dimension two (has value 0 in that dimension)
      final byte[] dividingRecord = /* 21 */
         { Byte.valueOf("00000000",2), Byte.valueOf("00000000",2), Byte.valueOf("00010101",2) };
      assertFalse(rangeScanUtil.isInSearchRange(dividingRecord));
      
      final byte[] bigMinAct = rangeScanUtil.calculateBigMin(dividingRecord);
      assertTrue(rangeScanUtil.isInSearchRange(bigMinAct));
      assertEquals(searchMaxZOrder, bigMinAct); /* the big min is the max zOrder value */
   }
   
   
   /**
    * Tests the load function in the {@link ZOrderRangeScanUtil} class.
    */
   public void testLoadFunction() {

      /**
       * Load with positive first parameter, 2 dimensions
       */
      final byte[] testByteArray1 =
         { Byte.valueOf("00000000",2), Byte.valueOf("01111111",2), Byte.valueOf("01111111",2) };
      ZOrderRangeScanUtil.load(true, 2, testByteArray1, 2); /* modifies input */
      final byte[] exp1 = 
         { Byte.valueOf("00100000",2), Byte.valueOf("01010101",2), Byte.valueOf("01010101",2) };
      assertEquals(testByteArray1, exp1);

      /**
       * Load with positive first parameter, 2 dimensions
       */
      final byte[] testByteArray2 =
         { Byte.valueOf("00100000",2), Byte.valueOf("01111111",2), Byte.valueOf("00000000",2) };
      ZOrderRangeScanUtil.load(false, 11, testByteArray2, 2); /* modifies input */
      final byte[] exp2 = 
         { Byte.valueOf("00100000",2), Byte.valueOf("01101111",2), Byte.valueOf("01010101",2) };
      assertEquals(testByteArray2, exp2);
      
      /**
       * Load with positive first parameter, 3 dimensions
       */
      final byte[] testByteArray3 =
         { Byte.valueOf("00000000",2), Byte.valueOf("01111111",2), Byte.valueOf("01111111",2) };
      ZOrderRangeScanUtil.load(true, 5, testByteArray3, 3); /* modifies input */
      final byte[] exp3 = 
         { Byte.valueOf("00000100",2), Byte.valueOf("01101101",2), Byte.valueOf("00110110",2) };
      assertEquals(testByteArray3, exp3);
   }

}
