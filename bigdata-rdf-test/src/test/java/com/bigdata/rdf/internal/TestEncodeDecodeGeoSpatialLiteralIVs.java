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
 * Created on August 31, 2015
 */

package com.bigdata.rdf.internal;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.URI;

import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription.Datatype;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.BigdataValueFactoryImpl;
import com.bigdata.service.geospatial.GeoSpatial;

/**
 * Unit tests for {@link GeoSpatialLiteralExtension}.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class TestEncodeDecodeGeoSpatialLiteralIVs extends
      AbstractEncodeDecodeKeysTestCase {

   /**
     * 
     */
   public TestEncodeDecodeGeoSpatialLiteralIVs() {
   }

   /**
    * @param name
    */
   public TestEncodeDecodeGeoSpatialLiteralIVs(String name) {
      super(name);
   }

   /**
    * Unit test for round-trip of GeoSpatial literals of lat+lon+time
    * GeoSpatial literals.
    */
   public void test_encodeDecodeLatLonTimeGeoSpatialLiterals() throws Exception {

      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
      
      final GeoSpatialLiteralExtension<BigdataValue> ext = 
         getLatLonTimeGSLiteralExtension(vf);
      
      encodeDecodeGeoSpatialLiterals(
         vf, getDummyGeospatialLiteralsLatLonTime(vf), ext);
   }

   
   /**
    * Unit test for round-trip of GeoSpatial literals of lat+lon
    * GeoSpatial literals.
    */
   public void test_encodeDecodeLatLonGeoSpatialLiterals() throws Exception {

      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
      
      final GeoSpatialLiteralExtension<BigdataValue> ext = 
         getLatLonGSLiteralExtension(vf);
      
      encodeDecodeGeoSpatialLiterals(
         vf, getDummyGeospatialLiteralsLatLon(vf), ext);
   }
   
   /**
    * Unit test asserting correct rejectance (error message) when passing
    * in literals that are incompatible with the datatype.
    */
   public void test_encodeDecodeGeoSpatialLiteralsWrongFormat() throws Exception {

      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
      
      
      // direction one: processing lat+lon literals with lat+lon+time datatype
      boolean case1Passed = false;
      try {
         
         // the following does not work: we encode lat + lon literals, but
         // our schema description expects lat + lon + time
         final GeoSpatialLiteralExtension<BigdataValue> extLatLonTime = 
               getLatLonTimeGSLiteralExtension(vf);
         
         encodeDecodeGeoSpatialLiterals(
            vf, getDummyGeospatialLiteralsLatLon(vf), extLatLonTime);
         
      } catch (IllegalArgumentException e) {
         
         case1Passed = true; // expected
         
      }
      if (!case1Passed) {
      
         throw new RuntimeException("Expected IllegalArgumentException");
         
      }
      
      // direction two: processing lat+lon+time literals with lat+lon datatype
      boolean case2Passed = false;
      try {
         
         // the following does not work: we encode lat + lon +time literals, but
         // our schema description expects lat + lon

         final GeoSpatialLiteralExtension<BigdataValue> extLatLon = 
               getLatLonGSLiteralExtension(vf);

         encodeDecodeGeoSpatialLiterals(
            vf, getDummyGeospatialLiteralsLatLonTime(vf), extLatLon);
         
      } catch (IllegalArgumentException e) {
         
         case2Passed = true; // expected
         
      }
      if (!case2Passed) {
      
         throw new RuntimeException("Expected IllegalArgumentException");
         
      }
      
   }
   
   /**
    * Test z-order string construction by means of a simple, two dimensional
    * index with positive integer values.
    */
   public void testZIndexOrderingPositive() {
      
      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");
      
      final GeoSpatialLiteralExtension<BigdataValue> litExt = 
            getSimpleLatLonGSLiteralExtension(vf); 
      
      
      zIndexOrderingPositiveBase(vf, litExt);
      
   }
   
   /**
    * Test z-order string construction by means of a simple, two dimensional
    * index with positive integer values, with range adjustment encoded in
    * the datatype.
    */
   public void testZIndexOrderingPositiveWithRangeAdjustment() {
      
      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");

      // the range in the test we delegate to is 0 .. 7
      final GeoSpatialLiteralExtension<BigdataValue> litExt = 
         getSimpleLatLonGSLiteralExtensionWithRange(vf, Long.valueOf(0)); 
      
      zIndexOrderingPositiveBase(vf, litExt);
   }
   

   /**
    * Test z-order string construction by means of a simple, two dimensional
    * index with mixed negative and positive integer values.
    */
   public void testZIndexOrderingMixed() {
      
      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");

      // the range in the test we delegate to is -2 .. 1
      final GeoSpatialLiteralExtension<BigdataValue> litExt = 
         getSimpleLatLonGSLiteralExtension(vf); 
      
      zIndexOrderingMixedBase(vf, litExt);
   }
   
   /**
    * Test z-order string construction by means of a simple, two dimensional
    * index with mixed negative and positive integer values, with range
    * adjustment encoded in the datatype.
    */
   public void testZIndexOrderingMixedWithRangeAdjustment() {
      
      final BigdataValueFactory vf = BigdataValueFactoryImpl.getInstance("test");

      // the range in the test we delegate to is -2 .. 1
      final GeoSpatialLiteralExtension<BigdataValue> litExt = 
         getSimpleLatLonGSLiteralExtensionWithRange(vf, Long.valueOf(-2)); 
      
      zIndexOrderingMixedBase(vf, litExt);
   }
   
   

   /**
    * Helper method to test encoding and decoding / roundtrips of GeoSpatial
    * literals for a given value factory vf, list of literals dt, and
    * a {@link GeoSpatialLiteralExtension} ext. Note that the extension must
    * have been initialized to match the specific structure of the datatype
    * (see example test cases for sample code).
    */
   protected void encodeDecodeGeoSpatialLiterals(
      final BigdataValueFactory vf,
      final BigdataLiteral[] dt,
      final GeoSpatialLiteralExtension<BigdataValue> ext) throws Exception {
   
      // create associated IVs
      final IV<?, ?>[] e = new IV[dt.length];
      for (int i = 0; i < dt.length; i++) {
         e[i] = ext.createIV(dt[i]);
      }
   
      // assert that the round-trip actually gives us the same semantical value
      // (i.e., syntax/formatting might differ, but the numeric value conincides)
      for (int i = 0; i < e.length; i++) {
         @SuppressWarnings("rawtypes")
         final BigdataValue val = ext.asValue((LiteralExtensionIV) e[i], vf);
         assertSemanticallyIdentical(val, dt[i]);
      }
   
      final IV<?, ?>[] a = doEncodeDecodeTest(e);
   
      if (log.isInfoEnabled()) {
         for (int i = 0; i < e.length; i++) {
            log.info("original: " + dt[i]);
            log.info("asValue : "
                  + ext.asValue((LiteralExtensionIV<?>) e[i], vf));
            log.info("decoded : "
                  + ext.asValue((LiteralExtensionIV<?>) a[i], vf));
            log.info("");
         }
      }
   
      doComparatorTest(e);
   
   }

   protected void zIndexOrderingPositiveBase(
      final BigdataValueFactory vf,
      final GeoSpatialLiteralExtension<BigdataValue> litExt) {
      
      /**
       * Scenario description: assume we have integers 0 .. 7 for each of the
       * index components.
       * 
       * 0 -> 000
       * 1 -> 001
       * 2 -> 010
       * 3 -> 011
       * 4 -> 100
       * 5 -> 101
       * 6 -> 110
       * 7 -> 111
       * 
       * This gives us the following literal pairs, encodings, and binary
       * values (where the binary values reflect the visiting order that
       * are subject to be tested in the following (x/y).
       * 
       * 0#0 -> 00 00 00 (0)
       * 1#0 -> 00 00 01 (1)
       * 2#0 -> 00 01 00 (4)
       * 3#0 -> 00 01 01 (5)
       * 4#0 -> 01 00 00 (16)
       * 5#0 -> 01 00 01 (17)
       * 6#0 -> 01 01 00 (20)
       * 7#0 -> 01 01 01 (21)
       * 
       * 0#1 -> 00 00 10 (2)
       * 1#1 -> 00 00 11 (3)
       * 2#1 -> 00 01 10 (6)
       * 3#1 -> 00 01 11 (7)
       * 4#1 -> 01 00 10 (18)
       * 5#1 -> 01 00 11 (19)
       * 6#1 -> 01 01 10 (22)
       * 7#1 -> 01 01 11 (23)
       * 
       * 0#2 -> 00 10 00 (8)
       * 1#2 -> 00 10 01 (9)
       * 2#2 -> 00 11 00 (12)
       * 3#2 -> 00 11 01 (13)
       * 4#2 -> 01 10 00 (24)
       * 5#2 -> 01 10 01 (25)
       * 6#2 -> 01 11 00 (28)
       * 7#2 -> 01 11 01 (29)
       *  
       * 0#3 -> 00 10 10 (10)
       * 1#3 -> 00 10 11 (11)
       * 2#3 -> 00 11 10 (14)
       * 3#3 -> 00 11 11 (15)
       * 4#3 -> 01 10 10 (26)
       * 5#3 -> 01 10 11 (27)
       * 6#3 -> 01 11 10 (30)
       * 7#3 -> 01 11 11 (31)
       * 
       * 0#4 -> 10 00 00 (32)
       * 1#4 -> 10 00 01 (33)
       * 2#4 -> 10 01 00 (36)
       * 3#4 -> 10 01 01 (37)
       * 4#4 -> 11 00 00 (48)
       * 5#4 -> 11 00 01 (49)
       * 6#4 -> 11 01 00 (52)
       * 7#4 -> 11 01 01 (53)
       *  
       * 0#5 -> 10 00 10 (34)
       * 1#5 -> 10 00 11 (35)
       * 2#5 -> 10 01 10 (38)
       * 3#5 -> 10 01 11 (39)
       * 4#5 -> 11 00 10 (50)
       * 5#5 -> 11 00 11 (51)
       * 6#5 -> 11 01 10 (54)
       * 7#5 -> 11 01 11 (55)
       *  
       * 0#6 -> 10 10 00 (40)
       * 1#6 -> 10 10 01 (41)
       * 2#6 -> 10 11 00 (44)
       * 3#6 -> 10 11 01 (45)
       * 4#6 -> 11 10 00 (56)
       * 5#6 -> 11 10 01 (57)
       * 6#6 -> 11 11 00 (60)
       * 7#6 -> 11 11 01 (61)
       * 
       * 0#7 -> 10 10 10 (42)
       * 1#7 -> 10 10 11 (43)
       * 2#7 -> 10 11 10 (46)
       * 3#7 -> 10 11 11 (47)
       * 4#7 -> 11 10 10 (58)
       * 5#7 -> 11 10 11 (59)
       * 6#7 -> 11 11 10 (62)
       * 7#7 -> 11 11 11 (63)
       */
      
      // Generate in syntactical order (as above): 0#0, ..., 0#7, 1#0, ... 7#7:
      final BigdataLiteral[] asWritten  =
         getGeospatialLiteralsLatLonInRange(vf,0,7);
      
      // convert into LiteralExtensionIVs (backed by BigInteger, in this case)
      @SuppressWarnings("rawtypes")
      final LiteralExtensionIV[] asWrittenConverted = 
         new LiteralExtensionIV[asWritten.length];
   
      for (int i=0; i<asWritten.length; i++) {
         asWrittenConverted[i] = litExt.createIV(asWritten[i]);
      }
   
      for (int i=0; i<asWrittenConverted.length; i++) {
         System.out.println(asWritten[i] + " -> " + asWrittenConverted[i]);
      }
      
      // manually bring into expected (z-)order
      @SuppressWarnings("rawtypes")
      final LiteralExtensionIV[] ordered = 
            new LiteralExtensionIV[asWrittenConverted.length];
      ordered[0] = asWrittenConverted[0];
      ordered[1] = asWrittenConverted[1];
      ordered[2] = asWrittenConverted[8];
      ordered[3] = asWrittenConverted[9];
      ordered[4] = asWrittenConverted[2];
      ordered[5] = asWrittenConverted[3];
      ordered[6] = asWrittenConverted[10];
      ordered[7] = asWrittenConverted[11];
      
      ordered[8] = asWrittenConverted[16];
      ordered[9] = asWrittenConverted[17];
      ordered[10] = asWrittenConverted[24];
      ordered[11] = asWrittenConverted[25];
      ordered[12] = asWrittenConverted[18];
      ordered[13] = asWrittenConverted[19];
      ordered[14] = asWrittenConverted[26];
      ordered[15] = asWrittenConverted[27];
      
      ordered[16] = asWrittenConverted[4];
      ordered[17] = asWrittenConverted[5];
      ordered[18] = asWrittenConverted[12];
      ordered[19] = asWrittenConverted[13];
      ordered[20] = asWrittenConverted[6];
      ordered[21] = asWrittenConverted[7];
      ordered[22] = asWrittenConverted[14];
      ordered[23] = asWrittenConverted[15];
      
      ordered[24] = asWrittenConverted[20];
      ordered[25] = asWrittenConverted[21];
      ordered[26] = asWrittenConverted[28];
      ordered[27] = asWrittenConverted[29];
      ordered[28] = asWrittenConverted[22];
      ordered[29] = asWrittenConverted[23];
      ordered[30] = asWrittenConverted[30];
      ordered[31] = asWrittenConverted[31];
      
      ordered[32] = asWrittenConverted[32];
      ordered[33] = asWrittenConverted[33];
      ordered[34] = asWrittenConverted[40];
      ordered[35] = asWrittenConverted[41];
      ordered[36] = asWrittenConverted[34];
      ordered[37] = asWrittenConverted[35];
      ordered[38] = asWrittenConverted[42];
      ordered[39] = asWrittenConverted[43];
      
      ordered[40] = asWrittenConverted[48];
      ordered[41] = asWrittenConverted[49];
      ordered[42] = asWrittenConverted[56];
      ordered[43] = asWrittenConverted[57];
      ordered[44] = asWrittenConverted[50];
      ordered[45] = asWrittenConverted[51];
      ordered[46] = asWrittenConverted[58];
      ordered[47] = asWrittenConverted[59];
      
      ordered[48] = asWrittenConverted[36];
      ordered[49] = asWrittenConverted[37];
      ordered[50] = asWrittenConverted[44];
      ordered[51] = asWrittenConverted[45];
      ordered[52] = asWrittenConverted[38];
      ordered[53] = asWrittenConverted[39];
      ordered[54] = asWrittenConverted[46];
      ordered[55] = asWrittenConverted[47];
      
      ordered[56] = asWrittenConverted[52];
      ordered[57] = asWrittenConverted[53];
      ordered[58] = asWrittenConverted[60];
      ordered[59] = asWrittenConverted[61];
      ordered[60] = asWrittenConverted[54];
      ordered[61] = asWrittenConverted[55];
      ordered[62] = asWrittenConverted[62];
      ordered[63] = asWrittenConverted[63];
   
      // assert that everything is in order
      int ctr = 0;
      for (int i=0; i<ordered.length-1; i++) {
         try {
            assertTrue(ordered[i].compareTo(ordered[i+1])<0);
            ctr++;
         } catch (Throwable e) {
            throw new RuntimeException("Problem with index " + i);
         }
      } 
      
      System.out.println("Executed " + ctr + " comparisons. All good, in z-order");
   }

   protected void zIndexOrderingMixedBase(
      final BigdataValueFactory vf,
      final GeoSpatialLiteralExtension<BigdataValue> litExt) {
      
      // Generate values
      final BigdataLiteral[] asWritten  =
         getGeospatialLiteralsLatLonInRange(vf,-2,1);
      
      // convert into LiteralExtensionIVs (backed by BigInteger, in this case)
      @SuppressWarnings("rawtypes")
      final LiteralExtensionIV[] asWrittenConverted = 
         new LiteralExtensionIV[asWritten.length];
      for (int i=0; i<asWritten.length; i++) {
         asWrittenConverted[i] = litExt.createIV(asWritten[i]);
      }
   
      for (int i=0; i<asWrittenConverted.length; i++) {
         System.out.println(asWritten[i] + " -> " + asWrittenConverted[i]);
      }
      
      // manually bring into expected (z-)order
      @SuppressWarnings("rawtypes")
      LiteralExtensionIV[] ordered = 
            new LiteralExtensionIV[asWrittenConverted.length];
      
      ordered[0] = asWrittenConverted[0];
      ordered[1] = asWrittenConverted[1];
      ordered[2] = asWrittenConverted[4];
      ordered[3] = asWrittenConverted[5];
      ordered[4] = asWrittenConverted[2];
      ordered[5] = asWrittenConverted[3];
      ordered[6] = asWrittenConverted[6];
      ordered[7] = asWrittenConverted[7];
      
      ordered[8] = asWrittenConverted[8];
      ordered[9] = asWrittenConverted[9];
      ordered[10] = asWrittenConverted[12];
      ordered[11] = asWrittenConverted[13];
      ordered[12] = asWrittenConverted[10];
      ordered[13] = asWrittenConverted[11];
      ordered[14] = asWrittenConverted[14];
      ordered[15] = asWrittenConverted[15];
      
      // assert that everything is in order
      int ctr = 0;
      for (int i=0; i<ordered.length-1; i++) {
         try {
            assertTrue(ordered[i].compareTo(ordered[i+1])<0);
            ctr++;
         } catch (Throwable e) {
            throw new RuntimeException("Problem with index " + i);
         }
      } 
      
      System.out.println("Executed " + ctr + " comparisons. All good, in z-order");
   }

   /**
    * Generates a list of about 600 dummy lat+lon GeoSpatial literals. 
    * These literals include both positive and negative values of different
    * orders of magnitude.
    * 
    * The basic schema is a three-component datatype string made up from the
    * following three components:
    * 
    * sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); 
    * sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); 
    * 
    * @param vf
    *           the value factory used to generate the literals
    * @return the list of generated literals
    */
   protected final BigdataLiteral[] getDummyGeospatialLiteralsLatLon(
         final BigdataValueFactory vf) {

      /**
       * The basic schema is a three-component datatype string made up from the
       * following three components:
       * 
       * sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); 
       * sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); 
       * sfd.add(new SchemaFieldDescription(Datatype.LONG, -1));
       */

      // let's start out with some random values of different magnitudes
      Double[] baseLatLong = { 
            -999999999.12345, 
            -88888888.34423,
            -7777777.345,
            -666666.0001, 
            -55555.21329, 
            -4444.2345, 
            -333.232,
            -22.5993, 
            -1.3533, 
            -0.65532, 
            -0.5332, 
            -0.453, 
            -0.33, 
            -0.2, 
            0.0,
            0.5, 
            0.85, 
            0.901,
            0.9399,
            0.95002, 
            1.13, 
            22.45,
            333.43453,
            4444.23423, 
            55555.32443,
            666666.22323,
            7777777.0, 
            88888888.023,
            999999999.2343
      };


      // we'll create a permutation over all values above
      final BigdataLiteral[] dt = 
         new BigdataLiteral[
            baseLatLong.length * baseLatLong.length];

      // compute permutations from the base arrays provided above
      int ctr = 0;
      for (int lat = 0; lat < baseLatLong.length; lat++) {
         for (int lon = 0; lon < baseLatLong.length; lon++) {
            dt[ctr++] = vf.createLiteral(
               baseLatLong[lat] + "#" + baseLatLong[lon], GeoSpatial.DATATYPE);
         }
      }

      return dt;
   }
   
   /**
    * Generates a list of about 20k dummy lat+lon GeoSpatial literals. 
    * These literals include both positive and negative values of different
    * orders of magnitude.
    * 
    * The basic schema is a two-component datatype string made up from the
    * following three components:
    * 
    * sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); sfd.add(new
    * SchemaFieldDescription(Datatype.DOUBLE, 5)); sfd.add(new
    * SchemaFieldDescription(Datatype.LONG, -1));
    * 
    * @param vf
    *           the value factory used to generate the literals
    * @return the list of generated literals
    */
   protected final BigdataLiteral[] getDummyGeospatialLiteralsLatLonTime(
         final BigdataValueFactory vf) {

      /**
       * The basic schema is a three-component datatype string made up from the
       * following three components:
       * 
       * sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); sfd.add(new
       * SchemaFieldDescription(Datatype.DOUBLE, 5)); sfd.add(new
       * SchemaFieldDescription(Datatype.LONG, -1));
       */

      // let's start out with some random values of different magnitudes
      Double[] baseLatLong = { 
            -999999999.12345, 
            -88888888.34423,
            -7777777.345,
            -666666.0001, 
            -55555.21329, 
            -4444.2345, 
            -333.232,
            -22.5993, 
            -1.3533, 
            -0.65532, 
            -0.5332, 
            -0.453, 
            -0.33, 
            -0.2, 
            0.0,
            0.5, 
            0.85, 
            0.901,
            0.9399,
            0.95002, 
            1.13, 
            22.45,
            333.43453,
            4444.23423, 
            55555.32443,
            666666.22323,
            7777777.0, 
            88888888.023,
            999999999.2343
      };

      Long[] baseTime = { 
            Long.valueOf(-747626633), 
            Long.valueOf(-93939483),
            Long.valueOf(-3884843), 
            Long.valueOf(-293939),
            Long.valueOf(-54775), 
            Long.valueOf(-4848), 
            Long.valueOf(-832),
            Long.valueOf(-22), 
            Long.valueOf(-2), 
            Long.valueOf(-1),
            Long.valueOf(0), 
            Long.valueOf(3), 
            Long.valueOf(7),
            Long.valueOf(25), 
            Long.valueOf(363), 
            Long.valueOf(5482),
            Long.valueOf(88482), 
            Long.valueOf(959593), 
            Long.valueOf(9399937),
            Long.valueOf(93994959),
            Long.valueOf(372772737) 
      };

      // we'll create a permutation over all values above
      final BigdataLiteral[] dt = 
         new BigdataLiteral[
            baseLatLong.length * baseLatLong.length * baseTime.length];

      // compute permutations from the base arrays provided above
      int ctr = 0;
      for (int lat = 0; lat < baseLatLong.length; lat++) {
         for (int lon = 0; lon < baseLatLong.length; lon++) {
            for (int time = 0; time < baseTime.length; time++) {
               dt[ctr++] = vf.createLiteral(baseLatLong[lat] + "#"
                     + baseLatLong[lon] + "#" + baseTime[time],
                     GeoSpatial.DATATYPE);
            }
         }
      }

      return dt;
   }
   
   
   /**
    * Generates the combination of all literals in the given range. E.g., passing
    * in 0 as from and 2 as to, we get 0#0, 0#1, 0#2, 1#0, 1#1, 1#2, 2#0, 2#1,
    * and 2#2.
    * @param vf
    *           the value factory used to generate the literals
    * @return the list of generated literals
    */
   protected final BigdataLiteral[] getGeospatialLiteralsLatLonInRange(
      final BigdataValueFactory vf, final int from, final int to) {
      
      final int numComponents = to-from+1;
      
      // we'll create a permutation over all values above
      final BigdataLiteral[] dt = 
         new BigdataLiteral[numComponents*numComponents];

      // compute permutations from the base arrays provided above:
      // 0#0, 0#1, ..., 0#n, 1#0, 1#1, 1#n, ..., n#n
      int ctr = 0;
      for (int y = from; y <= to; y++) {
         for (int x = from; x <= to; x++) {
            dt[ctr++] = vf.createLiteral(x + "#" + y, GeoSpatial.DATATYPE);
         }
      }

      return dt;
   }

   /**
    * Get a {@link GeoSpatialLiteralExtension} object processing lat+lon+time
    * schema literals.
    */
   protected GeoSpatialLiteralExtension<BigdataValue> 
      getLatLonTimeGSLiteralExtension(final BigdataValueFactory vf) {
      
      final List<SchemaFieldDescription> latLonTimeSfd = 
            new ArrayList<SchemaFieldDescription>();
      latLonTimeSfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* lat */
      latLonTimeSfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* lon */
      latLonTimeSfd.add(new SchemaFieldDescription(Datatype.LONG, 10));   /* time */
            
      return getGSLiteralExtension(vf, new SchemaDescription(latLonTimeSfd));
   }

   /**
    * Get a {@link GeoSpatialLiteralExtension} object processing lat+lon
    * schema literals.
    */
   protected GeoSpatialLiteralExtension<BigdataValue> 
      getLatLonGSLiteralExtension(final BigdataValueFactory vf) {
      
      final List<SchemaFieldDescription> latLonSfd = 
            new ArrayList<SchemaFieldDescription>();
      latLonSfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* lat */
      latLonSfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 100000)); /* lon */
            
      return getGSLiteralExtension(vf, new SchemaDescription(latLonSfd));
      
   }
   
   /**
    * Get simple lat lon schema description, where lat and lon correspong to
    * long values (rather than 5 precise doubles).
    */
   protected GeoSpatialLiteralExtension<BigdataValue> 
      getSimpleLatLonGSLiteralExtension(final BigdataValueFactory vf) {
      
      final List<SchemaFieldDescription> latLonSfd = 
            new ArrayList<SchemaFieldDescription>();
      latLonSfd.add(new SchemaFieldDescription(Datatype.LONG, 1)); /* lat */
      latLonSfd.add(new SchemaFieldDescription(Datatype.LONG, 1)); /* lon */
            
      return getGSLiteralExtension(vf, new SchemaDescription(latLonSfd));
      
   }
   
   /**
    * Get simple lat lon schema description, where lat and lon correspong to
    * long values (rather than 5 precise doubles).
    */
   protected GeoSpatialLiteralExtension<BigdataValue> 
      getSimpleLatLonGSLiteralExtensionWithRange(
         final BigdataValueFactory vf, final Long min) {
      
      final List<SchemaFieldDescription> latLonSfd = 
            new ArrayList<SchemaFieldDescription>();
      latLonSfd.add(new SchemaFieldDescription(Datatype.LONG, 1, min)); /* lat */
      latLonSfd.add(new SchemaFieldDescription(Datatype.LONG, 1, min)); /* lon */
            
      return getGSLiteralExtension(vf, new SchemaDescription(latLonSfd));
      
   }

   /**
    * Get a {@link GeoSpatialLiteralExtension} object processing literals of
    * the schema specified in the {@link SchemaDescription} object.
    */
   protected GeoSpatialLiteralExtension<BigdataValue> getGSLiteralExtension(
      final BigdataValueFactory vf, final SchemaDescription sd) {
      
      return 
         new GeoSpatialLiteralExtension<BigdataValue>(
            new IDatatypeURIResolver() {
               public BigdataURI resolve(URI uri) {
                  final BigdataURI buri = vf.createURI(uri.stringValue());
                  buri.setIV(newTermId(VTE.URI));
                  return buri;
               }
         },sd);
      
   }

   /**
    * Asserts that the two {@link BigdataValue}s that are passed in are
    * {@link BigdataLiteral} of the same type and that the actual values
    * value, interpreted as numerical value, are identical.
    * 
    * @param val
    * @param bigdataLiteral
    * 
    * @throw {@link AssertionError} in case they are not
    */
   protected void assertSemanticallyIdentical(final BigdataValue x1,
         final BigdataLiteral x2) {
   
      assertTrue(x1 instanceof BigdataLiteral);
   
      // assert they're both of the same datatype
      final BigdataLiteral x1AsLiteral = (BigdataLiteral) x1;
      assertEquals(x1AsLiteral.getDatatype(), x2.getDatatype());
   
      // compare the component values
      String[] x1Components = x1.stringValue().split("#");
      String[] x2Components = x2.stringValue().split("#");
   
      assertEquals(x1Components.length, x2Components.length);
   
      for (int i = 0; i < x1Components.length; i++) {
         BigDecimal d1i = new BigDecimal(x1Components[i]);
         BigDecimal d2i = new BigDecimal(x2Components[i]);
   
         assertEquals(d1i, d2i);
   
      }
   }

}
