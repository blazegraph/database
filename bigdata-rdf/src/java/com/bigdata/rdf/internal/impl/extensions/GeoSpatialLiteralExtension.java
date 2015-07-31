/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
 * Created on July 31, 2015
 */
package com.bigdata.rdf.internal.impl.extensions;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.service.geospatial.GeoSpatial;
import com.bigdata.util.Bits;

/**
 * Special encoding for GeoSpatial datatypes. We encode literals of the form
 * <int_i>#...#<int_n> as BigInteger using the xsd:integer type. The conversion
 * into BigInteger is based on a calculation of the z-order string for the n
 * components.
 * 
 * The code to create a literal is, e.g.:
 * <code>"2#4"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral></code>
 * 
 * The two components are first broken down into integers, namely
 * - 2 -> 00000000 00000000 00000010
 * - 4 -> 00000000 00000000 00000100
 * 
 * The z-order encoding of these two strings is then
 * 00000000 00000000 00000000 00000000 00000000 00000000 00011000
 * 
 * Interpreted as BigInteger, this is the value 24, which is stored as integer
 * literal in the database. The asValue method reverts this (lossless) encoding.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialLiteralExtension<V extends BigdataValue> implements IExtension<V> {

   // TODO: need mechanism to pass in the number of dimensions
   private static final int NUM_DIMENSIONS = 2;
   private static final int SIZE_INTEGER_IN_BYTES = 4;

   @SuppressWarnings("unused")
   private static final transient Logger log = Logger
         .getLogger(GeoSpatialLiteralExtension.class);

   private final URI datatypeURI = GeoSpatial.DATATYPE;
   
   private final BigdataURI datatype;
   
   public GeoSpatialLiteralExtension(final IDatatypeURIResolver resolver) {

      this.datatype = resolver.resolve(datatypeURI);

   }

   @Override
   public Set<BigdataURI> getDatatypes() {

      final HashSet<BigdataURI> datatypes = new LinkedHashSet<BigdataURI>();
      datatypes.add(datatype);
      return datatypes;

   }

   /**
    * Encodes an n-dimensional string of the form <int_1>#...#<int_n> as 
    * xsd:integer.
    */
   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public LiteralExtensionIV createIV(final Value value) {

      if (value instanceof Literal == false)
         throw new IllegalArgumentException("Value not a literal");

      final String literalString = value.stringValue();
      String[] componentsAsString = literalString.split("#");
      
      if (NUM_DIMENSIONS != componentsAsString.length) {
         throw new IllegalArgumentException(
            "Value has wrong format. Expected " + NUM_DIMENSIONS 
            + " dimensions for datatype");
      }

      final String[] componentsAsBinaryStrings = new String[NUM_DIMENSIONS];
      for (int i = 0; i < componentsAsString.length; i++) {
         
         final String componentAsString = componentsAsString[i];

         // TODO:
         // This is the code for floats, which has strange side effects
         // System.err.println("C=" + componentAsString);
         // Float componentAsFloat = Float.valueOf(componentAsString);
         // System.err.println("F=" + componentAsFloat);
         // int componentAsInt = Float.floatToIntBits(componentAsFloat);
         // System.err.println("I=" + componentAsInt);
         
         // parse integer
         int componentAsInt = Integer.valueOf(componentAsString);

         // TODO: for now, convert to string to compute the z-order string
         //       (of course, this can (and should) be done more efficiently
         //       using bit operations instead)
         // compute 32-pos string for integer binary representation padded with 0
         final String binaryString = String.format("%32s",
               Integer.toBinaryString(componentAsInt)).replace(' ', '0');
         componentsAsBinaryStrings[i] = binaryString;
         
         System.err.println("I=" + componentAsInt);
         System.err.println("B=" + binaryString);

      }

      // compose the z-order string using a string buffer
      StringBuffer buf = new StringBuffer();
      for (int i = 0; i < 32; i++) {
         for (int j = 0; j < componentsAsBinaryStrings.length; j++) {
            buf.append(componentsAsBinaryStrings[j].charAt(i));
         }
      }
      final String zOrderString = buf.toString();
      System.err.println("zOrderString=" + zOrderString);

      // convert the binary string back into a byte array
      int numZOrderBytes = zOrderString.length() / Byte.SIZE;
      byte[] zOrderBytes = new byte[numZOrderBytes];
      for (int i = 0; i < numZOrderBytes; i++) {
         final String substr = zOrderString.substring(i * 8, (i + 1) * 8);
         zOrderBytes[i] = Byte.parseByte(substr, 2);
      }

      BigInteger bi = new BigInteger(zOrderBytes);
      System.err.println("BI=" + bi);

      // store big integer using xsd:integer datatype
      final AbstractLiteralIV delegate = new XSDIntegerIV(bi);
      return new LiteralExtensionIV(delegate, datatype.getIV());

   }

   /**
    * Decodes an xsd:integer into an n-dimensional string of the form 
    * <int_1>#...#<int_n>.
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   @Override
   public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {

      if (!datatype.getIV().equals(iv.getExtensionIV())) {
         throw new IllegalArgumentException("unrecognized datatype");
      }

      final BigInteger bigInt = iv.getDelegate().integerValue();
      System.err.println("BI" + bigInt);

      // convert BigInteger back to byte array
      byte[] bigIntAsByteArr = bigInt.toByteArray();
      
      // pad 0-bytes if necessary and copy over bytes
      byte[] bigIntAsByteArrPad = new byte[NUM_DIMENSIONS * SIZE_INTEGER_IN_BYTES];
      int idx = 0;
      for (int i = 0; i < NUM_DIMENSIONS * SIZE_INTEGER_IN_BYTES - bigIntAsByteArr.length; i++) {
         bigIntAsByteArrPad[idx++] = 0;   // padding
      }
      for (int i = 0; i < bigIntAsByteArr.length; i++) {
         bigIntAsByteArrPad[idx] = bigIntAsByteArr[i];   // copy of bytes
      }

      System.err.print("zOrderString=");
      for (int i = 0; i < bigIntAsByteArrPad.length; i++) {
         System.err.print(">" + Bits.toString(bigIntAsByteArrPad[i]));
      }
      System.err.println();


      // TODO: again, we take the StringBuffer approach, which should be changed
      //       to bit operations
      final StringBuffer[] components = new StringBuffer[NUM_DIMENSIONS];
      for (int i = 0; i < NUM_DIMENSIONS; i++) {
         components[i] = new StringBuffer();
      }

      // iterate over the bytes and append to the associated buffers
      int bufId = 0;
      for (int i = 0; i < bigIntAsByteArrPad.length; i++) {
         for (int j = 7; j >= 0; j--) { // bytes are inverted

            // add to the corresponding buffer
            components[bufId].append(getBitFromByte(bigIntAsByteArrPad[i], j));
            bufId = ++bufId % NUM_DIMENSIONS;
         }
      }

      final Integer[] componentsAsInt = new Integer[components.length];
      for (int i = 0; i < components.length; i++) {

         final String componentAsStr = components[i].toString();
         System.err.println("cs[" + i + "]=" + componentAsStr);

         final Integer componentAsInt = Integer.parseInt(
               componentAsStr.toString(), 2);
         componentsAsInt[i] = componentAsInt;
         System.err.println("ci[" + i + "]=" + componentAsInt);

         // TODO: add this code when using float
         // final Float componentAsFloat = Float.intBitsToFloat(componentAsInt);
         // componentsAsFloat[i] = componentAsFloat;
         // System.err.println("cf[" + i + "]=" + componentAsFloat);

      }

      final StringBuffer valueStringBuf = new StringBuffer();
      for (int i = 0; i < componentsAsInt.length; i++) {
         if (i > 0)
            valueStringBuf.append("#");
         valueStringBuf.append(componentsAsInt[i]);
      }
      
      final String valueString = valueStringBuf.toString();
      return (V) vf.createLiteral(valueString, datatype);
   }


   private static String getBitFromByte(byte b, int bit) {
      return (b & (1 << bit)) != 0 ? "1" : "0";
   }
}
