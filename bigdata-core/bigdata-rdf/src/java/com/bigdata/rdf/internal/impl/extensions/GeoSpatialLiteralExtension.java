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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension.SchemaFieldDescription.Datatype;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.service.geospatial.GeoSpatial;

/**
 * Special encoding for GeoSpatial datatypes. We encode literals of the form
 * <int_i>#...#<int_n> as BigInteger using the xsd:integer type. The conversion
 * into BigInteger is based on a calculation of the z-order string for the n
 * components.
 * 
 * The code to create a literal is, e.g.:
 * <code>"2#4"^^<http://www.bigdata.com/rdf/geospatial#geoSpatialLiteral></code>
 * 
 * The two components are first broken down long integers, namely
 * - 2 -> 00000000 00000000 00000010
 * - 4 -> 00000000 00000000 00000100
 * 
 * The z-order encoding of these two strings is then
 * 00000000 00000000 00000000 00000000 00000000 00000000 00011000
 * 
 * Interpreted as BigInteger, this is the value 24, which is stored as integer
 * literal in the database. The asValue method reverts this (lossless) encoding.
 * 
 * TODO:
 * - push logics into object where we don't need to re-allocate arrays
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class GeoSpatialLiteralExtension<V extends BigdataValue> implements IExtension<V> {

   private static final int BASE_SIZE = Double.SIZE / 8;
   private static final String COMPONENT_SEPARATOR = "#";

   @SuppressWarnings("unused")
   private static final transient Logger log = Logger
         .getLogger(GeoSpatialLiteralExtension.class);

   private final URI datatypeURI = GeoSpatial.DATATYPE;
   
   private final BigdataURI datatype;
   
   private SchemaDescription sd;
   
   /**
    * Constructor setting up an instance with a default schema description.
    * 
    * @param resolver
    */
   public GeoSpatialLiteralExtension(final IDatatypeURIResolver resolver) {

      this(resolver, defaultSchemaDescription());
   }

   /**
    * Constructor setting up an instance with a custom schema description.
    * @param resolver
    * @param sd
    */
   public GeoSpatialLiteralExtension(final IDatatypeURIResolver resolver, 
      final SchemaDescription sd) {

      this.datatype = resolver.resolve(datatypeURI);
      this.sd = sd;
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
   @SuppressWarnings("rawtypes")
   @Override
   public LiteralExtensionIV createIV(final Value value) {

      if (value instanceof Literal == false)
         throw new IllegalArgumentException("Value not a literal");

      return createIV(value.stringValue().split(COMPONENT_SEPARATOR));
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   public LiteralExtensionIV createIV(Object[] components) {

      final long[] componentsAsLongArr = componentsAsLongArr(components, sd);

      final byte[] zOrderByteArray = toZOrderByteArray(componentsAsLongArr, sd);

      final byte[] zOrderByteArrayTwoCompl = unsignedToTwosComplement(zOrderByteArray);
      final BigInteger bi = new BigInteger(zOrderByteArrayTwoCompl);

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
      
      final long[] componentsAsLongArr = asLongArray(iv, vf);
      
      final String litStr = longArrAsComponentString(componentsAsLongArr);
      
      return (V) vf.createLiteral(litStr, datatype);
   }
   
   @SuppressWarnings("rawtypes")
   public long[] asLongArray(final LiteralExtensionIV iv, final BigdataValueFactory vf) {

      if (!datatype.getIV().equals(iv.getExtensionIV())) {
         throw new IllegalArgumentException("unrecognized datatype");
      }
      
      final int numDimensions = sd.getNumDimensions();
      
      final BigInteger bigInt = iv.getDelegate().integerValue();
   
      // convert BigInteger back to byte array
      final byte[] bigIntAsByteArr = bigInt.toByteArray();
      
      // pad 0-bytes if necessary and copy over bytes
      final int paddedArraySize = numDimensions * BASE_SIZE + 1; /* +1 leading zero byte to make value unsigned */
      
      final byte[] bigIntAsByteArrPad = new byte[paddedArraySize];
      
      int idx = 0;
      for (int i = 0; i < paddedArraySize - bigIntAsByteArr.length; i++) {
         bigIntAsByteArrPad[idx++] = 0;   // padding
      }
      for (int i = 0; i < bigIntAsByteArr.length; i++) {
         bigIntAsByteArrPad[idx++] = bigIntAsByteArr[i];   // copy of bytes
      }

      byte[] bigIntAsByteArrUnsigned = twosComplementToUnsigned(bigIntAsByteArrPad);

      
      long[] componentsAsLongArr = fromZOrderByteArray(bigIntAsByteArrUnsigned, sd);
      
      return componentsAsLongArr;
      

   }
   

   /**
    * Convert the components into a long array. The array is passed as an
    * Object[], in order to allow for unparsed strings as well as Long or
    * Double's (or any convertable) as input. The array must have the same
    * size as the number of dimensions, otherwise a runtime exception is thrown.
    * 
    * Longs (or other objects being parseable as Long) are copied to the target
    * array without modification. Floats (or objects being parseable as Float)
    * are converted into Long according to the precision specified in the
    * passed {@link SchemaDescription}.
    */
   final long[] componentsAsLongArr(
      final Object[] components, final SchemaDescription sd) {

      final long[] ret = new long[sd.getNumDimensions()];

      int numDimensions = sd.getNumDimensions();
      if (numDimensions != components.length) {
         throw new IllegalArgumentException(
            "Literal value has wrong format. Expected " + numDimensions 
            + " components for datatype.");
      }
      
      for (int i = 0; i < components.length; i++) {
         
         final Object component = components[i];
         final SchemaFieldDescription sfd = sd.getSchemaFieldDescription(i);
         
         final BigDecimal precisionAdjustment = 
               BigDecimal.valueOf(10).pow(sfd.getDoublePrecision());

         switch (sfd.getDatatype()) {
         case DOUBLE:
         {
            final BigDecimal componentAsBigInteger =
                  component instanceof BigDecimal ?
                  (BigDecimal)component : new BigDecimal(component.toString());
                  
            final BigDecimal x = precisionAdjustment.multiply(componentAsBigInteger);
            ret[i] = x.longValue();
                  
            break;
         }
         case LONG:
         {
            ret[i] = 
               component instanceof Long ? 
               (Long)component : Long.valueOf(component.toString());
            break;
         }
         default:
            throw new RuntimeException("Uncovered encoding case. Please fix code.");
         }

      }
      
      return ret;
   }

   

   
   /**
    * Converts a a Long[] reflecting the long values of the individual 
    * components back into a component string representing the literal.
    * 
    * @param arr the long array representing the components
    * @param sd the associated schema description
    * @return the string literal
    */
   final String longArrAsComponentString(final long[] arr) {
      
      final Object[] componentArr = longArrAsComponentArr(arr);
      
      final StringBuffer buf = new StringBuffer();
      for (int i=0; i<componentArr.length; i++) {
         
         if (i>0)
            buf.append(COMPONENT_SEPARATOR);
         
         buf.append(componentArr[i]);
         
      }
      
      return buf.toString();
   }

   
   /**
    * Converts a a Long[] reflecting the long values of the individual 
    * components back into a component array representing the literal.
    * 
    * @param arr the long array representing the components
    * @param sd the associated schema description
    * @return the component array (containing longs and precision adjusted doubles)
    */
   final public Object[] longArrAsComponentArr(final long[] arr) {
      
      int numDimensions = sd.getNumDimensions();
      if (arr.length!=numDimensions) {
         throw new IllegalArgumentException(
               "Encoding has wrong format. Expected " + numDimensions 
               + " components for datatype.");
      }
         
      final StringBuffer buf = new StringBuffer();
      final Object[] componentArr = new Object[arr.length];
      for (int i=0; i<arr.length; i++) {
         if (i>0)
            buf.append(COMPONENT_SEPARATOR);
         
         final SchemaFieldDescription sfd = sd.getSchemaFieldDescription(i);
         final double precisionAdjustment = Math.pow(10, sfd.getDoublePrecision());
         
         switch (sfd.getDatatype()) {
         case DOUBLE:
            componentArr[i] = (double)arr[i]/precisionAdjustment;
            break;
         case LONG:
            componentArr[i] = arr[i];
            break;
         default:
            throw new RuntimeException("Uncovered decoding case. Please fix code.");
         }

      }

      return componentArr;
   }
   
   /**
    * Converts a long array representing the components to a z-order byte array
    * 
    * @param componentsAsLongArr
    * @param sd
    * @return
    */
   byte[] toZOrderByteArray(
         final long[] componentsAsLongArr, final SchemaDescription sd) {
      
      IKeyBuilder kb = KeyBuilder.newInstance(componentsAsLongArr.length*BASE_SIZE);
      for (int i=0; i<componentsAsLongArr.length; i++) {
         kb.append(componentsAsLongArr[i]);
      }
      
      return kb.toZOrder(sd.getNumDimensions());
   }
   
   /**
    * Converts a z-order byte array to a long array representing the components
    * 
    * @param byteArr
    * @param sd2
    * @return
    */
   long[] fromZOrderByteArray(
      final byte[] byteArr, final SchemaDescription sd) {
      
      IKeyBuilder kb = KeyBuilder.newInstance(byteArr.length);
      for (int i=0; i<byteArr.length; i++) {
         kb.append(byteArr[i]);
      }
      
      return kb.fromZOrder(sd.getNumDimensions());
   }

   /**
    * Converts an unsigned byte array into a (positive) two's complement array.
    * 
    * The trick here is to pad a zero byte. This changes the value (which does
    * not harm) yet makes sure that the array is an unsigned value, for which
    * the two's complement representation does not differ.
    */
   byte[] unsignedToTwosComplement(byte[] arr) {
      byte[] ret = new byte[arr.length+1];
      
      // ret[0] = 0 by construction
      for (int i=0; i<arr.length; i++) {
         ret[i+1] = arr[i];
      }
      
      return ret;
      
   }

   /**
    * Reverts method {{@link #unsignedToTwosComplement(byte[])}.
    */
   byte[] twosComplementToUnsigned(byte[] arr) {
      
      byte[] ret = new byte[arr.length-1];
      
      // ret[0] = 0 by construction
      for (int i=0; i<ret.length; i++) {
         ret[i] = arr[i+1];
      }
      
      return ret;
      
   }

   /**
     * For now, we're using a fixed, three-dimensional datatype for
     * latitued, longitude and time.
     * 
     * TODO: this is fixed for now, we may want to make this take a datatype
     * as input.
     */
   private static SchemaDescription defaultSchemaDescription() {
      
      final List<SchemaFieldDescription> sfd = 
         new ArrayList<SchemaFieldDescription>();
   
      sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); /* latitude */
      sfd.add(new SchemaFieldDescription(Datatype.DOUBLE, 5)); /* longitude */
      sfd.add(new SchemaFieldDescription(Datatype.LONG, 1));  /* time */
         
      return new SchemaDescription(sfd);      
   }

   /**
    * Schema description for a geospatial literal
    */
   public static class SchemaDescription {

      private final List<SchemaFieldDescription> fields;
      
      public SchemaDescription(List<SchemaFieldDescription> fields) {
         this.fields = fields;
      }
      
      public int getNumDimensions() {
         return fields.size();
      }
      
      public SchemaFieldDescription getSchemaFieldDescription(int index) {
         return fields.get(index);
      }

   }
   
   /**
    * Description of a field in the schema.
    */
   public static class SchemaFieldDescription {
      
      /**
       * We support doubles and floats for now
       */
      public enum Datatype {
         LONG,
         DOUBLE
      }

      
      private final Datatype datatype;

      /**
       * Precision of float value. Based on the precision, we convert the
       * double into a long value. E.g., if the precision is 8 (meaning
       * 8 positions after decimal point), we multiply the double by 8
       * and convert into a long value (ignoring additional positions).
       */
      private final int doublePrecision;
      
      public SchemaFieldDescription(
         final Datatype datatype, final int doublePrecision) {
         this.datatype = datatype;
         this.doublePrecision = doublePrecision;
      }
      
      
      public Datatype getDatatype() {
         return datatype;
      }

      public int getDoublePrecision() {
         return doublePrecision;
      }
      
   }

}
