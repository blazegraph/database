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
   public GeoSpatialLiteralExtension(
      final IDatatypeURIResolver resolver, final SchemaDescription sd) {

      this.datatype = resolver.resolve(datatypeURI);
      this.sd = sd;
   }
   
   @Override
   public Set<BigdataURI> getDatatypes() {

      final HashSet<BigdataURI> datatypes = new LinkedHashSet<BigdataURI>();
      datatypes.add(datatype);
      return datatypes;

   }

   /****************************************************************************
    * DIRECTION "DOWN":
    * -----------------
    * The following methods implement either the full or parts of the "down"
    * direction. For the down, direction, conversion works as follows:
    * 
    * A.) We get as value a literal such as "2.54#3.21"^^geo:geoSpatialLiteral
    * 
    * B.) The literal is split into its components.
    * 
    * C.) The components are mapped to long values (either trivially, if they
    *     represent long values according to the schema, or based on a
    *     precision); for instance, assuming precision=2 in the schema for the
    *     two components, the literal above would be converted into [254,321]
    *     
    * D.) Next, from these long components, we compute the z-order string:
    *     D1.) If specified in the schema, we apply a range shift based on the
    *          minimum value known to shop up in the data. Assuming, e.g., our
    *          minimum value is zero, the components are shifted as
    *          [Long.MIN+254, Long.MIN+321].
    *     D2.) We compute the z-order string by mixing up the components bit
    *          representation. For instance (not matching the values from
    *          the example above), if component one has bit representation
    *          0011 and component two has bit representation 0110, we mix the
    *          bits as 00101101, where position 0,2,4,6 represent the second
    *          string, position 1,3,5,7 represent the first string.
    *          
    * E.) We pad a 0 byte to the z-order string, to make sure that the
    *     BigInteger constructor (which expects a two's complement), does
    *     not destroy order.
    *     
    * F.) The 0 byte padded string is converted into a BigInteger
    * 
    * G.) The BigInteger is converted to an XSDIntegerIV
    **************************************************************************/

   /**
    * Create an IV from a given value (where the value must be a Literal).
    * Implements transformation A->G.
    */
   @SuppressWarnings("rawtypes")
   @Override
   public LiteralExtensionIV createIV(final Value value) {

      if (value instanceof Literal == false)
         throw new IllegalArgumentException("Value not a literal");

      // delegate, splitting the value into its components
      return createIV(value.stringValue().split(COMPONENT_SEPARATOR));
   }

   /**
    * Create an IV from a given value (where the value must be a Literal).
    * Implements transformation B->F.
    */
   @SuppressWarnings({ "rawtypes", "unchecked" })
   public LiteralExtensionIV createIV(Object[] components) {

      // convert component array into long's (B->C)
      final long[] componentsAsLongArr = componentsAsLongArr(components, sd);

      // convert the long array into a byte[] (C->D)
      final byte[] zOrderByteArray = toZOrderByteArray(componentsAsLongArr, sd);

      // convert into a valid two's complement byte array (D->E)
      final byte[] zOrderByteArrayTwoCompl = padLeadingZero(zOrderByteArray);
      
      // we now can safely call the BigInteger constructor (E->F)
      final BigInteger bi = new BigInteger(zOrderByteArrayTwoCompl);

      // finally, wrap the big integer into an xsd:integer (F->G)
      final AbstractLiteralIV delegate = new XSDIntegerIV(bi);
      return new LiteralExtensionIV(delegate, datatype.getIV());
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
    * 
    * Implements step B->C.
    */
   public final long[] componentsAsLongArr(
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
    * Converts a long array representing the components to a z-order byte array.
    * Thereby, a range shift is performed, if specified.
    * 
    * Implements step C->D
    */
   public byte[] toZOrderByteArray(
         final long[] componentsAsLongArr, final SchemaDescription sd) {
      
      IKeyBuilder kb = KeyBuilder.newInstance(componentsAsLongArr.length*BASE_SIZE);
      for (int i=0; i<componentsAsLongArr.length; i++) {
         
         // get current component
         final long componentAsLong = componentsAsLongArr[i];
         
         // shift component by given range
         final Long minValue = sd.getSchemaFieldDescription(i).getMinValue();
         final long componentAsLongRangeShifted = 
            minValue==null ? 
            componentAsLong : 
            encodeRangeShift(componentAsLong, minValue);

         kb.append(componentAsLongRangeShifted);
      }
      
      return kb.toZOrder(sd.getNumDimensions());
   }
   
   /**
    * Shift values according to the minValue, making sure that we encode the
    * lowest value in the range as the lowest value 00000000... when 
    * encoded as byte array.
    * 
    * Implements steps C->D1.
    */
   protected Long encodeRangeShift(final Long val, final Long minValue) {
      
      if (minValue==null) { // do nothing if range shift not set
         return val;
      }
      
      if (val<minValue) {
         throw new RuntimeException("Illegal range shift -- datatype violation.");
      }
      
      return Long.MIN_VALUE + (val - minValue);
   }
   
   /**
    * Pads a leading zero byte to the byte array. This changes the value (which 
    * does not harm order, if we do it consistently for all zOrder strings
    * prior to saving them) and makes sure that the array represents an unsigned
    * value, for which the two's complement representation does not differ.
    * More concretely, having padded the zero, we may safely call the
    * {@link BigInteger} constructor (which expects a two's complement input).
    * 
    * Implements step D->E.
    */
   public byte[] padLeadingZero(byte[] arr) {
      byte[] ret = new byte[arr.length+1];
      
      for (int i=0; i<arr.length; i++) {
         ret[i+1] = arr[i];
      }
      
      return ret;
      
   }

   
   
   /****************************************************************************
    * DIRECTION "UP":
    * -----------------
    * The following methods implement either the full or parts of the "up"
    * direction (which is the inverse of the down direction discussed in detail
    * above.
    ***************************************************************************/

   /**
    * Decodes an xsd:integer into an n-dimensional string of the form 
    * <int_1>#...#<int_n>.
    * 
    * Implements transformation G->A.
    */
   @SuppressWarnings({ "unchecked", "rawtypes" })
   @Override
   public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {
      
      // get the components represented by the IV (which must be of type
      // xsd:integer (G->C)
      final long[] componentsAsLongArr = asLongArray(iv);
      
      // set up the component and merge them into a string (C->B)
      final String litStr = longArrAsComponentString(componentsAsLongArr);
      
      // setup a literal carrying the component string (B->A)
      return (V) vf.createLiteral(litStr, datatype);
   }

   
   /**
    * Decodes an xsd:integer into the long values of the z-order components
    * represented through the xsd:integer.
    * 
    * Implements transformation G->C
    * @param iv
    * @return
    */
   @SuppressWarnings("rawtypes")
   public long[] asLongArray(final LiteralExtensionIV iv) {

      if (!datatype.getIV().equals(iv.getExtensionIV())) {
         throw new IllegalArgumentException("unrecognized datatype");
      }
      
      final int numDimensions = sd.getNumDimensions();
      
      final BigInteger bigInt = iv.getDelegate().integerValue();
   
      // convert BigInteger back to byte array (F->E)
      final byte[] bigIntAsByteArr = bigInt.toByteArray();
      
      // pad 0-bytes if necessary and copy over bytes; note that we make sure
      // to get the correct number of bytes (no trailing bytes may be skipped),
      // so the code below looks somewhat complex
      final int paddedArraySize = numDimensions * BASE_SIZE + 1;
      final byte[] bigIntAsByteArrPad = new byte[paddedArraySize];
      int idx = 0;
      for (int i = 0; i < paddedArraySize - bigIntAsByteArr.length; i++) {
         bigIntAsByteArrPad[idx++] = 0;   // padding
      }
      for (int i = 0; i < bigIntAsByteArr.length; i++) {
         bigIntAsByteArrPad[idx++] = bigIntAsByteArr[i];   // copy of bytes
      }

      // removed the padded zero (E->D2)
      byte[] bigIntAsByteArrUnsigned = unpadLeadingZero(bigIntAsByteArrPad);

      // retrieve the original long values from z-order byte[] (D2 -> C)
      long[] componentsAsLongArr = fromZOrderByteArray(bigIntAsByteArrUnsigned);
      
      return componentsAsLongArr;
   }

   
   /**
    * Converts a z-order byte array to a long array representing the components.
    * As part of this transformation, a possible range shift is reverted.
    * 
    * Implements transformation D2 -> C.
    */
   public long[] fromZOrderByteArray(final byte[] byteArr) {
      
      IKeyBuilder kb = KeyBuilder.newInstance(byteArr.length);
      for (int i=0; i<byteArr.length; i++) {
         kb.append(byteArr[i]);
      }
      
      final long[] componentsAsLongArr = kb.fromZOrder(sd.getNumDimensions());
      
      // revert range shift
      for (int i=0; i<componentsAsLongArr.length; i++) {
         final Long minValue = sd.getSchemaFieldDescription(i).getMinValue();
         if (minValue!=null) {
            componentsAsLongArr[i] = decodeRangeShift(componentsAsLongArr[i], minValue);
         }
      }
      
      return componentsAsLongArr;
   }
   
   
   /**
    * Invert {@link #encodeRangeShift(Long, Long)} operation.
    * Implements steps D1->C.
    */
   protected Long decodeRangeShift(final Long val, final Long minValue) {

      if (minValue==null) { // do nothing if range shift not set
         return val;
      }
      
      return val - Long.MIN_VALUE + minValue;
   }

   
   /**
    * Converts a a Long[] reflecting the long values of the individual 
    * components back into a component string representing the literal.
    * 
    * Implements a wrapper around step C->B.
    */
   public final String longArrAsComponentString(final long[] arr) {
      
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
    * Implements step C->B.
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

         final SchemaFieldDescription sfd = sd.getSchemaFieldDescription(i);
         final double precisionAdjustment = Math.pow(10, sfd.getDoublePrecision());
         
         if (i>0)
            buf.append(COMPONENT_SEPARATOR);
         
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
    * Reverts method {{@link #padLeadingZero(byte[])}.
    * 
    * Implements step E->D.
    */
   public byte[] unpadLeadingZero(byte[] arr) {
      
      byte[] ret = new byte[arr.length-1];
      
      for (int i=0; i<ret.length; i++) {
         ret[i] = arr[i+1];
      }
      
      return ret;
      
   }

      
   ///////////   ///////////   ///////////   ///////////   ///////////
   ///////////   ///////////   ///////////   ///////////   ///////////
   ///////////   ///////////   ///////////   ///////////   ///////////
   // TODO: how do these methods fit in? The could/should probably be optimized
   //       in terms of performance
   
   // NEW (TODO: should go via IVs numerical value, no string manip)
   @SuppressWarnings("rawtypes")
   public byte[] toZOrderByteArray(final LiteralExtensionIV iv) {
      
      final long[] componentsAsLongArr = asLongArray(iv);

      final byte[] zOrderByteArray = toZOrderByteArray(componentsAsLongArr, sd);

      return padLeadingZero(zOrderByteArray);

   }
   
   // NEW (TODO: should go via IVs numerical value, no string manip)
   public byte[] toZOrderByteArray(Object[] components) {
      
      final long[] componentsAsLongArr = componentsAsLongArr(components, sd);

      final byte[] zOrderByteArray = toZOrderByteArray(componentsAsLongArr, sd);

      return padLeadingZero(zOrderByteArray);

   }
   
   
   // NEW (TODO: should go via IVs numerical value, no string manip)
   @SuppressWarnings("unchecked")
   public V asValue(byte[] key, final BigdataValueFactory vf) {
    
      byte[] bigIntAsByteArrUnsigned = unpadLeadingZero(key);
      
      long[] componentsAsLongArr = fromZOrderByteArray(bigIntAsByteArrUnsigned);

      final String litStr = longArrAsComponentString(componentsAsLongArr);
      
      return (V) vf.createLiteral(litStr, datatype);

   }
   


   /****************************************************************************
    ***************               SCHEMA MANAGEMENT              ***************
    ***************************************************************************/
   /**
    * Gets the number of dimensions from the underlying schema description
    */
   public int getNumDimensions() {
      return sd.getNumDimensions();
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

//      sfd.add(new SchemaFieldDescription(Datatype.LONG, 1, Long.valueOf(0)));  /* time */
//      sfd.add(new SchemaFieldDescription(Datatype.LONG, 1, Long.valueOf(0)));  /* time */
      
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
      
      public Long minValue; // the minimun value appearing in the data

      private final Datatype datatype;

      /**
       * Precision of float value. Based on the precision, we convert the
       * double into a long value. E.g., if the precision is 8 (meaning
       * 8 positions after decimal point), we multiply the double by 8
       * and convert into a long value (ignoring additional positions).
       * 
       * The range supported is the full range.
       */
      private final int doublePrecision;
      
      public SchemaFieldDescription(
         final Datatype datatype, final int doublePrecision) {
         
         this(datatype, doublePrecision, null);
      }
      
      /**
       * Precision of float value. Based on the precision, we convert the
       * double into a long value. E.g., if the precision is 8 (meaning
       * 8 positions after decimal point), we multiply the double by 8
       * and convert into a long value (ignoring additional positions).
       * 
       * The minValue and maxValue parameters allow to set a range in which
       * we know the parameters are. This is used internally for range
       * adjustments and may help to get better performance.
       */
      public SchemaFieldDescription(
            final Datatype datatype, final int doublePrecision,
            final Long minValue) {
         
            this.datatype = datatype;
            this.doublePrecision = doublePrecision;
            this.minValue = minValue;
                  
         }
      
      public Datatype getDatatype() {
         return datatype;
      }

      public int getDoublePrecision() {
         return doublePrecision;
      }
      
      public Long getMinValue() {
         return minValue;
      }

      
   }

}
