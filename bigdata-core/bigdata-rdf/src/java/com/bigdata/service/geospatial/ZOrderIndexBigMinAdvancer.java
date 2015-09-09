/*

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
 * Created on Sep 9, 2015
 */

package com.bigdata.service.geospatial;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.filter.Advancer;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.IVUtility;
import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.util.BytesUtil;

/**
 * Advances the cursor to the next zOrderKey that is greater or equal than the
 * first point in the next region. Note that this next key is not necessarily a
 * hit (but, depending on the data) this might be a miss again.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class ZOrderIndexBigMinAdvancer extends Advancer<SPO> {

   private static final long serialVersionUID = -6438977707376228799L;

   private final byte[] searchMin;
   
   private final byte[] searchMax;
   
   private final int zOrderComponentPos;
   
   private final GeoSpatialLiteralExtension<BigdataValue> litExt;
   
   private transient IKeyBuilder keyBuilder;

   public ZOrderIndexBigMinAdvancer(
      final byte[] searchMin, /* the minimum search key (top left) */
      final byte[] searchMax, /* the maximum search key (bottom right) */
      final GeoSpatialLiteralExtension<BigdataValue> litExt,
      final int zOrderComponentPos /* position of the zOrder in the index */) {

      this.litExt = litExt;
      this.searchMin = litExt.twosComplementToUnsigned(searchMin);
      this.searchMax = litExt.twosComplementToUnsigned(searchMax);
      this.zOrderComponentPos = zOrderComponentPos;
           
   }

   @Override
   protected void advance(final ITuple<SPO> tuple) {

      // check if the current tuple represents a match in the boundary box
      
      if (keyBuilder == null) {

         /*
          * Note: It appears that you can not set this either implicitly or
          * explicitly during ctor initialization if you want it to exist during
          * de-serialization. Hence it is initialized lazily here. This is Ok
          * since the iterator pattern is single threaded.
          */

         // assert arity == 3 || arity == 4;

         keyBuilder = KeyBuilder.newInstance();

      }
      
      final byte[] key = tuple.getKey();

      keyBuilder.reset();

      // TODO: need to parameterize, encoding n components (up to the z-order string)
      // decode components up to (and including) the z-order string
      IV[] ivs = IVUtility.decode(key,zOrderComponentPos+1);
      
      // encode everything up to (and excluding) the z-order component "as is"
      for (int i=0; i<ivs.length-1; i++) {
         IVUtility.encode(keyBuilder, ivs[i]);
      }
      
      // calculate bigmin over the z-order component
      @SuppressWarnings("unchecked")
      byte[] bigMin = 
         calculateBigMin(
            (LiteralExtensionIV<BigdataLiteral>)ivs[ivs.length-1] /* current z-order string */);
      
      // TODO: is this correct?
      keyBuilder.append(litExt.unsignedToTwosComplement(bigMin));
      
      System.out.println();
//
//      /*
//       * new approach.
//       */
//
//      final byte[] key = tuple.getKey();
//
//      keyBuilder.reset();
//
//      IVUtility.decode(key).encode(keyBuilder);
//
//      final byte[] fromKey = keyBuilder.getKey();
//
//      final byte[] toKey = SuccessorUtil.successor(fromKey.clone());
//
//      src.seek(toKey);

   }

   /** 
    * Returns the BIGMIN, i.e. the next relevant value in the search range.
    * The value is returned as unsigned, which needs to be converted into
    * two's complement prior to appending as a key 
    * (see {@link GeoSpatialLiteralExtension} for details).
    * 
    * This method implements the BIGMIN decision table as provided in
    * http://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf,
    * see page 76.
    * 
    * @param iv the IV of the dividing record
    * @return
    */
   private byte[] calculateBigMin(LiteralExtensionIV<BigdataLiteral> iv) {
      
      final byte[] dividingRecord = 
         litExt.twosComplementToUnsigned(litExt.toZOrderByteArray(iv));
      
      if (dividingRecord.length!=searchMin.length ||
          dividingRecord.length!=searchMax.length) {
         throw new RuntimeException("Key range differs");
      } 
      
      final int numBytes = dividingRecord.length;
      final int numDimensions = litExt.getNumDimensions();

      final byte[] min = searchMin; 
      final byte[] max = searchMax;
      byte[] bigmin = new byte[numBytes];
      boolean finished = false;
      for (int i = 0; i < numBytes * Byte.SIZE && !finished; i++) { 

         // TODO: optimize through usage of mask
         boolean dividingRecordBitSet = BytesUtil.getBit(dividingRecord, i);
         boolean minBitSet = BytesUtil.getBit(searchMin, i);
         boolean maxBitSet = BytesUtil.getBit(searchMax, i);

         if (!dividingRecordBitSet) {
            
            if (!minBitSet) {
               
               if (!maxBitSet) {
                  
                  // case 0 - 0 - 0: continue (nothing to do)
                  
               } else {
                  
                  // case 0 - 0 - 1
                  bigmin = min;
                  load(true /* setFirst */, i, bigmin, numDimensions);
                  load(false, i, max, numBytes);
                  
               }
               
            } else {
               
               if (!maxBitSet) {
                  
                  // case 0 - 1 - 0
                  throw new RuntimeException("MIN must be <= MAX.");
                  
               } else {
                  
                  // case 0 - 1 - 1
                  bigmin = min;
                  finished = true; 
                  
               }               
            }
         } else {
            
            if (!minBitSet) {
               
               if (!maxBitSet) {
                  
                  // case 1 - 0 - 0
                  finished = true;
                  
               } else {
                  
                  // case 1 - 0 - 1
                  load(true, i, min, numBytes);
                  
               }
               
            } else {
               
               if (!maxBitSet) {
                  
                  // case 1 - 1 - 0
                  throw new RuntimeException("MIN must be <= MAX.");                  
                  
               } else {
                  
                  // case 1 - 1 - 1: continue (nothing to do)
                  
               }               
            }
            
         }
      }
      
      
      System.out.println("seachMin=" + searchMin);
      System.out.println("bigmin=" + bigmin);
      
      
      return bigmin;
   }
   
   /**
    * Implements the load function from p.75 in
    * http://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf:
    * 
    * If firstBitSet, then load 10000... is executed as defined in the paper
    * (setting the bit identified through position to 1 and all following bits
    * in the respective dimension to 0); otherwise load 01111... is executed
    * (which sets the bit identified through position to 0 and all following
    * bits of the dimension to 1). The method has no return value, but instead
    * as a side effect the passed array arr will be modified.
    */
   private static void load(
      final boolean setFirst, final int position,
      final byte[] arr, final int numDimensions) {
      
      // iterate over bits associated with the current dimension
      for (int i=position; i<arr.length * Byte.SIZE; i+=numDimensions) {
         BytesUtil.setBit(arr, i, i==position ? setFirst : !setFirst );
      }      
   }

}
