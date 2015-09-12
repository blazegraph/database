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
import com.bigdata.rdf.model.BigdataValueFactory;
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

   // Search min (upper left) byte array as z-order string (as unsigned)
   private final byte[] searchMinZOrder;

   // the long values representing search min's values in the dimensions
   private final long[] seachMinLong;
   
   // Search max (lower right) byte array as z-order string (as unsigned)
   private final byte[] searchMaxZOrder;
   
   // the long values representing search max's values in the dimensions
   private final long[] seachMaxLong;

   // the position within the index in which we find the zOrderComponent
   private final int zOrderComponentPos;
   
   // the GeoSpatialLiteralExtension object
   private final GeoSpatialLiteralExtension<BigdataValue> litExt;
   
   // value factory
   private final BigdataValueFactory vf;
   
   
   private transient IKeyBuilder keyBuilder;

   public ZOrderIndexBigMinAdvancer(
      final byte[] searchMinZOrder, /* the minimum search key (top left) */
      final byte[] searchMaxZOrder, /* the maximum search key (bottom right) */
      final GeoSpatialLiteralExtension<BigdataValue> litExt,
      final int zOrderComponentPos /* position of the zOrder in the index */,
      final BigdataValueFactory vf) {

      // TODO: is the decoding here incorrect? but we want to operator on top of these values...
      this.litExt = litExt;
      this.searchMinZOrder = litExt.unpadLeadingZero(searchMinZOrder);
      this.seachMinLong = litExt.fromZOrderByteArray(this.searchMinZOrder);
      
      this.searchMaxZOrder = litExt.unpadLeadingZero(searchMaxZOrder);
      this.seachMaxLong = litExt.fromZOrderByteArray(this.searchMaxZOrder);
      
      this.zOrderComponentPos = zOrderComponentPos;
      this.vf = vf;
           
   }

   @Override
   protected void advance(final ITuple<SPO> tuple) {
      System.out.println("[Advancor]    " + tuple);

      // if we're beyond the end, nothing to do
      if (tuple==null) {
         return;
      }
      
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

      // this is the z-order literal
      @SuppressWarnings("unchecked")
      final LiteralExtensionIV<BigdataLiteral> zOrderIv = 
         (LiteralExtensionIV<BigdataLiteral>)ivs[ivs.length-1];
      
      // current record (aka dividing record) as unsigned
      final byte[] dividingRecord = 
         litExt.unpadLeadingZero(litExt.toZOrderByteArray(zOrderIv));
      
      long[] divRecordComponents = litExt.fromZOrderByteArray(dividingRecord);
      
      
      // TODO: make sure to catch case where we're beyond the search range 
      // (in that case, we don't need to proceed)
      boolean inRange = true;
      for (int i=0; i<divRecordComponents.length && inRange; i++) {
         inRange &= seachMinLong[i]<=divRecordComponents[i];
         inRange &= seachMaxLong[i]>=divRecordComponents[i];
      }
      
      if (!inRange) {
         
         // calculate bigmin over the z-order component
         final byte[] bigMin = calculateBigMin(dividingRecord);
         
         // pad a zero
         final byte[] bigMinAsUnsigned = litExt.padLeadingZero(bigMin);
         
         final BigdataValue value = litExt.asValue(bigMinAsUnsigned, vf);
         
         // TODO: can this be done more efficiently?
         LiteralExtensionIV bigMinIv = litExt.createIV(value);
         IVUtility.encode(keyBuilder, bigMinIv);

         // advance to the specified key ...
         ITuple<SPO> next = src.seek(keyBuilder.getKey());
         // ... or the next higher one
         if (next==null) {
            next = src.next(); 
         }

         // go into recursion (if the next value is fine, this call will have
         // no effect, otherwise it will advance the cursor once more)
         advance(next);
         
      } // else: nothing to do

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
   private byte[] calculateBigMin(final byte[] dividingRecord) {
            
      if (dividingRecord.length!=searchMinZOrder.length ||
          dividingRecord.length!=searchMaxZOrder.length) {
         // TODO: proper error handling
         throw new RuntimeException("Key dimenisions differs");
      } 
      
      final int numBytes = dividingRecord.length;
      final int numDimensions = litExt.getNumDimensions();

      final byte[] min = searchMinZOrder.clone(); 
      final byte[] max = searchMaxZOrder.clone();
      byte[] bigmin = new byte[numBytes];
      boolean finished = false;
      for (int i = 0; i < numBytes * Byte.SIZE && !finished; i++) { 

         // TODO: optimize through usage of mask
         boolean dividingRecordBitSet = BytesUtil.getBit(dividingRecord, i);
         boolean minBitSet = BytesUtil.getBit(min, i);
         boolean maxBitSet = BytesUtil.getBit(max, i);

         if (!dividingRecordBitSet) {
            
            if (!minBitSet) {
               
               if (!maxBitSet) {
                  
                  // case 0 - 0 - 0: continue (nothing to do)
                  
               } else {
                  
                  // case 0 - 0 - 1
                  bigmin = min.clone();
                  load(true /* setFirst */, i, bigmin, numDimensions);
                  load(false, i, max, numBytes);
                  
               }
               
            } else {
               
               if (!maxBitSet) {
                  
                  // case 0 - 1 - 0
                  throw new RuntimeException("MIN must be <= MAX.");
                  
               } else {
                  
                  // case 0 - 1 - 1
                  bigmin = min.clone();
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
      
//      System.out.println("seachMin=" + searchMinZOrder);
//      System.out.println("bigmin=" + bigmin);
      
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
