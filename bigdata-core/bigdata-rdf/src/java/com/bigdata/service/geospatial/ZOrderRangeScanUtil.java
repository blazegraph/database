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
package com.bigdata.service.geospatial;

import com.bigdata.rdf.internal.impl.extensions.GeoSpatialLiteralExtension;
import com.bigdata.util.BytesUtil;

/**
 * Class providing utility functions for efficient zOrder-based multi-dimensional
 * range scans, such as efficient range checks and functionality for BigMin calculation.
 * The latter follows the logics defined in the BIGMIN decision table as provided in
 * http://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf, page 76.
 * 
 * This class is not thread-safe.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class ZOrderRangeScanUtil {
   
   // length of the zOrder array (number of bytes)
   final int zOrderArrayLength;
   
   // Search min (upper left) byte array as z-order string (no leading zero)
   private final byte[] searchMinZOrder;

   // Search max (lower right) byte array as z-order string (no leading zero)
   private final byte[] searchMaxZOrder;

   // number of dimensions
   private final int numDimensions;
   
   // reusable byte arrays for calculation.
   final byte[] min;
   final byte[] max;
   byte[] bigmin;
   
   
   /**
    * Constructor for the {@link ZOrderRangeScanUtil}.
    * 
    * @param searchMinZOrder the top-left (minimum) value
    * @param searchMaxZOrder the bottom-righ (maximum) value
    * @param numDimensions the number of dimensions
    */
   public ZOrderRangeScanUtil(
      final byte[] searchMinZOrder, final byte[] searchMaxZOrder, final int numDimensions) {

      this.searchMinZOrder = searchMinZOrder;
      this.searchMaxZOrder = searchMaxZOrder;
      this.numDimensions = numDimensions;
      this.zOrderArrayLength = searchMinZOrder.length;
      
      min = new byte[zOrderArrayLength];
      max = new byte[zOrderArrayLength];
      bigmin = new byte[zOrderArrayLength];
   }
   
   /**
    * Checks if the dividing record passed as arguments is in the
    * multi-dimensional search range defined by this class.
    */
   public boolean isInSearchRange(final byte[] dividingRecord) {
      
      final boolean dimShownToBeLargerThanMin[] = new boolean[numDimensions]; 
      final boolean dimShownToBeSmallerThanMax[] = new boolean[numDimensions]; 
      
      // get first byte in which the values differ
      int firstDifferingByte=0;
      for (;firstDifferingByte<dividingRecord.length; firstDifferingByte++) {
         if (dividingRecord[firstDifferingByte]!=searchMinZOrder[firstDifferingByte] ||
             dividingRecord[firstDifferingByte]!=searchMaxZOrder[firstDifferingByte]) {
            break;
         }
      }
               
      /**
       * We now scan sequentially over the bit array, starting with firstDifferingByte.
       * Thereby, we notice whenever we detect a smaller or greater situation (and
       * make sure that, for the dimension under investigation, we do not check again
       * in future). Note that this operation operates on top of the zOrder string, in
       * which bits are interleaved.
       * 
       * The unsatisfiedConstraintsCtr is for performance optimizations. It is initialized
       * with numDimensions*2, and when its count reaches zero we know that all
       * dimensions have to be shown larger than min and smaller than max, i.e. that all 
       * constraints have been satisfied.
       */
      int unsatisfiedConstraintsCtr = numDimensions*2; 
      for (int i=firstDifferingByte*Byte.SIZE; 
            i<dividingRecord.length*Byte.SIZE && unsatisfiedConstraintsCtr>0; i++) {
         
         final int dimension = i%numDimensions;
         final boolean divRecordBitSet = BytesUtil.getBit(dividingRecord, i);

         if (!dimShownToBeLargerThanMin[dimension]) {
            
            final boolean searchMinBitSet = BytesUtil.getBit(searchMinZOrder, i);

            if (divRecordBitSet && !searchMinBitSet) {
               
               dimShownToBeLargerThanMin[dimension] = true;
               unsatisfiedConstraintsCtr--;
               
            } else if (!divRecordBitSet && searchMinBitSet) {
               
               return false; // conflict
               
            } // else: skip
            
         }
         
         if (!dimShownToBeSmallerThanMax[dimension]) {
            
            final boolean searchMaxBitSet = BytesUtil.getBit(searchMaxZOrder, i);
            
            if (!divRecordBitSet && searchMaxBitSet) {
               
               dimShownToBeSmallerThanMax[dimension] = true;
               unsatisfiedConstraintsCtr--;
               
            } else if (divRecordBitSet && !searchMaxBitSet) {
               
               return false;
               
            } // else: skip
            
         }
         
      }
      
      return true; // all is good
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
   public byte[] calculateBigMin(final byte[] dividingRecord) {
            
      if (dividingRecord.length!=searchMinZOrder.length ||
          dividingRecord.length!=searchMaxZOrder.length) {
         
         // this should never happen, assuming correct configuration
         throw new RuntimeException("Key dimenisions differs");
      } 
      
      final int numBytes = dividingRecord.length;

      System.arraycopy(searchMinZOrder, 0, min, 0, zOrderArrayLength);
      System.arraycopy(searchMaxZOrder, 0, max, 0, zOrderArrayLength);
      java.util.Arrays.fill(bigmin,(byte)0); // reset bigmin
      
      boolean finished = false;
      for (int i = 0; i < numBytes * Byte.SIZE && !finished; i++) { 

         final boolean divRecordBitSet = BytesUtil.getBit(dividingRecord, i);
         final boolean minBitSet = BytesUtil.getBit(min, i);
         final boolean maxBitSet = BytesUtil.getBit(max, i);

         if (!divRecordBitSet) {
            
            if (!minBitSet) {
               
               if (!maxBitSet) {
                  
                  // case 0 - 0 - 0: continue (nothing to do)
                  
               } else {
                  
                  // case 0 - 0 - 1
                  System.arraycopy(min, 0, bigmin, 0, zOrderArrayLength);
                  load(true /* setFirst */, i, bigmin, numDimensions);
                  load(false, i, max, numDimensions);
                  
               }
               
            } else {
               
               if (!maxBitSet) {
                  
                  // case 0 - 1 - 0
                  throw new RuntimeException("MIN must be <= MAX.");
                  
               } else {
                  
                  // case 0 - 1 - 1
                  System.arraycopy(min, 0, bigmin, 0, zOrderArrayLength);
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
                  load(true, i, min, numDimensions);
                  
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
   public static void load(
      final boolean setFirst, final int position,
      final byte[] arr, final int numDimensions) {
      
      // set the trailing bit
      if (setFirst)
         arr[(int)(position / Byte.SIZE)] |= 1<<7-(position%8);
      else
         arr[(int)(position / Byte.SIZE)] &= ~(1<<7-(position%8));

      // set the remaining bits (inverted)
      for (int i=position+numDimensions; i<arr.length * Byte.SIZE; i+=numDimensions) {
         
         final int posInByte = i%Byte.SIZE;

         // for performance reasons, we aggregate all changes to the current byte
         int posInByteInv = 7 - posInByte;
         int mask = 1 << posInByteInv;
         for (posInByteInv-=numDimensions; posInByteInv>=0; posInByteInv-=numDimensions) {
            mask |= 1 << posInByteInv;
            i+=numDimensions; // corresponds to one time skip of outer loop
         }
         
         if (setFirst)
            arr[(int) (i / Byte.SIZE)] &= ~mask;
         else
            arr[(int) (i / Byte.SIZE)] |= mask;
         
      }

   }

}
