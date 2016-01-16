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
 * Created on Jan 16, 2007
 */

package com.bigdata.btree;

import junit.framework.TestCase2;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.util.BytesUtil;

/**
 * Test suite for low-level operations on variable length byte[]s.
 * 
 * @see BytesUtil
 * 
 * @todo test with JNI integration.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestBytesUtil extends TestCase2 {

    /**
     * 
     */
    public TestBytesUtil() {
    }

    /**
     * @param name
     */
    public TestBytesUtil(String name) {
        super(name);
    }

    public void test_getByteCount() {

        try {
            BytesUtil.getByteCount(null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        try {
            BytesUtil.getByteCount("");
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
        try {
            BytesUtil.getByteCount("3x");
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        try {
            BytesUtil.getByteCount("x2");
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }

        assertEquals(3, BytesUtil.getByteCount("3"));

        assertEquals(3 * 1024L, BytesUtil.getByteCount("3k"));
        assertEquals(3 * 1024L, BytesUtil.getByteCount("3kb"));
        assertEquals(3 * 1024L, BytesUtil.getByteCount("3K"));
        assertEquals(3 * 1024L, BytesUtil.getByteCount("3KB"));
        assertEquals(3 * 1024L, BytesUtil.getByteCount("3Kb"));

        assertEquals(3 * 1024 * 1024L, BytesUtil.getByteCount("3m"));
        assertEquals(3 * 1024 * 1024L, BytesUtil.getByteCount("3mb"));
        assertEquals(3 * 1024 * 1024L, BytesUtil.getByteCount("3M"));
        assertEquals(3 * 1024 * 1024L, BytesUtil.getByteCount("3MB"));
        assertEquals(3 * 1024 * 1024L, BytesUtil.getByteCount("3Mb"));

        assertEquals(3 * 1024 * 1024 * 1024L, BytesUtil.getByteCount("3g"));
        assertEquals(3 * 1024 * 1024 * 1024L, BytesUtil.getByteCount("3gb"));
        assertEquals(3 * 1024 * 1024 * 1024L, BytesUtil.getByteCount("3G"));
        assertEquals(3 * 1024 * 1024 * 1024L, BytesUtil.getByteCount("3GB"));
        assertEquals(3 * 1024 * 1024 * 1024L, BytesUtil.getByteCount("3Gb"));

    }
    
    /**
     * A byte has a signed value between -128 and 127.
     * 
     * Note: we need to compare bytes as unsigned values. You can convert to a
     * signed integer using
     * 
     * <pre>
     *      byte b = ...;
     *      int signed_b = b &amp; 0xff;
     * </pre>
     */
    public void test_compareBytesSigned() {
        
        byte bmin = Byte.MIN_VALUE;
        byte b0 = 0;
        byte bmax = Byte.MAX_VALUE;
        
        assertEquals(-128,bmin);
        assertEquals(127,bmax);

        // signed byte comparisons.
        assertTrue(bmin<b0);
        assertTrue(bmin<bmax);
        assertTrue(b0<bmax);

        // unsigned byte comparisons.
        assertTrue((bmin&0xff)>bmax);
        assertTrue((bmax&0xff)>b0);
        
    }

    /**
     * Test comparison of two variable length signed byte[]s.
     */
    public void test_compareBytes() {

        /*
         * test version with no offsets.
         */
        
        assertEquals(0, BytesUtil.compareBytes(//
                new byte[] { 1, 3, 2 },
                new byte[] { 1, 3, 2 }));

        // differ at index := 2 and a<b.
        assertTrue(BytesUtil.compareBytes(//
                new byte[] { 1, 3 }, //
                new byte[] { 1, 3, 2 }//
                ) < 0);

        // differ at index := 2 and a>b.
        assertTrue(BytesUtil.compareBytes(//
                new byte[] { 1, 3, 2 },//
                new byte[] { 1, 3 }//
                ) > 0);

        /*
         * validate comparator treat bytes as unsigned. -1 expressed as a
         * unsigned byte is larger than the largest signed byte.
         */
        assertTrue(BytesUtil.compareBytes(//
                new byte[] { -1 },//
                new byte[] { Byte.MAX_VALUE }//
                ) > 0);

    }

    /**
     * Test of unsigned byte[] comparison with explicit offset into each array
     * and #of bytes to consider from that offset for each array.
     */
    public void test_compareBytesWithOffsetAndLength() {

        /*
         * test with zero offsets and true lengths.
         */
        
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(//
                0, 3, new byte[] { 1, 3, 2 },//
                0, 3, new byte[] { 1, 3, 2 }//
                ));

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                0, 2, new byte[] { 1, 3 },//
                0, 3, new byte[] { 1, 3, 2 }//
                ) < 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                0, 3, new byte[] { 1, 3, 2 },//
                0, 2, new byte[] { 1, 3 }//
                ) > 0);

        /*
         * test with non-zero offsets and all remaining bytes are compared.
         */
        
        assertEquals(0, BytesUtil.compareBytesWithLenAndOffset(//
                1, 3, new byte[] { 9, 1, 3, 2 },//
                0, 3, new byte[] { 1, 3, 2 }//
                ));

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 2, new byte[] { 9, 1, 3 },//
                0, 3, new byte[] { 1, 3, 2 }//
                ) < 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 3, new byte[] { -9, 1, 3, 2 },//
                0, 2, new byte[] { 1, 3 }//
                ) > 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 3, new byte[] { 9, 1, 3, 2 },//
                0, 4, new byte[] { 1, 3, 2, 1 }//
                ) < 0);

        /*
         * test with non-zero offsets and only some of the remaining bytes are
         * compared.
         */
        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 2, new byte[] { 9, 1, 3, 2 },// using {1,3}
                0, 2, new byte[] { 1, 3, 3, 1 }// using {1,3}
                ) == 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 2, new byte[] { 9, 1, 3, 2 },// using {1,3}
                0, 3, new byte[] { 1, 3, 3, 1 }// using {1,3,3}
                ) < 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 3, new byte[] { 9, 1, 3, 2 },// using {1,3,2}
                0, 2, new byte[] { 1, 3, 3, 1 }// using {1,3}
                ) > 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 3, new byte[] { 9, 1, 3, 2 },// using {1,3,2}
                0, 3, new byte[] { 1, 3, 3, 1 }// using {1,3,3}
                ) < 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                1, 3, new byte[] { 9, 1, 3, 2 },// using {1,3,2}
                0, 4, new byte[] { 1, 3, 3, 1 }// using {1,3,3,1}
                ) < 0);

        assertTrue(BytesUtil.compareBytesWithLenAndOffset(//
                0, 4, new byte[] { 9, 1, 3, 2 },// using {9,1,3,2}
                0, 4, new byte[] { 1, 3, 3, 1 }// using {1,3,3,1}
                ) > 0);
        
    }
    
    /**
     * Test method that returns the length of the longest common prefix for two
     * keys.
     */
    public void test_getPrefixLength() {

        assertEquals(0, BytesUtil.getPrefixLength(new byte[] { 1 },
                new byte[] { 2 }));
        
        assertEquals(1, BytesUtil.getPrefixLength(new byte[] { 1 },
                new byte[] { 1 }));
        
        assertEquals(2, BytesUtil.getPrefixLength(new byte[] { 1, 2, 3 },
                new byte[] { 1, 2, 4 }));
        
        assertEquals(2, BytesUtil.getPrefixLength(new byte[] { 1, 2 },
                new byte[] { 1, 2, 4 }));
        
    }

    /**
     * Test method that returns the longest common prefix for two keys.
     */
    public void test_getPrefix() {

        assertEquals(new byte[] {}, BytesUtil.getPrefix(new byte[] { 1 },
                new byte[] { 2 }));

        assertEquals(new byte[] { 1 }, BytesUtil.getPrefix(new byte[] { 1 },
                new byte[] { 1 }));

        assertEquals(new byte[] { 1, 2 }, BytesUtil.getPrefix(new byte[] { 1,
                2, 3 }, new byte[] { 1, 2, 4 }));

        assertEquals(new byte[] { 1, 2 }, BytesUtil.getPrefix(
                new byte[] { 1, 2 }, new byte[] { 1, 2, 4 }));

    }

    /**
     * Verify the semantics of the successor of a byte[] and that the successor
     * is computed correctly by {@link BytesUtil#successor(byte[])}.
     */
    public void test_successor() {

        /*
         * verify that compareBytes believes that the successor is formed by
         * appending a zero(0) byte.
         */
        assertTrue(BytesUtil.compareBytes(new byte[]{}, new byte[]{0})<0);
        assertTrue(BytesUtil.compareBytes(new byte[]{1}, new byte[]{1,0})<0);
        assertTrue(BytesUtil.compareBytes(new byte[]{1,0}, new byte[]{2})<0);

        assertTrue(BytesUtil.compareBytes(new byte[]{minSignedByte}, new byte[]{0})>0);
        assertTrue(BytesUtil.compareBytes(new byte[]{maxSignedByte}, new byte[]{0})>0);

        {
            byte[] zero = new byte[] { 0 };
            byte[] a = new byte[1];
            for (int i = minSignedByte; i <= maxSignedByte; i++) {
                
                a[0] = (byte) (i & 0xff);

                if(a[0]==zero[0]) continue; // skip over zero.
                
                assertTrue(BytesUtil.compareBytes(a, zero) > 0);

            }
        }
        
        // successor of an empty byte[].
        assertEquals(new byte[]{0}, BytesUtil.successor(new byte[]{}));
        
        // successor of a non-empty byte[].
        assertEquals(new byte[]{1,3,1,0}, BytesUtil.successor(new byte[]{1,3,1}));

    }

    final private byte maxSignedByte = 127;
    final private byte minSignedByte = -128;

    /**
     * Test logic to add one to an unsigned byte in Java. A byte in Java is a
     * signed value, so we have to convert to and from a signed quantity with a
     * larger range in order to do this operation.
     * 
     * 127 is the maximum value for a signed byte.
     * 
     * Adding 1 to 127 produces the bit pattern for -128. Contining to add one
     * to -128 progresses the value to -127, -126, -125, ..., -1. The bit
     * pattern for -1 is 0xff, which is 255 when interpreted as an unsigned
     * byte.
     * 
     * A safe operation for adding one to an unsigned byte must first check
     * whether the value of the byte is -1, in which case adding one would cause
     * overflow and result in a bit pattern of all zeros.
     * 
     * @see http://en.wikipedia.org/wiki/Two's_complement
     */
    public void test_unsignedByteInc() {

        assertEquals( (byte)-128, (byte)((((byte) 127) & 0xff) + 1));
        assertEquals( (byte)-127, (byte)((((byte)-128) & 0xff) + 1));

        byte b = 0;
        
        byte[] seq = new byte[256];
        
        int n = 0;

        seq[n++] = b;
        
        while( b != -1 ) {
            
            b = (byte)(((b) & 0xff) + 1);
            
            seq[n++] = b;
            
        }

        if(log.isInfoEnabled())
        	log.info(BytesUtil.toString(seq));

        /*
         * verify bytes in sequence corresponding to [0:255] when interpreted as
         * integers having a sufficent range to express the values.
         */
        
        for(int i=0; i<255; i++) {
            
            assertEquals(i, (seq[i] & 0xff));
            
        }
        
    }
    
    /**
     * Test method that chooses the shortest key that is less than one key but
     * greater than another.
     */
    public void test_getSeparatorKey() {

        /*
         * Bigbird, Burt, Cookiemonster, Ernie, Snuffleopogus
         */
        {
        
            // instance does not support unicode
            IKeyBuilder keyBuilder = KeyBuilder.newInstance();
            
            // first key for new right sibling : convert ASCII to byte[].
            byte[] givenKey = keyBuilder.reset().appendASCII("CookieMonster").getKey();

            assertEquals('C',(char)KeyBuilder.decodeByte(givenKey[0]));
            
            // last key for current node (prior key): convert ASCII to byte[].
            byte[] priorKey = keyBuilder.reset().appendASCII("Ernie").getKey();
            
            byte[] separatorKey = BytesUtil.getSeparatorKey(givenKey,priorKey);

            assertEquals(1,separatorKey.length);

            assertEquals('C',(char)KeyBuilder.decodeByte(separatorKey[0]));            
            
        }
        
        /*
         * Tests where the keys are in the correct order and differ only in the
         * last byte of the _givenKey_, including the case where the prior key
         * is a prefix of the given key and is lacking only the last byte of the
         * given key. In this case the givenKey is always returned to the caller
         * (by reference).
         */
        {
            
            byte[] givenKey = new byte[]{1,2,2};
            
            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,1}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,1,2}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,0}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,0,2}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2}));

        }
        
        /*
         * Edge case.  The prior key is an empty byte[].
         */
        {

            byte[] given = new byte[]{7};
            byte[] prior = new byte[]{};
            
            assertEquals(given, // separator
                    BytesUtil.getSeparatorKey(//
                            given,//
                            prior //
                            ));
            
        }

        /*
         * Tests in which there is a zero length shared prefix. In this case the
         * separator is always byte[1] and contains the first byte from the
         * given key.
         */
        {
            
            assertEquals(new byte[] {3}, // separator
                    BytesUtil.getSeparatorKey(//
                            new byte[] { 3, 5, 7 },// given
                            new byte[] { 1, 5, 7 }// prior
                            ));
            
            assertEquals(new byte[] {7}, // separator
                    BytesUtil.getSeparatorKey(//
                            new byte[] { 7 },// given
                            new byte[] { 5, 1 }// prior
                            ));
            
        }
        
        /*
         * Tests in which there is a non-zero length shared prefix but the keys
         * differ before the last byte. In this case the separator is formed
         * from all bytes in the shared prefix plus the next byte from the given
         * key.
         */
        {

            assertEquals(new byte[] {7, 1}, // separator
                    BytesUtil.getSeparatorKey(//
                            new byte[] { 7, 1, 3 },// given
                            new byte[] { 7,      } // prior
                            ));

            assertEquals(new byte[] {7, 1, 1}, // separator
                    BytesUtil.getSeparatorKey(//
                            new byte[] { 7, 1, 1, 3 },// given
                            new byte[] { 7, 1       } // prior
                            ));

        }
        
    }

    public void test_getSeparatorKey_correctRejection() {
        
        try { // null parameter.
            BytesUtil.getSeparatorKey(null, new byte[]{2});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            if(log.isInfoEnabled())
            	log.info("Ignoring expected exception: "+ex);
        }
        
        try { // null parameter.
            BytesUtil.getSeparatorKey(new byte[]{2},null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
        	if(log.isInfoEnabled())
            	log.info("Ignoring expected exception: "+ex);
        }

        try { // same reference (so givenKey and priorKey must be equal).
            byte[] tmp = new byte[]{2};
            BytesUtil.getSeparatorKey(tmp,tmp);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
        	if(log.isInfoEnabled())
            	log.info("Ignoring expected exception: "+ex);
        }

        /*
         * I have commented out the rest of the correct rejection tests. This
         * makes the code must more complex in getSeparatorKey() and you have to
         * compare bytes as unsigned values to get it right, which adds to the
         * complexity again. I don't see the need for these tests since the
         * B+Tree is never presenting the arguments out of order - the various
         * stress tests demonstrate that it correctly maintains its order
         * regardless.
         */
        
//        /*
//         * given [1,2,3] and [1,2]    , prefixLength=2 -- ok.
//         * given [1,2,3] and [1,2,2]  , prefixLength=2 -- ok.
//         * given [1,2,3] and [1,2,2,3], prefixLength=2 -- ok.
//         * given [1,2,3] and [1,2,3]  , prefixLength=3 -- keys out of order.
//         * given [1,2,3] and [1,2,4]  , prefixLength=2 -- keys out of order.
//         * given [1,3]   and [1,2,4]  , prefixLength=1 -- ok.
//         */
//        BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2}); // legal
//        BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,2}); // legal
//        BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,2,3}); // legal
//        try { // keys out of order.
//            BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,3}); // illegal
//            fail("Expecting: "+IllegalArgumentException.class);
//        } catch(IllegalArgumentException ex) {
//            System.err.println("Ignoring expected exception: "+ex);
//        }
//        try { // keys out of order.
//            BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,4}); // illegal
//            fail("Expecting: "+IllegalArgumentException.class);
//        } catch(IllegalArgumentException ex) {
//            System.err.println("Ignoring expected exception: "+ex);
//        }
//        BytesUtil.getSeparatorKey(new byte[]{1,3},new byte[]{1,2,4}); // legal
        
    }
    
    /**
     * A series of test cases for binary search for a "key" in an array of keys
     * with an adjustable base (starting offset) and length (#of members). This
     * tests for "key not found" resulting in a variety of insertion points
     * (including before and after all records in the array) as well as for key
     * found at a variety of locations. The first and last members of the array
     * are then "hidden" by shifting the base and reducing the #of declared
     * array elements and the tests are repeated in the new context to verify
     * that the base and nmem parameters are correctly respected.
     */
    public void test_binarySearch01()
    {

    final int capacity = 5;

    final byte[][] keys = new byte[ capacity ][];

    int base = 0;

    int nmem = 5;

    // The general formula for the record offset is:
    //
    //    offset := sizeof(record) * ( index - 1 )
    //
    // The general formula for the insertion point is:
    //
    //    insert := - ( offset + 1 )
    //
    // where [offset] is the offset of the record before which the
    // new record should be inserted.

    int i = 0;
    keys[ i ++ ] =  new byte[]{  5 };    // offset := 0, insert before := -1
    keys[ i ++ ] =  new byte[]{  7 };    // offset := 1, insert before := -2
    keys[ i ++ ] =  new byte[]{  9 };    // offset := 2, insert before := -3
    keys[ i ++ ] =  new byte[]{ 11 };    // offset := 3, insert before := -4
    keys[ i ++ ] =  new byte[]{ 13 };    // offset := 4, insert before := -5
                                        //               insert after := -6

    //
    // verify offset of record found.
    //

    // Verify finds the first record in the array.
    assertEquals
        ( 0,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 5 }
        )
          );

    // Verify finds the 2nd record in the array.
    assertEquals
        ( 1,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 7 }
        )
          );

    // Verify finds the penultimate record in the array.
    assertEquals
        ( 3,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 11 }
        )
          );

    // Verify finds the last record in the array.
    assertEquals
        ( 4,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 13 }
        )
          );

    //
    // verify insertion points (key not found).
    //

    // Verify insertion point for key less than any value in the
    // array.
    assertEquals
        ( -1,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 4 }
        )
          );
    
    // Verify insertion point for key between first and 2nd
    // records.
    assertEquals
        ( -2,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 6 }
        )
          );

    // Verify insertion point for key between penultimate and last
    // records.
    assertEquals
        ( -5,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 12 }
        )
          );

    // Verify insertion point for key greater than the last record.
    assertEquals
        ( -6,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 14 }
        )
          );

    //
    // Shift the array base and the #of members so that only the
    // three records in the middle of the array are "exposed" to
    // the search and then do a series of tests to verify that the
    // [base] and [nmem] parameters are being correctly respected.
    //
    
    base++;     // hides the first record.

    nmem -= 2;  // hides the last record (we have to
                // subtract two since we are actually
                // hiding two members).

    //
    // Verify offset of "hidden" records, which now qualify as
    // "not found".
    //

    // Verify does not find the first record in the array (which
    // is now "hidden").
    assertEquals
        ( -2,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 5 }
        )
          );

    // Verify does not find the last record in the array (which is
    // now hidden).
    assertEquals
        ( -5,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 13 }
        )
          );

    //
    // verify offset of record found.
    //

    // Verify finds the 2nd record in the array (the first that is
    // now visible).
    assertEquals
        ( 1,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 7 }
        )
          );

    // Verify finds the 3rd record in the array (the 2nd that is
    // now visible).
    assertEquals
        ( 2,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 9 }
        )
          );

    // Verify finds the penultimate record in the array (the 3rd
    // and last that is now visibile).
    assertEquals
        ( 3,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 11 }
        )
          );

    //
    // verify insertion points (keys not found in either the
    // hidden or the full array).
    //
//  buf[ i ++ ] =   7L ;    // offset := 1, insert before := -2
//  buf[ i ++ ] =   9L ;    // offset := 2, insert before := -3
//  buf[ i ++ ] =  11L ;    // offset := 3, insert before := -4
//                                              insert after  := -5

    // Verify insertion point for key less than any value in the
    // array.
    assertEquals
        ( -2,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 6 }
        )
          );
    
    // Verify insertion point for key between first and 2nd
    // visible records.
    assertEquals
        ( -3,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 8 }
        )
          );

    // Verify insertion point for key between 2nd and 3rd visible
    // records.
    assertEquals
        ( -4,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 10 }
        )
          );

    // Verify insertion point for key afer the last visible
    // record.
    assertEquals
        ( -5,
          BytesUtil.binarySearch
          ( keys, base, nmem, new byte[]{ 12 }
        )
          );

    }

    /*
     * Bit operations
     */
    
    public void test_bitFlagByteLength() {
        
        assertEquals(0,BytesUtil.bitFlagByteLength(0));
        assertEquals(1,BytesUtil.bitFlagByteLength(1));
        assertEquals(1,BytesUtil.bitFlagByteLength(2));
        assertEquals(1,BytesUtil.bitFlagByteLength(3));
        assertEquals(1,BytesUtil.bitFlagByteLength(4));
        assertEquals(1,BytesUtil.bitFlagByteLength(5));
        assertEquals(1,BytesUtil.bitFlagByteLength(6));
        assertEquals(1,BytesUtil.bitFlagByteLength(7));
        assertEquals(1,BytesUtil.bitFlagByteLength(8));
        assertEquals(2,BytesUtil.bitFlagByteLength(9));
        
    }
    
    public void test_bitSetTest() {
        
        final int nbits = 12;

        for (long bitIndex = 0; bitIndex < nbits; bitIndex++) {

            final byte[] buf = new byte[BytesUtil.bitFlagByteLength(nbits)];

            // should be clear
            assertEquals("get(" + bitIndex + ")", false, BytesUtil.getBit(buf,
                    bitIndex));

            // set
            assertEquals("set(" + bitIndex + ")", false, BytesUtil.setBit(buf,
                    bitIndex, true));

            // should be set.
            assertEquals("get(" + bitIndex + ")", true, BytesUtil.getBit(buf,
                    bitIndex));

            // clear
            assertEquals("set(" + bitIndex + ")", true, BytesUtil.setBit(buf,
                    bitIndex, false));

            // should be clear
            assertEquals("get(" + bitIndex + ")", false, BytesUtil.getBit(buf,
                    bitIndex));

        }

    }


    /**
     * Correct rejection tests for {@link BytesUtil#getMSBMask(int)}
     */
    public void test_getMaskBits_correctRejection() {
        try {
            BytesUtil.getMSBMask(-1);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            // ignore
        }
        try {
        	BytesUtil.getMSBMask(33);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            // ignore
        }
    }
    
    /**
     * Unit test for {@link BytesUtil#getMSBMask(int)}
     */
    public void test_getMaskBits() {
        
        assertEquals(0x00000000, getMSBMask(0));
        assertEquals(0x80000000, getMSBMask(1));
        assertEquals(0xc0000000, getMSBMask(2));
        assertEquals(0xe0000000, getMSBMask(3));
        assertEquals(0xf0000000, getMSBMask(4));
        assertEquals(0xf8000000, getMSBMask(5));
        assertEquals(0xfc000000, getMSBMask(6));
        assertEquals(0xfe000000, getMSBMask(7));
        assertEquals(0xff000000, getMSBMask(8));
        assertEquals(0xff800000, getMSBMask(9));
        assertEquals(0xffc00000, getMSBMask(10));
        assertEquals(0xffe00000, getMSBMask(11));
        assertEquals(0xfff00000, getMSBMask(12));
        assertEquals(0xfff80000, getMSBMask(13));
        assertEquals(0xfffc0000, getMSBMask(14));
        assertEquals(0xfffe0000, getMSBMask(15));
        assertEquals(0xffff0000, getMSBMask(16));
        assertEquals(0xffff8000, getMSBMask(17));
        assertEquals(0xffffc000, getMSBMask(18));
        assertEquals(0xffffe000, getMSBMask(19));
        assertEquals(0xfffff000, getMSBMask(20));
        assertEquals(0xfffff800, getMSBMask(21));
        assertEquals(0xfffffc00, getMSBMask(22));
        assertEquals(0xfffffe00, getMSBMask(23));
        assertEquals(0xffffff00, getMSBMask(24));
        assertEquals(0xffffff80, getMSBMask(25));
        assertEquals(0xffffffc0, getMSBMask(26));
        assertEquals(0xffffffe0, getMSBMask(27));
        assertEquals(0xfffffff0, getMSBMask(28));
        assertEquals(0xfffffff8, getMSBMask(29));
        assertEquals(0xfffffffc, getMSBMask(30));
        assertEquals(0xfffffffe, getMSBMask(31));
        assertEquals(0xffffffff, getMSBMask(32));
        
    }

    /**
     * Test help logs the {@link BytesUtil#getMSBMask(int)} results for
     * inspection.
     * 
     * @param nbits
     *            The #of bits whose MSB mask is desired.
     *            
     * @return The MSB mask.
     */
    private int getMSBMask(final int nbits) {

        final int mask = BytesUtil.getMSBMask(nbits);

        if(log.isInfoEnabled()) {
            System.err.printf("%2d : [%32s]\n", nbits, Integer.toBinaryString(mask));
        }

        return mask;

    }
    
    /**
     * Unit test for {@link BytesUtil#maskOffMSB(int, int)}
     */
    public void test_maskOff() {

        assertEquals(0x00000003, BytesUtil.maskOffMSB(0xffffffff/* key */, 2/* nbits */));
        
        assertEquals(0x00000003, BytesUtil.maskOffMSB(0xc0000000/* key */, 2/* nbits */));

        assertEquals(0x00000006, BytesUtil.maskOffMSB(0xc0000000/* key */, 3/* nbits */));
        
        assertEquals(0x0000000c, BytesUtil.maskOffMSB(0xc0000000/* key */, 4/* nbits */));
        
        assertEquals(0x00000018, BytesUtil.maskOffMSB(0xc0000000/* key */, 5/* nbits */));
        
    }

    /*
     * toBitString() tests. 
     */
    
    public void test_toBitString_correctRejection() {

		try {
			BytesUtil.toBitString(null);
			fail("Expecting: " + IllegalArgumentException.class);
		} catch (IllegalArgumentException ex) {
			if (log.isInfoEnabled())
				log.info("Ignoring expected exception: " + ex);
		}
		
    }

    public void test_toBitString_emptyByteArray() {

    	// zero length byte[] => zero length bit string.
		assertEquals("", BytesUtil.toBitString(new byte[0]));

    }
    
    public void test_toBitString_oneByte() {

		assertEquals("00000000", BytesUtil.toBitString(new byte[] { 0 }));
		assertEquals("00000001", BytesUtil.toBitString(new byte[] { 1 << 0 }));
		assertEquals("00000010", BytesUtil.toBitString(new byte[] { 1 << 1 }));
		assertEquals("00000100", BytesUtil.toBitString(new byte[] { 1 << 2 }));
		assertEquals("00001000", BytesUtil.toBitString(new byte[] { 1 << 3 }));
		assertEquals("00010000", BytesUtil.toBitString(new byte[] { 1 << 4 }));
		assertEquals("00100000", BytesUtil.toBitString(new byte[] { 1 << 5 }));
		assertEquals("01000000", BytesUtil.toBitString(new byte[] { 1 << 6 }));
		assertEquals("10000000", BytesUtil.toBitString(new byte[] { (byte) (1 << 7) }));

    }
    
	public void test_toBitString_twoBytes() {

		assertEquals("00000010" + "00100000", BytesUtil.toBitString(new byte[] { 1 << 1,
				1 << 5 }));

    }

    public void testMaskOffLSB() {
        final int v1 = 0x07;
        
        assertTrue(BytesUtil.maskOffLSB(v1, 0) == 0);
        assertTrue(BytesUtil.maskOffLSB(v1, 1) == 0x01);
        assertTrue(BytesUtil.maskOffLSB(v1, 2) == 0x03);
        assertTrue(BytesUtil.maskOffLSB(v1, 3) == 0x07);
        assertTrue(BytesUtil.maskOffLSB(v1, 4) == 0x07);
        
        final int v2 = 0x70;
        
        assertTrue(BytesUtil.maskOffLSB(v2, 4) == 0);
        assertTrue(BytesUtil.maskOffLSB(v2, 5) == 0x10);
        assertTrue(BytesUtil.maskOffLSB(v2, 6) == 0x30);
        assertTrue(BytesUtil.maskOffLSB(v2, 7) == 0x70);
        assertTrue(BytesUtil.maskOffLSB(v2, 8) == 0x70);
    }

}
