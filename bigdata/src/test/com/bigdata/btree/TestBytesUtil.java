/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Jan 16, 2007
 */

package com.bigdata.btree;

import junit.framework.TestCase2;

/**
 * Test suite for low-level operations on variable length byte[]s.
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
     * and #of bytes to consider from that offset for each array. This version
     * is used for comparison of keys in {@link IKeyBuffer} implementations
     * since it allows a proven shared prefix to be skipped and also supports
     * search on the compact representation used by {@link ImmutableKeyBuffer}.
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

        System.err.println(BytesUtil.toString(seq));

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
     * 
     * @todo write a stress test with random keys and verify that the separator
     *       key is always greater than the priorKey and less than or equal to
     *       the givenKey. This stress test does not prove that the shortest
     *       separator key was always choosen, but it will prove that we never
     *       choose a separator key that lies outside of the legal range.
     */
    public void test_getSeparatorKey() {

        /*
         * tests where the keys are in the correct order and differ only in the
         * last byte of the _givenKey_. in this case the givenKey is always
         * returned to the caller.
         */
        {
            
            byte[] givenKey = new byte[]{1,2,2};
            
            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,1}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,0}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2,0,2}));

            assertTrue(givenKey == BytesUtil.getSeparatorKey(givenKey, new byte[]{1,2}));

        }
        
        // consider lots more cases.
//        byte[] k4 = new byte[]{1,2,3,1}; // @todo expect {1,2,3} since 5-3 > 1.
//        byte[] k5 = new byte[]{0,3,1};   // @todo expect {1,2,3} since 5-3 > 1.
//        byte[] k6 = new byte[]{1,3,1};   // @todo expect {1,2,3} since 5-3 > 1.
//        byte[] k7 = new byte[]{0,3,1};   // @todo expect exception.
        
        {
            /*
             * the keys differ at index := 1. The successor is formed by
             * extracting prefixLength+1 bytes from the prior key and adding one
             * to the last byte. (@todo If the last byte is 255 then we would
             * append a nul or choose the givenKey, whichever is shorter.)
             */
            byte[] gk = new byte[]{1,2,3}; // givenKey
            byte[] pk = new byte[]{1,1,2}; // expect {1,2}

            assertEquals(2,BytesUtil.getPrefixLength(gk, pk));
            
            byte[] sk1 = BytesUtil.getSeparatorKey(gk, pk);

            System.err.println("sk1=" + BytesUtil.toString(sk1));

            assertEquals(0, BytesUtil.compareBytes(new byte[] { 1, 2 }, sk1));
            
        }

        {
            /*
             * the keys differ at index := 1. The successor is formed by
             * extracting prefixLength+1 bytes from the prior key and adding one
             * to the last byte. The last byte is the maximum value of a signed
             * byte so this tests that we are performing unsigned addition.
             * (@todo If the last byte is 255 then we would append a nul or
             * choose the givenKey, whichever is shorter.)
             */
            byte[] gk = new byte[]{1,Byte.MIN_VALUE,3}; // givenKey
            byte[] pk = new byte[]{1,Byte.MAX_VALUE}; // expect {1,2}

            assertEquals(2,BytesUtil.getPrefixLength(gk, pk));
            
            byte[] sk1 = BytesUtil.getSeparatorKey(gk, pk);

            System.err.println("sk1=" + BytesUtil.toString(sk1));

            assertEquals(0, BytesUtil.compareBytes(new byte[] { 1, 2 }, sk1));
            
        }

        {

            byte[] gk = new byte[]{1,2,3}; // givenKey
            byte[] pk = new byte[]{1,2,1}; // expect {1,2,2} since differ in last byte only and 3-1 > 1.

            assertEquals(2,BytesUtil.getPrefixLength(gk, pk));

            byte[] sk2 = BytesUtil.getSeparatorKey(gk, pk);

            System.err.println("sk2=" + BytesUtil.toString(sk2));

            assertEquals(0, BytesUtil.compareBytes(new byte[] { 1, 2, 3 }, sk2));
        }

    }

    public void test_getSeparatorKey_correctRejection() {
        
        try { // null parameter.
            BytesUtil.getSeparatorKey(null, new byte[]{2});
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        
        try { // null parameter.
            BytesUtil.getSeparatorKey(new byte[]{2},null);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        try { // same reference (so givenKey and priorKey must be equal).
            byte[] tmp = new byte[]{2};
            BytesUtil.getSeparatorKey(tmp,tmp);
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }

        /*
         * given [1,2,3] and [1,2]    , prefixLength=2 -- ok.
         * given [1,2,3] and [1,2,2]  , prefixLength=2 -- ok.
         * given [1,2,3] and [1,2,2,3], prefixLength=2 -- ok.
         * given [1,2,3] and [1,2,3]  , prefixLength=3 -- keys out of order.
         * given [1,2,3] and [1,2,4]  , prefixLength=2 -- keys out of order.
         * given [1,3]   and [1,2,4]  , prefixLength=1 -- ok.
         */
        BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2}); // legal
        BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,2}); // legal
        BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,2,3}); // legal
        try { // keys out of order.
            BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,3}); // illegal
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        try { // keys out of order.
            BytesUtil.getSeparatorKey(new byte[]{1,2,3},new byte[]{1,2,4}); // illegal
            fail("Expecting: "+IllegalArgumentException.class);
        } catch(IllegalArgumentException ex) {
            System.err.println("Ignoring expected exception: "+ex);
        }
        BytesUtil.getSeparatorKey(new byte[]{1,3},new byte[]{1,2,4}); // legal
        
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

}
