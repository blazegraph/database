/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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
 * Created on Sep 4, 2007
 */

package com.bigdata.rawstore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.TestCase;


/**
 * Test suite for {@link WormAddressManager}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestWormAddressManager extends TestCase {

    Random r = new Random();

    /**
     * 
     */
    public TestWormAddressManager() {
        super();
    }

    public TestWormAddressManager(String name) {
        super(name);
    }

//    /**
//     * verify that the constructor correctly interprets is parameter as the #of
//     * offset bits.
//     */
//    public void test_ctor_default() {
//        
//        WormAddressManager am = new WormAddressManager();
//        
//        assertEquals("offsetBits", WormAddressManager.DEFAULT_OFFSET_BITS,
//                am.getOffsetBits() );
//        
//        assertEquals("maxOffset", 4 * Bytes.terabyte - 1, am.getMaxOffset());
//        
//        assertEquals("maxByteCount", 4 * Bytes.megabyte - 1, am.getMaxByteCount());
//        
//    }
    
    /**
     * Test of constructor when splitting the long integer into two 32-bit
     * unsigned integer components (offset and #bytes).
     */
    public void test_ctor_32bits() {
    
        WormAddressManager am = new WormAddressManager(32);
        
        assertEquals("offsetBits",32,am.offsetBits);
        
        assertEquals("byteCountBits",32,am.byteCountBits);
        
        assertEquals("maxOffset",0xffffffffL,am.maxOffset);
        
        /*
         * Note: 32 _unsigned_ bits can actually store more than can fit into a
         * signed 32-bit integer so we are modeling the limit as a long integer.
         */
        assertEquals("maxByteCount",0xffffffffL,am.maxByteCount);

//      private static final transient long OFFSET_MASK = 0xffffffff00000000L;

        assertEquals("offsetMask",0xffffffff00000000L,am.offsetMask);

//      private static final transient long NBYTES_MASK = 0x00000000ffffffffL;

        assertEquals("byteCountMask",0x00000000ffffffffL,am.byteCountMask);
        
    }
    
    /**
     * Test of constructor when splitting the long integer into a 48-bit
     * unsigned integer (offset) and a 16-bit unsigned integer (#bytes).
     */
    public void test_ctor_48bits() {

        WormAddressManager am = new WormAddressManager(48);
        
        assertEquals("offsetBits",48,am.offsetBits);
        
        assertEquals("byteCountBits",16,am.byteCountBits);
        
        assertEquals("maxOffset",0xffffffffffffL,am.maxOffset);
        
        assertEquals("maxByteCount",0xffffL,am.maxByteCount);

        assertEquals("offsetMask",0xffffffffffff0000L,am.offsetMask);

        assertEquals("byteCountMask",0x000000000000ffffL,am.byteCountMask);
        
    }

    public void test_ctor_correctRejection() {

        try {
            
            new WormAddressManager(0);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        try {
            
            new WormAddressManager(-1);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        try {
            
            new WormAddressManager(WormAddressManager.MIN_OFFSET_BITS-1);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }
        
        try {
            
            new WormAddressManager(WormAddressManager.MAX_OFFSET_BITS+1);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ignoring expected exception: "+ex);
            
        }

        /*
         * and verify that the end points of the range are legal.
         */
        new WormAddressManager(WormAddressManager.MIN_OFFSET_BITS);
        
        new WormAddressManager(WormAddressManager.MAX_OFFSET_BITS);

    }
    
    /**
     * Verify that {@link WormAddressManager#toAddr(int, long)} will reject a zero
     * byte count or a zero offset since the value <code>0L</code> is reserved
     * to represent a null reference.
     */
    public void test_canNotConstructNULLs() {
        
        for(int i=WormAddressManager.MIN_OFFSET_BITS; i<=WormAddressManager.MAX_OFFSET_BITS; i++) {

            WormAddressManager am = new WormAddressManager( i );
            
            try {
                
                am.toAddr(0/*nbytes*/, 1L/*offset*/);
                
            } catch(IllegalArgumentException ex) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            }

            try {
                
                am.toAddr(1/*nbytes*/, 0L/*offset*/);
                
            } catch(IllegalArgumentException ex) {
                
                System.err.println("Ignoring expected exception: "+ex);
                
            }
            
        }
        
    }
    
    /**
     * Test of encoding and decoding addressed with a small set of 32 bit
     * offsets.
     */
    public void test_get_32() {

        doTestGet(32, 1000);

    }

    /**
     * Test of encoding and decoding addressed with a small set of 48 bit
     * offsets.
     */
    public void test_get_48() {

        doTestGet(48, 1000);

    }

    /**
     * Test of encoding and decoding addresses with a set of addresses selected
     * from each of the legal values of the offsetBits.
     */
    public void test_get() {

        for (int i = WormAddressManager.MIN_OFFSET_BITS; i <= WormAddressManager.MAX_OFFSET_BITS; i++) {

            doTestGet(i, 10000);

        }

    }

    /**
     * Test of packing and unpacking addresses with a small set of addresses
     * using 32 bit offsets.
     */
    public void test_packUnpack_32() throws IOException {

        doTestPackUnpack(32, 100);

    }

    /**
     * Test of packing and unpacking addresses with a small set of addresses
     * using 48 bit offsets.
     */
    public void test_packUnpack_48() throws IOException {

        doTestPackUnpack(48, 100);

    }

    /**
     * Test of packing and unpacking addresses with a set of addresses selected
     * from each of the legal values of the offsetBits.
     */
    public void test_packUnpack() throws IOException {

        for (int i = WormAddressManager.MIN_OFFSET_BITS; i <= WormAddressManager.MAX_OFFSET_BITS; i++) {

            doTestPackUnpack(i, 10000);

        }

    }
    
    /**
     * Helper performs random tests of {@link WormAddressManager#toAddr(int, long)}
     * and is intended to verify both consistency of encoding and decoding and
     * correct rejection when encoding.
     * 
     * @param offsetBits 
     */
    public void doTestGet(int offsetBits,int limit) {

        WormAddressManager am = new WormAddressManager(offsetBits);

        for(int i=0; i<limit; i++) {

            /*
             * next #of bytes in [0:maxByteCount], but never more tha
             * Integer.MAX_VALUE bytes.
             */ 
            final int nbytes = r
                    .nextInt((am.byteCountBits >= 32 ? Integer.MAX_VALUE
                            : (int) am.maxByteCount));
            
            /*
             * Any long value, but when 0, negative, or too large then we will
             * check for correct rejection.
             */
            final long offset = r.nextLong();
            
            if (nbytes < 0 || offset > am.maxOffset || offset < 0L) {

                /*
                 * Check for correct rejection.
                 */
                try {

                    am.toAddr(nbytes, offset);
                    
                    fail("Expecting: " + IllegalArgumentException.class
                            + " (nbytes=" + nbytes + ", offset=" + offset + ")");
                    
                } catch(IllegalArgumentException ex) {
                    
                    // Ignoring expected exception.
                    
                }
                
            } else {
                
                long addr = am.toAddr(nbytes, offset);
                
                assertEquals("nbytes",nbytes,am.getByteCount(addr));

                assertEquals("offset",offset,am.getOffset(addr));
                
            }
            
        }
        
    }

    /**
     * Helper method verifies (de-)serialization of addresses.
     * 
     * @param offsetBits
     * 
     * @throws IOException
     */
    public void doTestPackUnpack(int offsetBits,int limit) throws IOException {

        WormAddressManager am = new WormAddressManager(offsetBits);
        
        /*
         * Generate an array of valid encoded addresses, including a bunch that
         * are NULLs.
         */
        long[] addrs = new long[limit];
        
        for(int i=0; i<limit; i++) {
            
            long addr;

            if (r.nextInt(100) < 5) {

                // 5% are NULLs.
                addr = 0L;

            } else {

                addr = nextAddr(r,am);
                
//                System.err.print(".");

            }

            addrs[i] = addr;
            
        }
        
//        System.err.println("Generated "+limit+" addresses.");
        
        /*
         * Pack the generated addresses.
         */
        final byte[] packed;
        {

            /*
             * The result should tend to be significantly smaller than directly
             * writing 8 bytes per address into the store, but of course that
             * depends on the actual addresses.
             */
            ByteArrayOutputStream baos = new ByteArrayOutputStream(limit*Bytes.SIZEOF_LONG);

            DataOutputStream os = new DataOutputStream(baos);

            for (int i = 0; i < limit; i++) {

                long addr = addrs[i];
                
                try {

                    am.packAddr(os, addr);
                    
                } catch(IllegalArgumentException ex) {
                    
                    fail("Could not pack addr="+addr+"("+am.toString(addr)+") : "+ex);
                    
                }

            }

            os.flush();

            packed = baos.toByteArray();
            
        }

        /*
         * @todo compute and show the compression ratio but note that it will
         * not be good when the addresses are choosen randomly.
         */
//        System.err.println("Compression ratio: "+(limit))
        
        /*
         * Verify that the addresses unpack correctly.
         */
        {
            
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(
                    packed));

            for (int i = 0; i < limit; i++) {

                long addr = am.unpackAddr(in);

                assertEquals(addrs[i],addr);
                
            }
            
        }

    }

    /**
     * Returns a legal random address and NULL 5% of the time.
     */
    static public long nextAddr(Random r,WormAddressManager am) {

        if(r.nextInt(100)<5) return 0L;
        
        return nextNonZeroAddr(r,am);
        
    }
    
    /**
     * Returns a legal random non-NULL address.
     */
    static public long nextNonZeroAddr(Random r,WormAddressManager am) {

        final int nbytes = nextNonZeroByteCount(r,am);
        
        final long offset = nextNonZeroOffset(r,am);

        assertEncodeDecode(am,nbytes,offset);

        final long addr = am.toAddr(nbytes, offset);
        
        return addr;
        
    }

    /**
     * Returns a legal random non-NULL address that does not extend as far as
     * <i>limit</i>.
     * 
     * @param limit
     *            The first byte that must not be covered by the returned
     *            address.
     * 
     * @todo this does not excercise the entire possible range addresses that
     *       would satisify the contract of this method.
     */
    static public long nextNonZeroAddr(Random r,WormAddressManager am, long limit) {

        final long offset = r.nextInt((int)Math.min(Integer.MAX_VALUE,limit));

        final int nbytes = r.nextInt((int) Math.min(am.getMaxByteCount(), Math
                .min(Integer.MAX_VALUE, limit - offset)));
        
        assert offset + nbytes < limit;
        
        assertEncodeDecode(am,nbytes,offset);

        final long addr = am.toAddr(nbytes, offset);
        
        return addr;
        
    }

    /**
     * Verify that we can encode and decode that address.
     * 
     * @param am
     *            The address manager.
     * @param nbytes
     *            The ground truth byte count.
     * @param offset
     *            The ground truth byte offset.
     */
    static void assertEncodeDecode(WormAddressManager am, int nbytes,
            long offset) {

        final long addr = am.toAddr(nbytes, offset);
        
        if (nbytes != am.getByteCount(addr)) {

            fail("offsetBits=" + am.offsetBits + ", addr=" + addr
                    + ", expected nbytes=" + nbytes + ", actual="
                    + am.getByteCount(addr) + ", offset=" + offset);

        }

        if (offset != am.getOffset(addr)) {

            fail("offsetBits=" + am.offsetBits + ", addr=" + addr
                    + ", expected offset=" + offset + ", actual="
                    + am.getOffset(addr) + ", nbytes=" + nbytes);

        }

    }
    
    /**
     * Next random byte count in [0:maxByteCount], but never more than
     * {@link Integer#MAX_VALUE} bytes and zero (0) 5% of the time.
     */
    static public int nextByteCount(Random r, WormAddressManager am) {

        if(r.nextInt(100)<5) {
            
            return 0;
            
        }

        final int nbytes = nextNonZeroByteCount(r, am);

        return nbytes;

    }

    /**
     * Next random legal non-zero byte count less than maxByteCount and never
     * more than {@link Integer#MAX_VALUE} bytes.
     */
    static public int nextNonZeroByteCount(Random r, WormAddressManager am) {

        int nbytes = 0;
        
        while(nbytes==0) {
            
            nbytes = r
                .nextInt((am.byteCountBits >= 32 ? Integer.MAX_VALUE
                        : (int) am.maxByteCount));
            
        }

        return nbytes;

    }

    /**
     * Next random byte offset and <code>0L</code> 5% of the time.
     */
    static public long nextOffset(Random r, WormAddressManager am) {

        if(r.nextInt()<5) return 0L;

        return nextNonZeroOffset(r, am);
        
    }
    
    /**
     * Next non-zero random byte offset less than maxOffset.
     */
    static public long nextNonZeroOffset(Random r, WormAddressManager am) {

        long offset = 0L;
        
        while (offset == 0L) {

            /*
             * start with any random long value and then mask off the bits that
             * are not allowed and shift down to to cover the zeros that leaves
             * in the lower order bits.
             */
            
            long offset1 = r.nextLong();

            long offset2 = offset1 & am.offsetMask;

            offset = offset2 >>> am.byteCountBits;

        }

        assert offset <= am.maxOffset;

        return offset;
        
    }
    
    /**
     * One of a series of tests of encoding and decoding addresses that have
     * proven themselves to be edge conditions in the code.
     */
    public void test_encodeDecode_offsetBits32_01() {
        
        WormAddressManager am = new WormAddressManager(32);
        
        final int nbytes = 1537183690;
        
        final long offset = 3154105902L;
  
        final long addr = am.toAddr(nbytes, offset);
        
        assertEquals("offset",offset,am.getOffset(addr));

        assertEquals("nbytes",nbytes,am.getByteCount(addr));
        
    }
    
    public void test_encodeDecode_offsetBits48_01() {
        
        WormAddressManager am = new WormAddressManager(48);
        
        final int nbytes = 55065;
        
        final long offset = 227799501816710L;
  
        final long addr = am.toAddr(nbytes, offset);
        
        assertEquals("offset",offset,am.getOffset(addr));

        assertEquals("nbytes",nbytes,am.getByteCount(addr));
        
    }

    public void test_static_getMaxByteCount() {
        
        /*
         * edge case - 32 unsigned bits can not be represented in a Java signed
         * integer so the max byte count is limited by the API to the maximum
         * value of a signed integer.
         */
        assertEquals("maxByteCount", Integer.MAX_VALUE, WormAddressManager
                .getMaxByteCount(32));
        
        /*
         * 31 unsigned bits can represent the Java signed int maximum value.
         */
        assertEquals("maxByteCount", Integer.MAX_VALUE, WormAddressManager
                .getMaxByteCount(33));

        /*
         * And check a few other data points.
         */

        assertEquals("maxByteCount",  4194304, WormAddressManager
                .getMaxByteCount(42));

        assertEquals("maxByteCount",  1024, WormAddressManager
                .getMaxByteCount(54));

    }

    /**
     * Test that {@link IAddressManager#toAddr(int, long)} will correctly reject
     * requests where the byte count is invalid.
     */
    public void test_toAddr_correctRejection_byteCount() {

        WormAddressManager am = new WormAddressManager(48);

        // correct rejection of a negative byte count.
        try {

            am.toAddr(-1,0L);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ingoring expected exception: "+ex);
            
        }

        /*
         * correct acceptance of the maximum byte count.
         */
        am.toAddr(am.getMaxByteCount(),0L);
        
        // correct rejection of the maximum byte count plus one.
        try {

            am.toAddr(am.getMaxByteCount()+1,0L);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ingoring expected exception: "+ex);
            
        }
        
    }
    
    /**
     * Test that {@link IAddressManager#toAddr(int, long)} will correctly reject
     * requests where the byte offset is invalid.
     */
    public void test_toAddr_correctRejection_byteOffset() {
        
        WormAddressManager am = new WormAddressManager(48);

        // correct rejection of a negative byte offset.
        try {

            am.toAddr(1,-1L);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ingoring expected exception: "+ex);
            
        }

        /*
         * correct acceptance of the maximum byte offset.
         */
        am.toAddr(1,am.getMaxOffset());
        
        // correct rejection of the maximum byte offset plus one.
        try {

            am.toAddr(1,am.getMaxOffset()+1);
            
            fail("Expecting: "+IllegalArgumentException.class);
            
        } catch(IllegalArgumentException ex) {
            
            System.err.println("Ingoring expected exception: "+ex);
            
        }

    }
    
}
