package com.bigdata.btree;

import java.io.IOException;
import java.util.Random;

import com.bigdata.util.BytesUtil;

/**
 * Rather than run in a JUnit, the performance tests are best run
 * standalone.
 * 
 * @author Martyn Cutcher
 *
 */
public class TestGetBitsApplication {

    public static void test_perf_getBits7() throws IOException {
    	doTestBits32("test_perf_getBits7", 7);
    }
    public static void test_perf_getBits10() throws IOException {
    	doTestBits32("test_perf_getBits10", 10);
    }
    public static void test_perf_getBits21() throws IOException {
    	doTestBits32("test_perf_getBits21", 21);
    }
    public static void test_perf_getBits47() throws IOException {
    	doTestBits64("test_perf_getBits47", 47);
    }   
    
    static void doTestBits32(final String msg, final int bits) throws IOException {
        
        final Random r = new Random();

        // #of
        final int limit = 1000000000; // 100000000;

        // Note: length is guaranteed to be LT int32 bits so [int] index is Ok.
        final int len = 124500; // + r.nextInt(Bytes.kilobyte32 * 8) + 1;
        final int bitlen = len << 3;
        // Fill array with random data.
        final byte[] b = new byte[len];
        r.nextBytes(b);
        
        int[] sliceOffsets = new int[] {
        		1245,
        		12450,
        		102000,
        		80000,
        		120000
        };

        int runs = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < limit; i++) {

            // start of the bit slice.
            final int sliceBitOff = sliceOffsets[i%5]; // r.nextInt(bitlen - 64);

            final int bitsremaining = bitlen - sliceBitOff;

            // allow any slice of between 1 and 32 bits length.
            final int sliceBitLen = bits; // r.nextInt(Math.min(64, bitsremaining)) + 1;
            assert sliceBitLen >= 1 && sliceBitLen <= bits;

            BytesUtil.getBits(b, sliceBitOff, sliceBitLen);
            
            runs++;
        }
        System.out.println(msg + " completed: " + runs + ", in " + (System.currentTimeMillis()-start) + "ms");
    }
    
    public static void test_perf_tstGetBits7() throws IOException {
    	doAltTestBits32("test_perf_tstGetBits7", 7);
    }
    public static void test_perf_tstGetBits10() throws IOException {
    	doAltTestBits32("test_perf_tstGetBits10", 10);
    }
    public static void test_perf_tstGetBits21() throws IOException {
    	doAltTestBits32("test_perf_tstGetBits21", 21);
    }
    public static void test_perf_tstGetBits47() throws IOException {
    	doAltTestBits64("test_perf_tstGetBits47", 47);
    }
    static void doAltTestBits32(final String msg, final int bits) throws IOException {
        
        final Random r = new Random();

        // #of
        final int limit = 1000000000; // 100000000;

        // Note: length is guaranteed to be LT int32 bits so [int] index is Ok.
        final int len = 124500; // + r.nextInt(Bytes.kilobyte32 * 8) + 1;
        final int bitlen = len << 3;
        // Fill array with random data.
        final byte[] b = new byte[len];
        r.nextBytes(b);
        
        int[] sliceOffsets = new int[] {
        		1245,
        		12450,
        		102000,
        		80000,
        		120000
        };

        int runs = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < limit; i++) {

            // start of the bit slice.
            final int sliceBitOff = sliceOffsets[i%5]; // r.nextInt(bitlen - 64);

            final int bitsremaining = bitlen - sliceBitOff;

            // allow any slice of between 1 and 32 bits length.
            final int sliceBitLen = bits; // r.nextInt(Math.min(64, bitsremaining)) + 1;
            assert sliceBitLen >= 1 && sliceBitLen <= 32;

            BytesUtil.altGetBits32(b, sliceBitOff, sliceBitLen);
            
            runs++;
        }
        System.out.println(msg + " completed: " + runs + ", in " + (System.currentTimeMillis()-start) + "ms");
    }
    static void doAltTestBits64(final String msg, final int bits) throws IOException {
        
        final Random r = new Random();

        // #of
        final int limit = 500000000; // 100000000;

        // Note: length is guaranteed to be LT int32 bits so [int] index is Ok.
        final int len = 124500; // + r.nextInt(Bytes.kilobyte32 * 8) + 1;
        final int bitlen = len << 3;
        // Fill array with random data.
        final byte[] b = new byte[len];
        r.nextBytes(b);
        
        int[] sliceOffsets = new int[] {
        		1245,
        		12450,
        		102000,
        		80000,
        		120000
        };

        int runs = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < limit; i++) {

            // start of the bit slice.
            final int sliceBitOff = sliceOffsets[i%5]; // r.nextInt(bitlen - 64);

            final int bitsremaining = bitlen - sliceBitOff;

            // allow any slice of between 1 and 32 bits length.
            final int sliceBitLen = bits; // r.nextInt(Math.min(64, bitsremaining)) + 1;
            assert sliceBitLen >= 1 && sliceBitLen <= 32;

            BytesUtil.altGetBits64(b, sliceBitOff, sliceBitLen);
            
            runs++;
        }
        System.out.println(msg + " completed: " + runs + ", in " + (System.currentTimeMillis()-start) + "ms");
    }    
    
    static void doTestBits64(final String msg, final int bits) throws IOException {
        
        final Random r = new Random();

        // #of
        final int limit = 500000000; // 100000000;

        // Note: length is guaranteed to be LT int32 bits so [int] index is Ok.
        final int len = 124500; // + r.nextInt(Bytes.kilobyte32 * 8) + 1;
        final int bitlen = len << 3;
        // Fill array with random data.
        final byte[] b = new byte[len];
        r.nextBytes(b);
        
        int[] sliceOffsets = new int[] {
        		1245,
        		12450,
        		102000,
        		80000,
        		120000
        };

        int runs = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < limit; i++) {

            // start of the bit slice.
            final int sliceBitOff = sliceOffsets[i%5]; // r.nextInt(bitlen - 64);

            final int bitsremaining = bitlen - sliceBitOff;

            // allow any slice of between 1 and 32 bits length.
            final int sliceBitLen = bits; // r.nextInt(Math.min(64, bitsremaining)) + 1;
            assert sliceBitLen >= 1 && sliceBitLen <= 32;

            BytesUtil.getBits64(b, sliceBitOff, sliceBitLen);
            
            runs++;
        }
        System.out.println(msg + " completed: " + runs + ", in " + (System.currentTimeMillis()-start) + "ms");
    }

    public static void main(String[] args) throws IOException {
		test_perf_tstGetBits7();
		test_perf_getBits7();
		test_perf_tstGetBits10();
		test_perf_getBits10();
		test_perf_tstGetBits21();
		test_perf_getBits21();
		test_perf_tstGetBits47();
		test_perf_getBits47();
		
		System.out.println("All done!");
	}
}
