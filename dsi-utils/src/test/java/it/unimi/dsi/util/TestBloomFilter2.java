package it.unimi.dsi.util;

import it.unimi.dsi.util.BloomFilter2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Random;

import junit.framework.TestCase;

public class TestBloomFilter2 extends TestCase {
	
	public void testAdd() {
		BloomFilter2 bloomFilter = new BloomFilter2( 10, 30 ); // High precision 
		assertTrue( bloomFilter.add( "test" ) );
		assertFalse( bloomFilter.add( "test" ) );
		assertTrue( bloomFilter.add( "foo" ) );
		assertTrue( bloomFilter.add( "bar" ) );
		assertEquals( 3, bloomFilter.size() );
		
		bloomFilter.clear();
		assertTrue(bloomFilter.add(new int[] { 0, 1 }));
        assertFalse(bloomFilter.add(new int[] { 0, 1 }));
        assertTrue(bloomFilter.add(new int[] { 1, 2 }));
        assertTrue(bloomFilter.add(new int[] { 1, 0 }));
		assertEquals( 3, bloomFilter.size() );
	}

//    public void testAdd2() {
//
//        BloomFilter bloomFilter = new BloomFilter( 10, 30 ); // High precision 
//
//        // correct rejections (must succeed)
//        assertFalse("3",bloomFilter.contains(new int[]{3},0,1));
//        assertFalse("5",bloomFilter.contains(new int[]{5},0,1));
//        assertFalse("7",bloomFilter.contains(new int[]{7},0,1));
//        assertFalse("4",bloomFilter.contains(new int[]{4},0,1));
//        assertFalse("9",bloomFilter.contains(new int[]{9},0,1));
//
//        // add keys.
//        assertTrue("3",bloomFilter.add(new int[]{3},0,1));
//        assertTrue("5",bloomFilter.add(new int[]{5},0,1));
//        assertTrue("7",bloomFilter.add(new int[]{7},0,1));
//
//        // false positive tests (should succeed with resonable errorRate).
//        assertTrue("3",bloomFilter.contains(new int[]{3},0,1));
//        assertTrue("5",bloomFilter.contains(new int[]{5},0,1));
//        assertTrue("7",bloomFilter.contains(new int[]{7},0,1));
//
//        // false positive tests with non-zero offset (should succeed with resonable errorRate).
//        assertTrue("3",bloomFilter.contains(new int[]{0,3},1,1));
//        assertTrue("5",bloomFilter.contains(new int[]{0,5},1,1));
//        assertTrue("7",bloomFilter.contains(new int[]{0,7},1,1));
//
//        // correct rejections (must succeed)
//        assertFalse("4",bloomFilter.contains(new int[]{4},0,1));
//        assertFalse("9",bloomFilter.contains(new int[]{9},0,1));
//
//        // correct rejections with non-zero offset (must succeed)
//        assertFalse("4",bloomFilter.contains(new int[]{0,4},1,1));
//        assertFalse("9",bloomFilter.contains(new int[]{0,9},1,1));
//
//        // correct rejections with non-zero offset (must succeed)
//        assertFalse("4",bloomFilter.contains(new int[]{3,4},1,1));
//        assertFalse("9",bloomFilter.contains(new int[]{5,9},1,1));
//
//    }
    
	public void testConflicts() {
		BloomFilter2 bloomFilter = new BloomFilter2( 1000, 10 ); // Low precision
//		LongOpenHashSet longs = new LongOpenHashSet();
        HashSet<Long>longs = new HashSet<Long>();
		Random random = new Random( 0 );
		
		for( int i = 1000; i-- != 0; ) {
			final long l = random.nextLong();
			longs.add( l );
			bloomFilter.add( Long.toBinaryString( l ) );
		}
		
        /*
         * Note: I see bloomFile.size() coming in at 999 sometimes. This is a
         * minimum size so I expect that the error is allowable and that the
         * test could be better.
         */
		assertEquals( longs.size(), bloomFilter.size() );
	}

    public void testSerialization() throws IOException {

        BloomFilter2 bloomFilter = new BloomFilter2( 10, 30 ); // High precision 
        assertTrue( bloomFilter.add( new int[] { 0, 1 } ) );
        assertFalse( bloomFilter.add( new int[] { 0, 1 } ) );
        assertTrue( bloomFilter.add( new int[] { 1, 2 } ) );
        assertTrue( bloomFilter.add( new int[] { 1, 0 } ) );
        assertEquals( 3, bloomFilter.size() );
        assertTrue( bloomFilter.contains( new int[] { 0, 1 } ) );
        assertTrue( bloomFilter.contains( new int[] { 1, 2 } ) );
        assertTrue( bloomFilter.contains( new int[] { 1, 0 } ) );

        final byte[] buf = toByteArray(bloomFilter);
        
        bloomFilter = fromByteArray(buf);

        assertEquals( 3, bloomFilter.size() );

        assertTrue( bloomFilter.contains( new int[] { 0, 1 } ) );
        assertTrue( bloomFilter.contains( new int[] { 1, 2 } ) );
        assertTrue( bloomFilter.contains( new int[] { 1, 0 } ) );
        assertFalse( bloomFilter.contains( new int[]{ 2, 2 } ) );
        
    }

    public void testSerialization2() throws IOException {

        BloomFilter2 bloomFilter = new BloomFilter2( 10, 30 ); // High precision 
        assertTrue( bloomFilter.add( "test" ) );
        assertTrue( bloomFilter.add( "foo" ) );
        assertTrue( bloomFilter.add( "bar" ) );
        assertEquals( 3, bloomFilter.size() );

        final byte[] buf = toByteArray(bloomFilter);
        
        bloomFilter = fromByteArray(buf);

        assertEquals( 3, bloomFilter.size() );
        assertTrue( bloomFilter.contains( "test" ) );
        assertTrue( bloomFilter.contains( "foo" ) );
        assertTrue( bloomFilter.contains( "bar" ) );
        assertFalse( bloomFilter.contains( "baz" ) );

    }
    
    protected byte[] toByteArray(BloomFilter2 bloomFilter) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        
        oos.writeObject(bloomFilter);
        
        oos.flush();
        
        oos.close();
        
        byte[] bloomBytes = baos.toByteArray();
        
        return bloomBytes;

    }
    
    protected BloomFilter2 fromByteArray(byte[] buf) throws IOException {

        ByteArrayInputStream bais = new ByteArrayInputStream(buf);

        try {

            ObjectInputStream ois = new ObjectInputStream(bais);

            BloomFilter2 bloomFilter = (BloomFilter2) ois.readObject();

            return bloomFilter;

        }

        catch (Exception ex) {

            IOException ex2 = new IOException("Could not read bloom filter: "
                    + ex);

            ex2.initCause(ex);

            throw ex2;

        }
    }

}
