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
 * Created on Nov 20, 2006
 */

package com.bigdata.objectIndex;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.bigdata.journal.IRawStore;
import com.bigdata.journal.SlotMath;
import com.bigdata.objectIndex.IndexEntrySerializer.ByteBufferInputStream;
import com.bigdata.objectIndex.IndexEntrySerializer.ByteBufferOutputStream;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestIndexEntrySerializer extends AbstractObjectIndexTestCase {

    /**
     * 
     */
    public TestIndexEntrySerializer() {
    }

    /**
     * @param name
     */
    public TestIndexEntrySerializer(String name) {
        super(name);
    }

    IRawStore store = new SimpleStore();
    
    IndexEntrySerializer valueSer = new IndexEntrySerializer(store.getSlotMath());
    
//    public void test_byteToInt() {
//        
//        assertEquals((byte)0xff,(byte)-1);
//        
//    }
    
    public void testByteBufferStreams00() throws IOException {
        
        doRoundTripTest( (byte)0 );
        doRoundTripTest( (byte)127 );
        doRoundTripTest( (byte)128 );
        doRoundTripTest( (byte)129 );
        doRoundTripTest( (byte)255 );
        doRoundTripTest( (byte)256 );
        
    }

    public void doRoundTripTest(byte expected) throws IOException {

        final ByteBuffer buf = ByteBuffer.allocate(1);
        
        {
            
            ByteBufferOutputStream os = new ByteBufferOutputStream(buf);

            os.write(expected);

        }
        
        buf.flip();
        
        {
            
            ByteBufferInputStream is = new ByteBufferInputStream(buf);
            
            int b = is.read();
            
            if( b == -1 ) fail("EOF");
            
            byte actual = (byte)b;
            
            assertEquals( expected, actual );
            
        }

    }
    
//    /**
//     * Test the utility classes for reading and writing on a {@link ByteBuffer}
//     * as if it were a stream.
//     */
//    public void testByteBufferStreams01() throws IOException {
//        
//        ByteBuffer buf = ByteBuffer.allocate(256);
//
//        {
//            ByteBufferOutputStream os = new ByteBufferOutputStream(buf);
//
//            int b = Byte.MIN_VALUE;
//            
//            for (int i = 0; i < 256; i++) {
//
//                os.write(b);
//                
//                b++;
//
//            }
//            
//        }
//
//        buf.flip();
//
//        {
//        
//            ByteBufferInputStream is = new ByteBufferInputStream(buf);
//            
//            for (int i = 0; i < 256; i++) {
//
//                assertEquals(i, is.read());
//
//            }
//
//        }
//
//    }
    
    public void testByteBufferStreams02() throws IOException {
        
        byte[] expected = new byte[1024*8];
        
        r.nextBytes(expected);
        
        final ByteBuffer buf = ByteBuffer.allocate(expected.length);
        
        {
            
            DataOutputStream dos = new DataOutputStream(
                    new ByteBufferOutputStream(buf));

            dos.write(expected);

            dos.flush();

            dos.close();

        }
        
        buf.flip();
        
        {
            
            DataInputStream dis = new DataInputStream( new ByteBufferInputStream(buf));
            
            byte[] actual = new byte[expected.length];
            
            dis.read(actual);
            
            assertEquals(expected,actual);
            
        }
        
    }
    
    public void testByteBufferStreams03() throws IOException {
        
        byte[] expected = new byte[256];

        int b = Byte.MIN_VALUE;
        
        for (int i = 0; i < 256; i++) {

            expected[i] = (byte) b++;
//            expected[i] = (byte)i;

        }
        
        final ByteBuffer buf = ByteBuffer.allocate(expected.length);
        
        {
            
            DataOutputStream dos = new DataOutputStream(
                    new ByteBufferOutputStream(buf));

            dos.write(expected);

            dos.flush();

            dos.close();

        }
        
        buf.flip();
        
        {
            
            DataInputStream dis = new DataInputStream( new ByteBufferInputStream(buf));
            
            byte[] actual = new byte[expected.length];
            
            dis.read(actual);
            
            assertEquals(expected,actual);
            
        }
        
    }

    public void testByteBufferStreams04() throws IOException {
        
        for (int i = 0; i < 1000; i++) {

            final String expected = getRandomString(256, 0);

            // Note: presuming that this is sufficient capacity.
            final int capacity = expected.length() * 3;

            final ByteBuffer buf = ByteBuffer.allocate(capacity);

            {

                DataOutputStream dos = new DataOutputStream(
                        new ByteBufferOutputStream(buf));

                dos.writeUTF(expected);

                dos.flush();

                dos.close();

            }

            buf.flip();

            {

                DataInputStream dis = new DataInputStream(
                        new ByteBufferInputStream(buf));

                String actual = dis.readUTF();

                assertEquals(expected, actual);

            }

        }
        
    }
    
    /**
     * Test with entry whose fields are all zeros.
     */
    public void test01() {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)0,0L,0L);
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values.
     */
    public void test02() {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)1,SlotMath.toLong(12, 90),0L);
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values.
     */
    public void test03() {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)2,0L,SlotMath.toLong(22, 80));
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values.
     */
    public void test04() {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)3,SlotMath.toLong(32, 92),SlotMath.toLong(2, 10));
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Test with entry having known field values corresponding to an observed
     * test failure.
     */
    public void test05() {
        
        IndexEntry e1 = new IndexEntry(store.getSlotMath(),(short)28942,6851360340572110870L,2927585090617737519L);
        
        ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(1));
        
        doRoundTripTest(buf, new IndexEntry[]{e1}, 1);
        
    }
    
    /**
     * Stress test with entry whose fields are random.
     * 
     * @todo modify test to track the #of bytes written and compute the space
     * savings over the maximum.  Note that this is not really representative
     * since the data are random for the test but significantly non-random for
     * a real object index.
     */
    public void testStress() {
        
//        final int LIMIT = 1;
//
//        final int maxBranchingFactor = 4;
        
        final int LIMIT = 1000;

        final int maxBranchingFactor = 1024;

        final SlotMath slotMath = store.getSlotMath();
        
        for( int i=0; i<LIMIT; i++ ) {

            final int branchingFactor = r.nextInt(maxBranchingFactor
                    - BTree.MIN_BRANCHING_FACTOR)
                    + BTree.MIN_BRANCHING_FACTOR;
        
            final int nkeys = r.nextInt(branchingFactor);
            
            ByteBuffer buf = ByteBuffer.allocate(valueSer.getSize(nkeys));

            IndexEntry expected[] = new IndexEntry[branchingFactor];
            
            for( int j=0; j<nkeys; j++ ) {

                expected[j] = getRandomEntry(slotMath);
            
            }

            doRoundTripTest(buf, expected, nkeys);
        
        }
        
    }
    
    public void doRoundTripTest( ByteBuffer buf, IndexEntry[] expected, int n ) {
        
        // clear before writing.
        buf.clear();
        
        valueSer.putValues(buf, expected, n);
        
        IndexEntry[] actual = new IndexEntry[expected.length];
        
        // flip for reading.
        buf.flip();
        
        try {

            valueSer.getValues(buf, actual, n);
            
        } catch(BufferUnderflowException ex) {
            
            for( int i=0; i<n; i++ ) {
                
                System.err.println("expected[i]="+expected[i]);
                
            }

            throw ex;

        }
        
        for( int i=0; i<n; i++ ) {
            
            assertEquals("values["+i+"]", expected[i], actual[i]);
            
        }
        
    }
    
}
