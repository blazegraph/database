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
 * Created on Dec 15, 2006
 */

package com.bigdata.objndx;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import junit.framework.TestCase2;

/**
 * Test suite for classes that let us treat a {@link ByteBuffer} as an
 * {@link InputStream} or an {@link OutputStream}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestByteBufferStreams extends TestCase2 {

    private Random r = new Random();
    
    /**
     * 
     */
    public TestByteBufferStreams() {
    }

    /**
     * @param arg0
     */
    public TestByteBufferStreams(String arg0) {
        super(arg0);
    }

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
    
}
