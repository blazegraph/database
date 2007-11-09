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
 * Created on Dec 15, 2006
 */

package com.bigdata.io;

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
