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
 * Created on May 26, 2011
 */

package com.bigdata.io.compression;

import java.io.IOException;

import junit.framework.TestCase2;

import com.bigdata.io.ByteArrayBuffer;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;

/**
 * Test suite for {@link UnicodeHelper}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestUnicodeHelper extends TestCase2 {

    /**
     * 
     */
    public TestUnicodeHelper() {
    }

    /**
     * @param name
     */
    public TestUnicodeHelper(String name) {
        super(name);
    }

    public void test_ctor() {
        try {
            new UnicodeHelper(null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }
    }

    /*
     * Note: BOCU and SCSU charsets cause problems in CI unless we fork a JVM
     * to run the tests.  Since they are also slow and not currently in use I
     * have modified the tests such that they are not being materialized.
     */
    protected IUnicodeCompressor getUnicodeCompressor() {

    	return new NoCompressor();

    }
    
    public void test_encodeDecode1() throws IOException {

        doEncodeDecodeTest(new UnicodeHelper(getUnicodeCompressor()), "bigdata");

    }

    public void test_encodeDecode1_emptyString() throws IOException {

        doEncodeDecodeTest(new UnicodeHelper(getUnicodeCompressor()), "");

    }

    public void test_encodeDecode1_veryLargeString() throws IOException {

        final int len = 1024000;

        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {

            sb.append(Character.toChars('A' + (i % 26)));

        }

        final String s = sb.toString();

        if (log.isInfoEnabled())
            log.info("length(s)=" + s.length());
        
        doEncodeDecodeTest(new UnicodeHelper(getUnicodeCompressor()), s);

    }
    
    /**
     * Test helper for encode and decode of a single {@link String}.
     */
    private void doEncodeDecodeTest(final UnicodeHelper un,
            final String expected) throws IOException {

        // buffer for encode result.
        final DataOutputBuffer out = new DataOutputBuffer();

        // tmp buffer for encode.
        final ByteArrayBuffer tmp = new ByteArrayBuffer();

        // encode
        final int nencoded = un.encode(expected, out, tmp);

        // extract encoded byte[].
        final byte[] data = out.toByteArray();

        // verify encode1() gives same result.
        final byte[] data1 = un.encode1(expected);
        assertEquals(data, data1);
        
        // buffer for decode result
        final StringBuilder sb = new StringBuilder();
        final int ndecoded = un.decode(new DataInputBuffer(data), sb);
        
        // verify the decode result.
        assertEquals(expected, sb.toString());
        
        // verify the #of decoded bytes.
        assertEquals(nencoded,ndecoded);

        // reset the decode buffer.
        sb.setLength(0);
        // decode string
        assertEquals(expected, un.decode1(new DataInputBuffer(data), sb));

    }

    public void test_encodeDecode2() throws IOException {
        
        doEncodeDecodeTest(new UnicodeHelper(getUnicodeCompressor()),
                new String[] { //
            "en", "bigdata" //
            });

    }

    private void doEncodeDecodeTest(final UnicodeHelper un,
            final String expected[]) throws IOException {

        // buffer for encode result.
        final DataOutputBuffer out = new DataOutputBuffer();

        // tmp buffer for encode.
        final ByteArrayBuffer tmp = new ByteArrayBuffer();

        // the expected data in a single buffer.
        final StringBuilder exp = new StringBuilder();
        int nencoded = 0;
        for (String s : expected) {
            // encode
            nencoded += un.encode(s, out, tmp);
            // concatenate
            exp.append(s);
        }

        // extract encoded byte[].
        final byte[] data = out.toByteArray();

        // buffer for decode result
        final StringBuilder sb = new StringBuilder();

        // decode each string component.
        final DataInputBuffer in = new DataInputBuffer(data);
        int ndecoded = 0;
        for (int i = 0; i < expected.length; i++) {

            ndecoded += un.decode(in, sb);

        }
        
        // verify the decode.
        assertEquals(exp.toString(), sb.toString());

        // verify the #of decoded bytes.
        assertEquals(nencoded,ndecoded);

    }

}
