/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 9, 2011
 */

package com.bigdata.util.httpd;

import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import junit.framework.TestCase2;

/**
 * Test suite for {@link NanoHTTPD#decodeParams(String, LinkedHashMap)}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDecodeParams extends TestCase2 {

    /**
     * 
     */
    public TestDecodeParams() {
    }

    /**
     * @param name
     */
    public TestDecodeParams(String name) {
        super(name);
    }
    
    private void assertSameParams(
            final LinkedHashMap<String, Vector<String>> expected,
            final LinkedHashMap<String, Vector<String>> actual) {

        assertEquals("size", expected.size(), actual.size());

        for (Map.Entry<String, Vector<String>> e : expected.entrySet()) {

            final String k = e.getKey();

            final Vector<String> ev = e.getValue();

            final Vector<String> av = actual.get(k);

            assertEquals("sizeOf(" + k + ")", ev.size(), av.size());

            for (int i = 0; i < ev.size(); i++) {

                assertEquals(k + "[" + i + "]", ev.get(i), av.get(i));

            }

        }

    }

    /**
     * Unit test for {@link NanoHTTPD#decodeParams(String, LinkedHashMap)}.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_decodeParams() throws UnsupportedEncodingException {

        final LinkedHashMap<String, Vector<String>> expected = new LinkedHashMap<String, Vector<String>>();
        {
            {
                final Vector<String> v = new Vector<String>();
                v.add("1");
                v.add("2");
                v.add("3");
                expected.put("a", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                v.add("a b c");
                v.add("d e");
                v.add("f");
                expected.put("blue", v);
            }
        }

        final String uri = NanoHTTPD.encodeParams(expected).toString();

        if (log.isInfoEnabled())
            log.info("encoded=" + uri);

        final LinkedHashMap<String, Vector<String>> actual = new LinkedHashMap<String, Vector<String>>();

        NanoHTTPD.decodeParams(uri, actual);

        assertSameParams(expected, actual);

    }

    public void test_paramHasEmptyStringValue() throws UnsupportedEncodingException {
        
        final LinkedHashMap<String, Vector<String>> expected = new LinkedHashMap<String, Vector<String>>();
        {
            {
                final Vector<String> v = new Vector<String>();
                v.add("1"); // non-empty value.
                expected.put("a", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                v.add("");// empty value.
                expected.put("b", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                // no value.
                expected.put("c", v);
            }
        }

        final String uri = "a=1&b=&c";
        
        final LinkedHashMap<String, Vector<String>> actual = new LinkedHashMap<String, Vector<String>>();

        NanoHTTPD.decodeParams(uri, actual);

        assertSameParams(expected,actual);
        
    }
    
    /**
     * Examines behavior with leading/trailing whitespace.
     * 
     * @throws UnsupportedEncodingException 
     */
    public void test_decodeParams_with_whitespace() throws UnsupportedEncodingException {

        final LinkedHashMap<String, Vector<String>> expected = new LinkedHashMap<String, Vector<String>>();
        {
            {
                final Vector<String> v = new Vector<String>();
                v.add(" 1 ");
                expected.put("a", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                v.add(" 2");
                expected.put("b", v);
            }
            {
                final Vector<String> v = new Vector<String>();
                v.add(" ");
                expected.put("c", v);
            }
        }

        final String uri = "a= 1 &b= 2&c= ";

        final LinkedHashMap<String, Vector<String>> actual = new LinkedHashMap<String, Vector<String>>();

        NanoHTTPD.decodeParams(uri, actual);

        assertSameParams(expected, actual);

    }

}
