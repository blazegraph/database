/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Aug 6, 2009
 */

package com.bigdata.btree.raba.codec;

import it.unimi.dsi.fastutil.bytes.CustomByteArrayFrontCodedList;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;


import com.bigdata.btree.raba.IRandomAccessByteArray;
import com.bigdata.btree.raba.ReadOnlyRaba;

/**
 * Test suite for the {@link FrontCodedDataCoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo unit tests where the successor in the byte[][] has fewer bytes in
 *       common when compared to its predecessor.
 * 
 * @todo performance tuning, including for the best ratio for an application.
 * 
 * @todo also test for probe keys that are not found.
 */
public class TestFrontCodedDataCoder extends AbstractDataCoderTestCase {

    /**
     * 
     */
    public TestFrontCodedDataCoder() {
    }

    /**
     * @param name
     */
    public TestFrontCodedDataCoder(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        
        // @todo test with different ratios.
        dataCoder = new FrontCodedDataCoder(8/* ratio */);
        
    }

    /**
     * Unit test demonstrates and verifies front coding of a well known example
     * with a ratio of 4.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_example_ratio4() throws UnsupportedEncodingException {
        
        final byte[][] a = new byte[4][];
        a[0] = "foo".getBytes("US-ASCII");
        a[1] = "foobar".getBytes("US-ASCII");
        a[2] = "fool".getBytes("US-ASCII");
        a[3] = "football".getBytes("US-ASCII");
        final IRandomAccessByteArray expected = new ReadOnlyRaba(a);
       
        // front-code the list.
        final int ratio = 4;
        final CustomByteArrayFrontCodedList frontCodedList = new CustomByteArrayFrontCodedList(
                expected.iterator(), ratio);
        
        {
            final byte[] t = frontCodedList.getBackingBuffer().toArray();

            System.out.println("coded: " + Arrays.toString(t));
        }

        for (int i = 0; i < a.length; i++) {

            assertEquals("get(" + i + ")", a[i], frontCodedList.get(i));

            assertEquals("length(" + i + ")", a[i].length, frontCodedList
                    .arrayLength(i));
            
        }

        for (int i = 0; i < a.length; i++) {

            assertEquals("search(" + i + ")", i, frontCodedList.search(a[i]));

        }
        
    }

    /**
     * Unit test demonstrates and verifies front coding of a well known example
     * with a ratio of 3.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_example_ratio3() throws UnsupportedEncodingException {
        
        final byte[][] a = new byte[4][];
        a[0] = "foo".getBytes("US-ASCII");
        a[1] = "foobar".getBytes("US-ASCII");
        a[2] = "fool".getBytes("US-ASCII");
        a[3] = "football".getBytes("US-ASCII");
        final IRandomAccessByteArray expected = new ReadOnlyRaba(a);
       
        // front-code the list.
        final int ratio = 3;
        final CustomByteArrayFrontCodedList frontCodedList = new CustomByteArrayFrontCodedList(
                expected.iterator(), ratio);
        
        {
            final byte[] t = frontCodedList.getBackingBuffer().toArray();

            System.out.println("coded: " + Arrays.toString(t));
        }

        for (int i = 0; i < a.length; i++) {

            assertEquals("get(" + i + ")", a[i], frontCodedList.get(i));

            assertEquals("length(" + i + ")", a[i].length, frontCodedList
                    .arrayLength(i));

        }

        assertEquals("search(" + 2 + ")", 2, frontCodedList.search(a[2]));

        for (int i = 0; i < a.length; i++) {

            assertEquals("search(" + i + ")", i, frontCodedList.search(a[i]));

        }

    }

    /**
     * Unit test demonstrates and verifies front coding of a well known example
     * using a ratio of 2.
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_example1_ratio2() throws UnsupportedEncodingException {
        
        final byte[][] a = new byte[4][];
        a[0] = "foo".getBytes("US-ASCII");
        a[1] = "foobar".getBytes("US-ASCII");
        a[2] = "fool".getBytes("US-ASCII");
        a[3] = "football".getBytes("US-ASCII");
        final IRandomAccessByteArray expected = new ReadOnlyRaba(a);
       
        // front-code the list.
        final int ratio = 2;
        final CustomByteArrayFrontCodedList frontCodedList = new CustomByteArrayFrontCodedList(
                expected.iterator(), ratio);
        
        {
            final byte[] t = frontCodedList.getBackingBuffer().toArray();

            System.out.println("coded: " + Arrays.toString(t));
        }

        for (int i = 0; i < a.length; i++) {

            assertEquals("get(" + i + ")", a[i], frontCodedList.get(i));

            assertEquals("length(" + i + ")", a[i].length, frontCodedList
                    .arrayLength(i));

        }
        
        for (int i = 0; i < a.length; i++) {

            assertEquals("search(" + i + ")", i, frontCodedList.search(a[i]));

        }

    }

    /**
     * Unit test demonstrates and verifies front coding of a well known example
     * using a ratio of 1 (no compression).
     * 
     * @throws UnsupportedEncodingException
     */
    public void test_example1_ratio1() throws UnsupportedEncodingException {
        
        final byte[][] a = new byte[4][];
        a[0] = "foo".getBytes("US-ASCII");
        a[1] = "foobar".getBytes("US-ASCII");
        a[2] = "fool".getBytes("US-ASCII");
        a[3] = "football".getBytes("US-ASCII");
        final IRandomAccessByteArray expected = new ReadOnlyRaba(a);
       
        // front-code the list.
        final int ratio = 1;
        final CustomByteArrayFrontCodedList frontCodedList = new CustomByteArrayFrontCodedList(
                expected.iterator(), ratio);
        
        {
            final byte[] t = frontCodedList.getBackingBuffer().toArray();

            System.out.println("coded: " + Arrays.toString(t));
        }

        for (int i = 0; i < a.length; i++) {

            assertEquals("get(" + i + ")", a[i], frontCodedList.get(i));

            assertEquals("length(" + i + ")", a[i].length, frontCodedList
                    .arrayLength(i));

        }
        
        for (int i = 0; i < a.length; i++) {

            assertEquals("search(" + i + ")", i, frontCodedList.search(a[i]));

        }

    }

}
