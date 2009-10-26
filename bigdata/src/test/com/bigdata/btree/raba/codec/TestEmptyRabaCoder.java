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
 * Created on Sep 3, 2009
 */

package com.bigdata.btree.raba.codec;


import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.raba.EmptyRaba;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.ReadOnlyValuesRaba;
import com.bigdata.io.DataOutputBuffer;


/**
 * Unit tests for the {@link EmptyRabaValueCoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestEmptyRabaCoder extends TestCase2 {

    /**
     * 
     */
    public TestEmptyRabaCoder() {
    }

    /**
     * @param name
     */
    public TestEmptyRabaCoder(String name) {
        super(name);
    }

    /**
     * Verify will not code keys. 
     */
    public void test_emptyRabaCoder_keysNotAllowed() {

        final IRabaCoder rabaCoder = EmptyRabaValueCoder.INSTANCE;

        // not permitted for keys since keys are not optional.
        assertFalse(rabaCoder.isKeyCoder());

        // does support values.
        assertTrue(rabaCoder.isValueCoder());

        // verify will not accept keys.
        try {
            assertTrue(EmptyRaba.KEYS.isKeys());
            rabaCoder.encode(EmptyRaba.KEYS, new DataOutputBuffer());
            fail("Expecting: "+UnsupportedOperationException.class);
        } catch(UnsupportedOperationException ex) {
            if(log.isInfoEnabled())
                log.info("Ignoring expected exception: "+ex);
        }
        
    }
    
    /**
     * Unit test with an empty byte[][].
     */
    public void test_emptyRabaCoder() {
        
        final IRabaCoder rabaCoder = EmptyRabaValueCoder.INSTANCE;

        final IRaba expected = new ReadOnlyValuesRaba(0/* size */,
                new byte[2][]);

        // empty raba.
        AbstractRabaCoderTestCase.doRoundTripTest(rabaCoder, expected);

    }

    /**
     * Verify discards data with the {@link IRaba} is non-empty but all
     * <code>null</code>s.
     */
    public void test_emptyRabaCoder_nonZeroSize() {

        final IRabaCoder rabaCoder = EmptyRabaValueCoder.INSTANCE;

        final IRaba expected = new ReadOnlyValuesRaba(2/* size */,
                new byte[2][]);

        final IRaba actual = rabaCoder.decode(rabaCoder.encode(expected,
                new DataOutputBuffer()));

        AbstractBTreeTestCase.assertSameRaba(expected, actual);

    }

    /**
     * Verify discards data with the {@link IRaba} is non-empty but all
     * non-<code>null</code>s.
     */
    public void test_emptyRabaCoder_discardsData() {
        
        final IRabaCoder rabaCoder = EmptyRabaValueCoder.INSTANCE;

        final IRaba expected = new ReadOnlyValuesRaba(2/* size */,
                new byte[2][]);

        final byte[][] a = new byte[2][];
        a[0] = new byte[] { 1, 2, 3 };
        a[1] = new byte[] { 2 };

        final IRaba actual = rabaCoder.decode(rabaCoder.encode(expected,
                new DataOutputBuffer()));

        AbstractBTreeTestCase.assertSameRaba(expected, actual);

    }

}
