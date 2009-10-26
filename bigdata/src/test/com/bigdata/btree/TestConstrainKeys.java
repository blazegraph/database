/*

Copyright (C) SYSTAP, LLC 2006-2009.  All rights reserved.

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
 * Created on Oct 15, 2009
 */

package com.bigdata.btree;

import junit.framework.TestCase2;

import com.bigdata.btree.proc.AbstractKeyRangeIndexProcedure;
import com.bigdata.mdi.ISeparatorKeys;

/**
 * Test imposing constraint on a fromKey or toKey based on an index partition's
 * boundaries.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConstrainKeys extends TestCase2 {

    /**
     * 
     */
    public TestConstrainKeys() {
    }

    /**
     * @param name
     */
    public TestConstrainKeys(String name) {
        super(name);
    }

    public void test_constrain_fromKey() {

        /*
         * The intersection of a null fromKey with any index partition is the
         * leftSeparator of that index partition.
         */
        assertEquals(new byte[] { 1 },// expected
                AbstractKeyRangeIndexProcedure.constrainFromKey(//
                        null,// key
                        new MockLocator(//
                                new byte[] { 1 }, // fromKey
                                null// toKey
                        )));

        /*
         * The intersection of a non-null fromKey with any index partition is
         * the greater of the fromKey and the leftSeparator of that index
         * partition.
         */
        assertEquals(new byte[] { 3 },// expected
                AbstractKeyRangeIndexProcedure.constrainFromKey(//
                        new byte[] { 1 },// key
                        new MockLocator(//
                                new byte[] { 3 }, // fromKey
                                null// toKey
                        )));
        assertEquals(new byte[] { 5 },// expected
                AbstractKeyRangeIndexProcedure.constrainFromKey(//
                        new byte[] { 5 },// key
                        new MockLocator(//
                                new byte[] { 3 }, // fromKey
                                null// toKey
                        )));

    }

    public void test_constrain_toKey() {

        /*
         * The intersection of a null toKey with any index partition is the
         * rightSeparator of that index partition (which may itself be null).
         */
        assertEquals(new byte[] { 2 },// expected
                AbstractKeyRangeIndexProcedure.constrainToKey(//
                        null,// key
                        new MockLocator(//
                                new byte[] { 1 }, // fromKey
                                new byte[] { 2 }// toKey
                        )));
        assertEquals(null,// expected
                AbstractKeyRangeIndexProcedure.constrainToKey(//
                        null,// key
                        new MockLocator(//
                                new byte[] { 1 }, // fromKey
                                null// toKey
                        )));

        /*
         * The intersection of a non-null toKey with any index partition whose
         * exclusive upper bound is null (no upper bound) is that toKey.
         */
        assertEquals(new byte[] { 5 },// expected
                AbstractKeyRangeIndexProcedure.constrainToKey(//
                        new byte[] { 5 },// key
                        new MockLocator(//
                                new byte[] { 3 }, // fromKey
                                null// toKey
                        )));

        /*
         * The intersection of a non-null toKey with any index partition whose
         * exclusive upper bound is defined is the lesser of the toKey and the
         * leftSeparator of that index partition.
         */
        assertEquals(new byte[] { 7 },// expected
                AbstractKeyRangeIndexProcedure.constrainToKey(//
                        new byte[] { 7 },// key
                        new MockLocator(//
                                new byte[] { 3 }, // fromKey
                                new byte[] { 12 }// toKey
                        )));
        assertEquals(new byte[] { 12 },// expected
                AbstractKeyRangeIndexProcedure.constrainToKey(//
                        new byte[] { 20 },// key
                        new MockLocator(//
                                new byte[] { 3 }, // fromKey
                                new byte[] { 12 }// toKey
                        )));

    }

    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     */
    private static class MockLocator implements ISeparatorKeys {

        private final byte[] fromKey;

        private final byte[] toKey;

        /**
         * 
         * @param fromKey
         *            The inclusive lower bound (must be non-null).
         * @param toKey
         *            The exclusive upper bound (may be null).
         */
        public MockLocator(final byte[] fromKey, final byte[] toKey) {

            if (fromKey == null)
                throw new IllegalArgumentException();

            if (toKey != null && BytesUtil.compareBytes(fromKey, toKey) >= 0) {
                throw new IllegalArgumentException();
            }

            this.fromKey = fromKey;

            this.toKey = toKey;

        }

        @Override
        public byte[] getLeftSeparatorKey() {

            return fromKey;
            
        }

        @Override
        public byte[] getRightSeparatorKey() {
            
            return toKey;
            
        }

    }

}
