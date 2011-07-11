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
 * Created on Jul 11, 2011
 */
package com.bigdata.htree;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import junit.framework.TestCase2;

import com.bigdata.btree.AbstractBTreeTestCase;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.data.ILeafData;

public class AbstractHTreeTestCase extends TestCase2 {

	public AbstractHTreeTestCase() {
	}

	public AbstractHTreeTestCase(String name) {
		super(name);
	}

    /*
     * TODO This might need to be modified to verify the sets of tuples in each
     * buddy bucket without reference to their ordering within the buddy bucket.
     * If so, then we will need to pass in the global depth of the bucket page
     * and clone and write new logic for the comparison of the leaf data state.
     */
    static void assertSameBucketData(ILeafData expected, ILeafData actual) {
        
        AbstractBTreeTestCase.assertSameLeafData(expected, actual);
        
    }
    
    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
    static public void assertSameIteratorAnyOrder(final byte[][] expected,
            final Iterator<byte[]> actual) {

        assertSameIteratorAnyOrder("", expected, actual);

    }

    /**
     * Verifies that the iterator visits the specified objects in some arbitrary
     * ordering and that the iterator is exhausted once all expected objects
     * have been visited. The implementation uses a selection without
     * replacement "pattern".
     */
	static public void assertSameIteratorAnyOrder(final String msg,
			final byte[][] expected, final Iterator<byte[]> actual) {

		// stuff everything into a list (allows duplicates).
		final List<byte[]> range = new LinkedList<byte[]>();
		for (byte[] b : expected)
			range.add(b);

		// Do selection without replacement for the objects visited by
		// iterator.
		for (int j = 0; j < expected.length; j++) {

			if (!actual.hasNext()) {

				fail(msg + ": Index exhausted while expecting more object(s)"
						+ ": index=" + j);

			}

			final byte[] actualValue = actual.next();

			boolean found = false;

			final Iterator<byte[]> titr = range.iterator();

			while (titr.hasNext()) {

				final byte[] b = titr.next();

				if (BytesUtil.bytesEqual(b, actualValue)) {
					found = true;
					titr.remove();
					break;
				}

			}

			if (!found) {

				fail("Value not expected" + ": index=" + j + ", object="
						+ actualValue);

			}

		}

		if (actual.hasNext()) {

			fail("Iterator will deliver too many objects.");

		}

	}

}
