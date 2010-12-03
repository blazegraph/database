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
 * Created on Aug 5, 2009
 */

package com.bigdata.htree.data;

import com.bigdata.btree.data.AbstractLeafDataRecordTestCase;
import com.bigdata.btree.data.DefaultLeafCoder;
import com.bigdata.btree.data.ILeafData;
import com.bigdata.btree.raba.IRaba;
import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder;

/**
 * Test suite for the HTree {@link ILeafData} records (accessing coded data in
 * place).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLeafDataRecord_CanonicalHuffman_CanonicalHuffman extends AbstractLeafDataRecordTestCase {

    /**
     * 
     */
    public TestLeafDataRecord_CanonicalHuffman_CanonicalHuffman() {
    }

    /**
     * @param name
     */
    public TestLeafDataRecord_CanonicalHuffman_CanonicalHuffman(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        
        super.setUp();

        coder = new DefaultLeafCoder(//
                CanonicalHuffmanRabaCoder.INSTANCE,// keys
                CanonicalHuffmanRabaCoder.INSTANCE // vals
        );
        
    }

	protected ILeafData mockLeafFactory(final IRaba keys, final IRaba vals,
			final boolean[] deleteMarkers, final long[] versionTimestamps) {

		/*
		 * Note: This computes the MSB prefix and the hash codes using the
		 * standard Java semantics for the hash of a byte[]. In practice, the
		 * hash value is normally computed from the key using an application
		 * specified hash function.
		 */
		final int lengthMSB = 0;
		
		final int[] hashCodes = new int[keys.size()];
		
		for (int i = 0; i < hashCodes.length; i++) {
	
			hashCodes[i] = keys.get(i).hashCode();
			
		}

		return new MockBucketData(keys, vals, deleteMarkers, versionTimestamps,
				lengthMSB, hashCodes);

	}

}
