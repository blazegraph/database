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
 * Created on Sep 16, 2009
 */

package com.bigdata.cache;

import com.bigdata.LRUNexus.AccessPolicyEnum;
import com.bigdata.rawstore.Bytes;

/**
 * Some unit tests for the {@link BCHMGlobalLRU} usings its LRU access policy.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see TestBCHMGlobalLRUWithLIRS
 */
public class TestBCHMGlobalLRU extends
        AbstractHardReferenceGlobalLRUTest {

    /**
     * 
     */
    public TestBCHMGlobalLRU() {
    }

    /**
     * @param name
     */
    public TestBCHMGlobalLRU(String name) {
        super(name);
    }

    protected void setUp() throws Exception {

        super.setUp();

        final long maximumBytesInMemory = 10 * Bytes.kilobyte;

        final int minimumCacheSetCapacity = 0;

        final int limitingCacheCapacity = 100000;

        final float loadFactor = .75f;
        
        final int concurrencyLevel = 16;

        lru = new BCHMGlobalLRU<Object>(maximumBytesInMemory,
                minimumCacheSetCapacity, limitingCacheCapacity, loadFactor,
                concurrencyLevel,
                AccessPolicyEnum.LRU
                );

    }

}
