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
 * Created on Mar 21, 2011
 */

package com.bigdata.btree.keys;

import com.bigdata.io.SerializerUtil;

import junit.framework.TestCase2;

/**
 * Test suite for {@link ICUVersionRecord}
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestICUVersionRecord extends TestCase2 {

    /**
     * 
     */
    public TestICUVersionRecord() {
    }

    /**
     * @param name
     */
    public TestICUVersionRecord(String name) {
        super(name);
    }

    public void test_roundTrip() {

        final ICUVersionRecord r1 = ICUVersionRecord.newInstance();

        final ICUVersionRecord r2 = ICUVersionRecord.newInstance();
        
        assertTrue(r1.equals(r2));
        
        final ICUVersionRecord r3 = (ICUVersionRecord) SerializerUtil
                .deserialize(SerializerUtil.serialize(r1));

        assertTrue(r1.equals(r3));

    }
    
}
