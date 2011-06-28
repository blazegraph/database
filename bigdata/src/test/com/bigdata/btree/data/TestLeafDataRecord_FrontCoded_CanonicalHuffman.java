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

package com.bigdata.btree.data;

import com.bigdata.btree.raba.codec.CanonicalHuffmanRabaCoder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;

/**
 * Test suite for the B+Tree {@link ILeafData} records (accessing coded data in
 * place).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestLeafDataRecord_FrontCoded_CanonicalHuffman extends AbstractLeafDataRecordTestCase {

    /**
     * 
     */
    public TestLeafDataRecord_FrontCoded_CanonicalHuffman() {
    }

    /**
     * @param name
     */
    public TestLeafDataRecord_FrontCoded_CanonicalHuffman(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        
        super.setUp();

        coder = new DefaultLeafCoder(//
                new FrontCodedRabaCoder(8/* ratio */),// keys
                CanonicalHuffmanRabaCoder.INSTANCE // vals
        );

    }

}
