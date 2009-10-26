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

import com.bigdata.btree.raba.ConditionalRabaCoder;

/**
 * Test suite for the {@link ConditionalRabaCoder}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestConditionalRabaCoder_values_simple_canonical extends
        AbstractRabaCoderTestCase {

    /**
     * 
     */
    public TestConditionalRabaCoder_values_simple_canonical() {
    }

    /**
     * @param name
     */
    public TestConditionalRabaCoder_values_simple_canonical(String name) {
        super(name);
    }

    protected void setUp() throws Exception {

        super.setUp();

        rabaCoder = new ConditionalRabaCoder( //
                SimpleRabaCoder.INSTANCE,//
                CanonicalHuffmanRabaCoder.INSTANCE,
                32 // bigsize
        );

    }

}
