/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
 * Created on Aug 18, 2010
 */

package com.bigdata.bop.fed;

import com.bigdata.service.AbstractEmbeddedFederationTestCase;

/**
 * Unit tests for mapping binding sets over shards.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo While we can test against a single shard, this also needs to setup
 *       sufficient data volume (perhaps by controlling the nominal shard size
 *       on the disk) that it is actually running operators across more than one
 *       shard.
 */
public class TestMapBindingSetsOverShards extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestMapBindingSetsOverShards() {
        super();
    }

    /**
     * @param name
     */
    public TestMapBindingSetsOverShards(final String name) {
        super(name);
    }

    public void test_something() {
        
        fail("write tests");
        
    }
    
}
