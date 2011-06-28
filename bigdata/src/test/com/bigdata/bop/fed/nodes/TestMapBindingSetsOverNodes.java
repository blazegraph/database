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

package com.bigdata.bop.fed.nodes;

import com.bigdata.service.AbstractEmbeddedFederationTestCase;
import com.bigdata.service.DataService;

/**
 * Unit tests for mapping binding sets over nodes. For the purpose of this test
 * suite there is only a single node, but it runs with 2 {@link DataService}s
 * which is what we mean by "nodes" in this context. Unlike mapping binding sets
 * over shards, we can test this operation at very low data scales.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestMapBindingSetsOverNodes extends
        AbstractEmbeddedFederationTestCase {

    /**
     * 
     */
    public TestMapBindingSetsOverNodes() {
        super();
    }

    /**
     * @param name
     */
    public TestMapBindingSetsOverNodes(final String name) {
        super(name);
    }

    public void test_something() {
        
        fail("write tests");
        
    }
    
}
