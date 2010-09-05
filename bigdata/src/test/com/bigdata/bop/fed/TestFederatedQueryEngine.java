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
 * Created on Sep 5, 2010
 */

package com.bigdata.bop.fed;

import com.bigdata.bop.engine.FederatedQueryEngine;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.service.AbstractEmbeddedFederationTestCase;

/**
 * Unit tests for {@link FederatedQueryEngine}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Write unit tests for distributed pipeline query with an eye towards
 *       verification of integration of the {@link QueryEngine} with the
 *       protocol used to move data around among the nodes.
 *       <p>
 *       Each of the operators which has specialized execution against a
 *       federation should be tested as well. This includes:
 *       <p>
 *       distributed merge sort (assuming that there is a requirement to
 *       interchange buffers as part of the distributed sort).
 *       <p>
 *       distributed hash tables used in a distributed distinct filter on an
 *       access path (for scale-out default graph queries in quads mode).
 *       <p>
 *       ...
 */
public class TestFederatedQueryEngine extends AbstractEmbeddedFederationTestCase {

    public TestFederatedQueryEngine() {
        
    }
    
    public TestFederatedQueryEngine(String name) {
        
        super(name);
        
    }

    public void test_something() {
        
        fail("write tests");
        
    }
    
}
