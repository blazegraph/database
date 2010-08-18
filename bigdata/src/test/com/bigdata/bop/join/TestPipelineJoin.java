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

package com.bigdata.bop.join;

import com.bigdata.bop.ChunkedOrderedIteratorOp;
import com.bigdata.bop.ap.Predicate;

import junit.framework.TestCase2;

/**
 * Unit tests for the {@link PipelineJoin} operator.
 * <p>
 * Note: The operators to map binding sets over shards are tested independently.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Write unit tests for simple joins. This can be done without using an
 *       {@link IPredicate} since the right operand is a
 *       {@link ChunkedOrderedIteratorOp}.
 * 
 * @todo Write unit tests for joins where the right operand is optional.
 * 
 * @todo Write unit tests for correct reporting of join statistics.
 * 
 * @todo Write unit tests for star-joins (in their own test suite).
 */
public class TestPipelineJoin extends TestCase2 {

    /**
     * 
     */
    public TestPipelineJoin() {
    }

    /**
     * @param name
     */
    public TestPipelineJoin(String name) {
        super(name);
    }

    /**
     * 
     */
    public void test_pipelineJoin() {
        
        fail("write tests");
        
    }
    
}
