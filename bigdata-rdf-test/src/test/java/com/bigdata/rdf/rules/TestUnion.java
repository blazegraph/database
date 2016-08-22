/*

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Sep 26, 2008
 */

package com.bigdata.rdf.rules;

import com.bigdata.relation.rule.IProgram;
import com.bigdata.relation.rule.IRule;

/**
 * Unit test for processing of queries consisting of {@link IProgram}s
 * comprised of more than one {@link IRule}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class TestUnion extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestUnion() {
    }

    /**
     * @param name
     */
    public TestUnion(String name) {
        super(name);
    }

    /**
     * @todo write unit tests for "union".
     * 
     * @todo write test for union of two rules with parallel execution.
     * 
     * @todo write test for union of two rules with stable execution (serialize
     *       the program and serialize subquery in joins).
     * 
     * @todo write test for SLICE with UNION (requires stable execution of the
     *       union).
     * 
     * @todo write test for ORDER_BY with UNION (requires order imposed on the
     *       aggregate program results).
     * 
     * @todo write test for DISTINCT with UNION (requires order imposed on the
     *       aggregate program results).
     */
    public void test_union() {
        
        log.error("write test"); // FIXME Write tests.
        
    }
    
}
