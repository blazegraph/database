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
 * Created on Sep 26, 2008
 */

package com.bigdata.rdf.rules;

import com.bigdata.relation.rule.IQueryOptions;

/**
 * Unit tests for {@link IQueryOptions#isDistinct()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestDistinct extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestDistinct() {

    }

    /**
     * @param name
     */
    public TestDistinct(String name) {

        super(name);
     
    }

    /**
     * @todo Unit test where there are no results.
     * 
     * @todo Unit test where duplicates are correctly dropped.
     */
    public void test_distinct() {
        
        fail("write tests");
        
    }
    
}
