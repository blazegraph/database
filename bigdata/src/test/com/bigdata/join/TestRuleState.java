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
 * Created on Jun 20, 2008
 */

package com.bigdata.join;

import java.util.Comparator;

import com.bigdata.join.rdf.ISPO;

/**
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestRuleState extends AbstractRuleTestCase {

    /**
     * 
     */
    public TestRuleState() {

    }

    /**
     * @param name
     */
    public TestRuleState(String name) {

        super(name);
        
    }

    /**
     * FIXME {@link RuleState} has become an evaluation order and some access
     * path caching. In order to test this class we need to have either a mock
     * access path or some real data.
     */
    public void test_ruleState() {

        final IRelation<ISPO> relation = new MockRelation<ISPO>();
        
        final IRule r = new TestRuleRdfs9(relation);

        final RuleState state = new RuleState(r);

        fail("write test");

    }
    
    static class MockAccessPathFactory<E> implements IAccessPathFactory<E> {
        
        public IAccessPath<E> getAccessPath(IPredicate<E> predicate) {

            return new EmptyAccessPath<E>(predicate, new IKeyOrder<E>(){

                public Comparator<E> getComparator() {
                    
                    throw new UnsupportedOperationException();
                    
                }});
            
        }
        
    }

}
