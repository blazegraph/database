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

import com.bigdata.btree.IIndex;
import com.bigdata.join.rdf.AbstractTripleStore;
import com.bigdata.join.rdf.SPOJoinNexus;
import com.bigdata.join.rdf.SPOKeyOrder;
import com.bigdata.join.rdf.SPORelationLocator;


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

        final IRelationName relationName = new MockRelationName();
        
//        final IRelation<ISPO> relation = new SPORelation( );
        
        final IRule r = new TestRuleRdfs9(relationName);

        final RuleState state = new RuleState(r, new SPOJoinNexus(
                false/* elementOnly */, new SPORelationLocator(
                        new AbstractTripleStore() {

                            public IIndex getStatementIndex(
                                    SPOKeyOrder keyOrder) {
                                // TODO Auto-generated method stub
                                return null;
                            }

                        })));
        
        fail("write test");

    }
    
}
