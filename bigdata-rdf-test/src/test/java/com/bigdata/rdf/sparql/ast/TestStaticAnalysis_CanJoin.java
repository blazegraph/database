/**

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
 * Created on Oct 20, 2011
 */

package com.bigdata.rdf.sparql.ast;

import java.util.Collections;

import junit.framework.TestCase2;

import com.bigdata.bop.Constant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.VTE;
import com.bigdata.rdf.internal.impl.TermId;

/**
 * @see StaticAnalysis#canJoin(IJoinNode, IJoinNode)
 */
public class TestStaticAnalysis_CanJoin extends TestCase2 {

    /**
     * 
     */
    public TestStaticAnalysis_CanJoin() {
    }

    /**
     * @param name
     */
    public TestStaticAnalysis_CanJoin(final String name) {
        super(name);
    }

    /**
     * Return a (Mock) IV.
     */
    @SuppressWarnings("rawtypes")
    protected IV mockIV() {
        return TermId.mockIV(VTE.URI);
    }

    /**
     * Correct rejection tests.
     * 
     * @see StaticAnalysis#canJoin(IBindingProducerNode, IBindingProducerNode)
     */
    public void test_canJoin_correctRejection() {
        
        @SuppressWarnings("rawtypes")
        final ConstantNode p = new ConstantNode(new Constant<IV>(mockIV()));
        @SuppressWarnings("rawtypes")
        final ConstantNode q = new ConstantNode(new Constant<IV>(mockIV()));
        @SuppressWarnings("rawtypes")
        final ConstantNode r = new ConstantNode(new Constant<IV>(mockIV()));
        
        final VarNode x = new VarNode("x");
        final VarNode y = new VarNode("y");
        final VarNode z = new VarNode("z");
        
        final StatementPatternNode p1 = new StatementPatternNode(p, y, z);
        final StatementPatternNode p2 = new StatementPatternNode(x, q, r);

        final StaticAnalysis sa = new StaticAnalysis(new QueryRoot(QueryType.SELECT));

        // correct rejection w/ null arg.
        try {
            sa.canJoin(null, p2);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

        // correct rejection w/ null arg.
        try {
            sa.canJoin(p1, null);
            fail("Expecting: " + IllegalArgumentException.class);
        } catch (IllegalArgumentException ex) {
            if (log.isInfoEnabled())
                log.info("Ignoring expected exception: " + ex);
        }

    }

    /**
     * Semantics tests focused on shared variables in the operands.
     * 
     * @see StaticAnalysis#canJoin(IBindingProducerNode, IBindingProducerNode)
     */
    public void test_canJoin() {
        
        @SuppressWarnings("rawtypes")
        final ConstantNode p = new ConstantNode(new Constant<IV>(mockIV()));
        @SuppressWarnings("rawtypes")
        final ConstantNode q = new ConstantNode(new Constant<IV>(mockIV()));
        @SuppressWarnings("rawtypes")
        final ConstantNode r = new ConstantNode(new Constant<IV>(mockIV()));
        
        final VarNode u = new VarNode("u");
        final VarNode x = new VarNode("x");
        final VarNode y = new VarNode("y");
        final VarNode z = new VarNode("z");
        
        // p1 and p2 can join on y.
        // p2 and p3 can join on z.
        final StatementPatternNode p1 = new StatementPatternNode(x, y, p);
        final StatementPatternNode p2 = new StatementPatternNode(y, z, q);
        final StatementPatternNode p3 = new StatementPatternNode(u, z, r);

        final StaticAnalysis sa = new StaticAnalysis(new QueryRoot(QueryType.SELECT));

        // share y
        assertTrue(sa.canJoin(p1, p2));
        
        // share z
        assertTrue(sa.canJoin(p2, p3));
        
        // share z
        assertFalse(sa.canJoin(p1, p3));

        // shares (x,y) with self.
        assertTrue(sa.canJoin(p1, p1));

    }

//    /**
//     * TODO Write unit tests for canJoin of {@link IBindingProducerNode}s which
//     * are not just {@link StatementPatternNode}s, including {@link ServiceNode}
//     * , {@link SubqueryRoot}, etc.
//     */
//    public void test_canJoin_notJustStatementPatterns() {
//        fail("write test");
//    }
    
    /**
     * Verify that joins are not permitted when the variables are
     * only shared via a FILTER.
     * 
     * @see StaticAnalysis#canJoin(IBindingProducerNode, IBindingProducerNode)
     */
    public void test_canJoin_filtersAreIngored() {
        
        @SuppressWarnings("rawtypes")
        final ConstantNode p = new ConstantNode(new Constant<IV>(mockIV()));
        @SuppressWarnings("rawtypes")
        final ConstantNode q = new ConstantNode(new Constant<IV>(mockIV()));
        @SuppressWarnings("rawtypes")
        final ConstantNode r = new ConstantNode(new Constant<IV>(mockIV()));
        
        final VarNode x = new VarNode("x");
        final VarNode y = new VarNode("y");
        final VarNode z = new VarNode("z");

        /*
         * Note: p2 shares a variable via the FILTER.
         * 
         * TODO Make sure that we test the simple optional case for
         * canJoinWithFilters().
         */
        final StatementPatternNode p1 = new StatementPatternNode(p, q, x);
        final StatementPatternNode p2 = new StatementPatternNode(y, r, z);
        p2.setOptional(true);
        p2.setAttachedJoinFilters(Collections.singletonList(new FilterNode(x)));

        final StaticAnalysis sa = new StaticAnalysis(new QueryRoot(QueryType.SELECT));

        // verify that the variables in the annotations are ignored.
        assertFalse(sa.canJoin(p1, p2));

    }

}
