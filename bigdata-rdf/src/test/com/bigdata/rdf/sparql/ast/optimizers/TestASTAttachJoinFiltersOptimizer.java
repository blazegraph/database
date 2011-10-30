/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Aug 29, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Collections;

import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.AbstractASTEvaluationTestCase;
import com.bigdata.rdf.sparql.ast.BSBMQ5Setup;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;

/**
 * Test suite for {@link ASTAttachJoinFiltersOptimizer}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestASTAttachJoinFiltersOptimizer extends AbstractASTEvaluationTestCase {

    /**
     * 
     */
    public TestASTAttachJoinFiltersOptimizer() {
    }

    /**
     * @param name
     */
    public TestASTAttachJoinFiltersOptimizer(String name) {
        super(name);
    }

    /**
     * Unit test for the attachment of the join filters to the required joins in
     * a {@link JoinGroupNode}.
     * <p>
     * Note: The core logic for deciding join filter attachment is tested
     * elsewhere. This is only testing the attachment once those decisions are
     * made.
     */
    @SuppressWarnings("unchecked")
    public void test_attachFilters() {

      /*
      * Note: DO NOT share structures in this test!!!!
      */
     final IBindingSet[] bsets = new IBindingSet[]{};

        // The source AST.
        final QueryRoot given;
        {

            final BSBMQ5Setup s = new BSBMQ5Setup(store);
            
            given = s.queryRoot;

            /**
             * Put the joins into a known order whose correct filter attachments
             * are also known. This order is <code>[5, 3, 1, 0, 2, 4, 6]</code>.
             */
            final GraphPatternGroup<IGroupMemberNode> whereClause = new JoinGroupNode();
            given.setWhereClause(whereClause);
            
            // the required joins in a known order.
            whereClause.addChild(s.p5);
            whereClause.addChild(s.p3);
            whereClause.addChild(s.p1);
            whereClause.addChild(s.p0);
            whereClause.addChild(s.p2);
            whereClause.addChild(s.p4);
            whereClause.addChild(s.p6);
            
            // now add in the filters.
            whereClause.addChild(s.c0);
            whereClause.addChild(s.c1);
            whereClause.addChild(s.c2);

        }

        // The expected AST after the rewrite.
        final QueryRoot expected;
        {

            final BSBMQ5Setup s = new BSBMQ5Setup(store);
            
            expected = s.queryRoot;

            /*
             * Build up the join group. The joins will appear in the same order
             * but all of the filters will have been attached to joins.
             */
            final GraphPatternGroup<IGroupMemberNode> whereClause = new JoinGroupNode();
            expected.setWhereClause(whereClause);
            
            // the required joins in the same order.
            whereClause.addChild(s.p5);
            whereClause.addChild(s.p3);
            whereClause.addChild(s.p1);
            whereClause.addChild(s.p0);
            whereClause.addChild(s.p2);
            whereClause.addChild(s.p4);
            whereClause.addChild(s.p6);
            
            // Clear the parent references on the filters.
            s.c0.setParent(null);
            s.c1.setParent(null);
            s.c2.setParent(null);
            
            // now attach the filters to the joins.
            s.p0.setAttachedJoinFilters(Collections.singletonList(s.c0));
            s.p4.setAttachedJoinFilters(Collections.singletonList(s.c1));
            s.p6.setAttachedJoinFilters(Collections.singletonList(s.c2));
            
        }

        final IASTOptimizer rewriter = new ASTAttachJoinFiltersOptimizer();
        
        final AST2BOpContext context = new AST2BOpContext(new ASTContainer(
                given), store);

        final IQueryNode actual = rewriter.optimize(context,
                given/* queryNode */, bsets);

        assertSameAST(expected, actual);

        /*
         * Verify no change if we feed the output back into the input. This
         * tests the ability to pick up the attached join filters for
         * consideration the next time around.
         * 
         * TODO Actually, the best test would be if we changed the join order
         * as well.
         */
        final IQueryNode actual2 = rewriter.optimize(context,
                BOpUtility.deepCopy(expected)/* queryNode */, bsets);

        assertSameAST(expected, actual2);
        
    }

}
