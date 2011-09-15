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
 * Created on Sep 15, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.controller.SubqueryOp;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.FilterNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;

/**
 * A "simple optional" is an optional sub-group that contains only one statement
 * pattern, no sub-groups of its own, and no filters that require materialized
 * variables. We can lift these "simple optionals" into the parent group without
 * incurring the costs of launching a {@link SubqueryOp}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTSimpleOptionalOptimizer implements IASTOptimizer {

    @Override
    public IQueryNode optimize(final AST2BOpContext context,
            final IQueryNode queryNode, final IBindingSet[] bindingSets) {        
        
        return queryNode;
        
    }

    /**
     * Get the single "simple optional" statement pattern.
     * 
     * @return The simple optional from the group -or- <code>null</code> iff the
     *         group is not a simple optional.
     * 
     *         FIXME UNIT TEST
     */
    public StatementPatternNode getSimpleOptional(final JoinGroupNode group) {
        
        if (!group.isOptional()) {
         
            // first, the whole group must be optional TODO test this in caller
            return null;

        }

        /*
         * Second, make sure we have only one statement pattern, no sub-queries,
         * and no filters that require materialization.
         */
        StatementPatternNode sp = null;

        for (IQueryNode node : group) {

            if (node instanceof StatementPatternNode) {

                if (sp != null) {
                    /*
                     * We already have one statement pattern so this is not a
                     * simple optional.
                     */
                    return null;
                }

                sp = (StatementPatternNode) node;

            } else if (node instanceof FilterNode) {

                final FilterNode filter = (FilterNode) node;

                final INeedsMaterialization req = filter
                        .getMaterializationRequirement();

                if (req.getRequirement() != INeedsMaterialization.Requirement.NEVER) {

                    /*
                     * There are materialization requirements for this join.
                     * 
                     * TODO If the filter can be lifted into the parent then we
                     * can still do this rewrite!
                     */

                    return null;

                }

            } else {

                /*
                 * Anything else will queer the deal.
                 */

                return null;

            }

        }

        // if we've made it this far, we are simple optional
        return sp;

    }

}
