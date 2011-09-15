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
 * Created on Sep 1, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.UnionNode;

/**
 * Utility class detects a badly designed left join. This is used to decide
 * whether an optional join is "well designed" as defined in section 4.2 of
 * "Semantics and Complexity of SPARQL", 2006, Jorge Pérez et al.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BadlyDesignedLeftJoinsUtil.java 5118 2011-09-01 17:36:12Z
 *          thompsonbry $
 * 
 * @see https://sourceforge.net/apps/trac/bigdata/ticket/232
 * @see http://www.dcc.uchile.cl/~cgutierr/papers/sparql.pdf
 * 
 * @deprecated by {@link ASTBottomUpOptimizer}
 */
public class BadlyDesignedLeftJoinsUtil {

    /**
     * We are looking for queries of the form:
     * 
     * P = ((?X, name, paul) OPT ((?Y, name, george) OPT (?X, email, ?Z)))
     * 
     * i.e. variables used by the right side of a left join that are not bound
     * in the parent group but are bound in groups above the parent group.
     */
    public static void checkForBadlyDesignedLeftJoin(final QueryRoot tree) {
        
        final Set<IVariable<?>> bindings = new LinkedHashSet<IVariable<?>>();

        final IGroupNode<IGroupMemberNode> root = tree.getWhereClause();

        addVars(bindings, root, false/* includeFilters */);

        if (!root.isEmpty()) {

            for (IGroupMemberNode child : root) {

                if (child instanceof IGroupNode<?>) {

                    checkForBadlyDesignedLeftJoin(bindings, (IGroupNode) child);

                }

            }

        }

    }

    private static void checkForBadlyDesignedLeftJoin(
            final Set<IVariable<?>> bindings,
            final IGroupNode<IGroupMemberNode> group) {

        /*
         * Identify problem vars: 
         * 
         * 1. Add all vars used in the group (statement patterns and filters)
         * 2. Remove all vars bound by the parent group (statement patterns)
         * 3. Retain all vars from the grandparent groups (statement patterns)
         */

        if (group.isOptional()) {

            final Set<IVariable<?>> problemVars = new LinkedHashSet<IVariable<?>>();

            addVars(problemVars, group, true/* includeFilters */);

            final Set<IVariable<?>> parentVars = new LinkedHashSet<IVariable<?>>();

            IGroupNode<? extends IGroupMemberNode> parent = group.getParent();

            while (parent instanceof UnionNode) {

                parent = parent.getParent();

            }

            addVars(parentVars, parent, false/* includeFilters */);

            problemVars.removeAll(parentVars);

            problemVars.retainAll(bindings);

            if (!problemVars.isEmpty()) {

                throw new BadlyDesignedLeftJoinIteratorException();

            }

        }

        /*
         * Recursively check the children.
         */
        if (!group.isEmpty()) {

            addVars(bindings, group, false/* includeFilters */);

            for (IGroupMemberNode child : group) {

                if (child instanceof IGroupNode<?>) {

                    checkForBadlyDesignedLeftJoin(bindings, (IGroupNode) child);

                }

            }

        }

    }

    /**
     * Collect all variables appearing in the WHERE clause.
     * @param bindings
     * @param group
     * @param includeFilters
     * 
     * TODO This does not pay attention to subquery boundaries.
     */
    private static void addVars(final Set<IVariable<?>> bindings,
            final IGroupNode<? extends IGroupMemberNode> group,
            final boolean includeFilters) {

        for (IGroupMemberNode op : group) {

            if (!(op instanceof StatementPatternNode) && !includeFilters)
                continue;

            final Iterator<IVariable<?>> it = BOpUtility
                    .getSpannedVariables((BOp) op);

            while (it.hasNext()) {

                bindings.add(it.next());
                
            }

        }

    }

    public static final class BadlyDesignedLeftJoinIteratorException extends
            RuntimeException {

        private static final long serialVersionUID = 1L;

    }

}
