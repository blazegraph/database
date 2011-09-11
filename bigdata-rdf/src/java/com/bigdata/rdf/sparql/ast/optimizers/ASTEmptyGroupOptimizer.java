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
 * Created on Sep 10, 2011
 */

package com.bigdata.rdf.sparql.ast.optimizers;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.GroupNodeBase;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IQueryNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;

/**
 * Eliminate semantically empty join group nodes which are the sole child of
 * another join groups. Such nodes either do not specify a context or they
 * specify the same context as the parent.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ASTEmptyGroupOptimizer implements IASTOptimizer {

    private static final Logger log = Logger
            .getLogger(ASTEmptyGroupOptimizer.class);

    @Override
    public IQueryNode optimize(AST2BOpContext context, IQueryNode queryNode,
            IBindingSet[] bindingSets) {

        eliminateEmptyGroups(null/* parentGroup */, (BOp) queryNode);

        return queryNode;

    }

    private static BOp eliminateEmptyGroups(
            final GroupNodeBase<IGroupMemberNode> parentGroup, final BOp op) {

        if (op == null)
            return null;

        /*
         * Either the parentGroup on entry or this operator if it is a group
         * node.
         */
        @SuppressWarnings("unchecked")
        final GroupNodeBase<IGroupMemberNode> newParentGroup = (GroupNodeBase<IGroupMemberNode>) ((op instanceof GroupNodeBase<?>) ? op
                : parentGroup);

        boolean dirty = false;

        /*
         * Children.
         */
        final int arity = op.arity();

        final BOp[] args = arity == 0 ? BOp.NOARGS : new BOp[arity];

        for (int i = 0; i < arity; i++) {

            final BOp child = op.get(i);

            // depth first recursion.
            args[i] = eliminateEmptyGroups(newParentGroup, child);

            if (args[i] != child)
                dirty = true;

        }

        /*
         * Annotations.
         */

        final LinkedHashMap<String, Object> anns = new LinkedHashMap<String, Object>();

        for (Map.Entry<String, Object> e : op.annotations().entrySet()) {

            final String name = e.getKey();

            final Object oval = e.getValue();

            final Object nval;

            if (oval instanceof BOp) {

                nval = eliminateEmptyGroups(null/* parentGroup */, (BOp) oval);

                if (oval != nval)
                    dirty = true;

            } else {

                nval = oval;

            }

            anns.put(name, nval);

        }

        if (parentGroup != null && op instanceof JoinGroupNode) {

            final JoinGroupNode tmp = (JoinGroupNode) op;

            if (parentGroup == tmp.getParent() && parentGroup.size() == 1) {

                /*
                 * The parent is a join group and this operator also a join
                 * group and, further, it is the only child of that parent.
                 */

                if (parentGroup.getContext() == tmp.getContext()
                        || parentGroup.getContext() == null
                        || tmp.getContext() == null) {

                    /*
                     * Note: We can eliminate the parent even if it is optional
                     * or has a context by setting those attributes on the child
                     * (as long as the child does not have a different context).
                     * 
                     * Eliminate the parentGroup, replacing it with this
                     * operator.
                     * 
                     * TODO We probably should scan the parent's annotations for
                     * other things which could be copied to this operator. That
                     * will let us preserve hints for a join group if a parent
                     * existed only to communicate those hints.
                     */

                    if (log.isInfoEnabled())
                        log.info("Eliminating parent group: " + parentGroup
                                + ", replacing it with: " + op);

                    /*
                     * If the parent has a context, then set it on this
                     * operator.
                     */
                    if (parentGroup.getContext() != null) {
                        tmp.setContext(parentGroup.getContext());
                    }

                    /*
                     * If the parent was optional, then mark this operator as
                     * optional.
                     */
                    if (parentGroup.isOptional()) {
                        tmp.setOptional(parentGroup.isOptional());
                    }

                    /*
                     * Update the parent on this operator to it's parent's
                     * parent.
                     */

                    tmp.setParent(parentGroup.getParent());

                    return op;

                }

            }

        }

        if (!dirty)
            return op;

        try {

            final Constructor<BOp> ctor = (Constructor<BOp>) op.getClass()
                    .getConstructor(BOp[].class, Map.class);

            return ctor.newInstance(args, anns);

        } catch (Exception e1) {

            throw new RuntimeException(e1);

        }

    }

}
