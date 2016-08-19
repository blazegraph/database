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
package com.bigdata.rdf.spo;

import com.bigdata.bop.IPredicate;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * {@link IAccessPath} implementation for an {@link SPORelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class SPOAccessPath extends AccessPath<ISPO> {

    /**
     * 
     * @param relation (optional).
     * @param indexManager (required) 
     * @param timestamp
     * @param predicate
     * @param keyOrder
     */
    public SPOAccessPath(final IRelation<ISPO> relation,
            final IIndexManager localIndexManager,
            final IPredicate<ISPO> predicate, final IKeyOrder<ISPO> keyOrder) {

        super(relation, localIndexManager, predicate, keyOrder);

    }

    /**
     * Strengthens the return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SPOAccessPath init() {

        super.init();

        return this;

    }

    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SPORelation getRelation() {

        return (SPORelation) super.getRelation();

    }

    /**
     * Strengthened return type.
     * <p>
     * {@inheritDoc}
     */
    @Override
    public SPOPredicate getPredicate() {

        return (SPOPredicate) super.getPredicate();

    }

//    /**
//     * Strengthened return type.
//     * <p>
//     * {@inheritDoc}
//     */
//    @Override
//    @SuppressWarnings("rawtypes")
//    public IV get(final int index) {
//
//        return (IV) super.get(index);
//
//    }

    /**
     * Overridden to delegate to
     * {@link AbstractTripleStore#removeStatements(IChunkedOrderedIterator)} in
     * order to (a) write on all access paths; (b) handle statement identifiers,
     * including truth maintenance for statement identifiers; and (c) if
     * justifications are being maintained, then retract justifications having
     * no support once the statements visitable by this access path have been
     * retracted.
     */
    @Override
    public long removeAll() {

        return getRelation().getContainer().removeStatements(iterator());

    }

//    /**
//     * Return a new {@link SPOAccessPath} where the context position has been
//     * bound to the specified constant. The context position MUST be a variable.
//     * All instances of that variable will be replaced by the specified
//     * constant. This is used to constrain an access path to each graph in the
//     * set of default graphs when evaluating a SPARQL query against the
//     * "default graph".
//     * <p>
//     * Note: The added constraint may mean that a different index provides more
//     * efficient traversal. For scale-out, this means that the data may be on
//     * different index partition.
//     * 
//     * @param c
//     *            The context term identifier.
//     * 
//     * @return The constrained {@link IAccessPath}.
//     * 
//     * @deprecated with {@link DefaultGraphSolutionExpander} and
//     *             {@link NamedGraphSolutionExpander}.
//     */
//    public SPOAccessPath bindContext(final IV c) {
//
//        return bindPosition(3, c);
//
//    }
//
//    /**
//     * Return a new {@link SPOAccessPath} where the given position has been
//     * bound to the specified constant. The given position MUST be a variable.
//     * All instances of that variable will be replaced by the specified
//     * constant.
//     * <p>
//     * Note: The added constraint may mean that a different index provides more
//     * efficient traversal. For scale-out, this means that the data may be on
//     * different index partition.
//     * 
//     * @param position
//     *            The position to replace.
//     * @param v
//     *            The constant value to which the variable at the given position
//     *            is to be set
//     * 
//     * @return The constrained {@link IAccessPath}.
//     * 
//     * @deprecated with {@link #bindContext(IV)}
//     */
//    private SPOAccessPath bindPosition(final int position, final IV v) {
//
//        if (v == null) {
//
//            // or return EmptyAccessPath.
//            throw new IllegalArgumentException();
//
//        }
//
//        final IVariableOrConstant<IV> var = getPredicate().get(position);
//
//        /*
//         * Constrain the access path by setting the given position on its
//         * predicate.
//         * 
//         * Note: This option will always do better when you are running against
//         * local data (LocalTripleStore).
//         */
//
//        final SPOPredicate p;
//
//        if (position == 3 && var == null) {
//
//            /*
//             * The context position was never set on the original predicate, so
//             * it is neither a variable nor a constant. In this case we just set
//             * the context position to the desired constant.
//             */
//
//            p = getPredicate().setC(new Constant<IV>(v));
//
//        } else if (var.isVar()) {
//
//            /*
//             * The context position is a variable. Replace all occurrences of
//             * that variable in the predicate with the desired constant.
//             */
//
//            p = getPredicate().asBound(new ListBindingSet(//
//                    new IVariable[] { (IVariable<IV>) var },//
//                    new IConstant[] { new Constant<IV>(v) }//
//                    ));
//        } else {
//
//            /*
//             * The context position is already bound to a constant.
//             */
//
//            if (var.get().equals(v)) {
//
//                /*
//                 * The desired constant is already specified for the context
//                 * position.
//                 */
//
//                return this;
//
//            }
//
//            /*
//             * A different constant is already specified for the context
//             * position. This is an error since you are only allowed to add
//             * constraint, not change an existing constraint.
//             */
//
//            throw new IllegalStateException();
//
//        }
//
//        /*
//         * Let the relation figure out which access path is best given that
//         * added constraint.
//         */
//
//        return (SPOAccessPath) this.getRelation().getAccessPath(p);
//
//    }
//
//    /**
//     * Return a new {@link SPOAccessPath} where the given positions have been bound to the specified constants. The given positions MUST all be variables. All
//     * instances of that variable will be replaced by the specified constant.
//     * <p>
//     * Note: The added constraints may mean that a different index provides more efficient traversal. For scale-out, this means that the data may be on
//     * different index partition.
//     * 
//     * @param values
//     *            The constant values used to constrain the accesspath.
//     * 
//     * @return The constrained {@link IAccessPath}.
//     */
//    public SPOAccessPath bindPosition(final IV[] values) {
//        IVariableOrConstant<IV> s = null, p = null, o = null, c = null;
//
//        s = getPredicate().get(0);
//        if (values[0] != null) {
//            if (s.isVar()) {
//                s = new Constant<IV>(values[0]);
//            }
//        }
//
//        p = getPredicate().get(1);
//        if (values[1] != null) {
//            if (p.isVar()) {
//                p = new Constant<IV>(values[1]);
//            }
//        }
//        o = getPredicate().get(2);
//        if (values[2] != null) {
//            if (o.isVar()) {
//                o = new Constant<IV>(values[2]);
//            }
//        }
//        c = getPredicate().get(3);
//        if (values[3] != null) {
//            if (c == null || c.isVar()) {
//                c = new Constant<IV>(values[3]);
//            }
//        }
//        /*
//         * Constrain the access path by setting the given positions on its
//         * predicate.
//         * 
//         * Note: This option will always do better when you are running against
//         * local data (LocalTripleStore).
//         */
//
//        final SPOPredicate pred = getPredicate().reBound(s, p, o, c);
//
//        /*
//         * Let the relation figure out which access path is best given that
//         * added constraint.
//         */
//
//        final SPOAccessPath spoa = (SPOAccessPath) this.getRelation()
//                .getAccessPath(pred);
//
//        return spoa;
//    }

}
