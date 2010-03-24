/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.rdf.spo;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.IRawTripleStore;
import com.bigdata.relation.accesspath.AbstractAccessPath;
import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.relation.rule.ArrayBindingSet;
import com.bigdata.relation.rule.Constant;
import com.bigdata.relation.rule.IConstant;
import com.bigdata.relation.rule.IPredicate;
import com.bigdata.relation.rule.IVariable;
import com.bigdata.relation.rule.IVariableOrConstant;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

/**
 * {@link IAccessPath} implementation for an {@link SPORelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class SPOAccessPath extends AbstractAccessPath<ISPO> {

    private static transient final long NULL = IRawTripleStore.NULL;

    private SPOTupleSerializer          tupleSer;

    /** Relation (resolved lazily if not specified to the ctor). */
    private SPORelation                 relation;

    /**
     * Variant when the {@link SPORelation} has already been materialized.
     * <p>
     * Note: Filters should be specified when the {@link IAccessPath} is
     * constructed so that they will be evaluated on the data service rather
     * than materializing the elements and then filtering them. This can be
     * accomplished by adding the filter as a constraint on the predicate when
     * specifying the access path.
     * 
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     */
    public SPOAccessPath(final SPORelation relation,
            final IPredicate<ISPO> predicate, final IKeyOrder<ISPO> keyOrder,
            final IIndex ndx, final int flags, final int chunkOfChunksCapacity,
            final int chunkCapacity, final int fullyBufferedReadThreshold) {

        this(relation.getIndexManager(), relation.getTimestamp(), predicate,
                keyOrder, ndx, flags, chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);

        this.relation = relation;

    }

    /**
     * Variant does not require the {@link SPORelation} to have been
     * materialized. This is useful when you want an {@link IAccessPath} for a
     * specific index partition.
     * 
     * @param indexManager
     * @param timestamp
     * @param predicate
     * @param keyOrder
     * @param ndx
     * @param flags
     * @param chunkOfChunksCapacity
     * @param chunkCapacity
     * @param fullyBufferedReadThreshold
     */
    public SPOAccessPath(final IIndexManager indexManager,
            final long timestamp, final IPredicate<ISPO> predicate,
            final IKeyOrder<ISPO> keyOrder, final IIndex ndx, final int flags,
            final int chunkOfChunksCapacity, final int chunkCapacity,
            final int fullyBufferedReadThreshold) {

        super(indexManager, timestamp, predicate, keyOrder, ndx, flags,
                chunkOfChunksCapacity, chunkCapacity,
                fullyBufferedReadThreshold);

    }

    /**
     * Return the constant bound on the {@link #getPredicate() predicate} for
     * this access path at the specified index -or- {@link #NULL} iff the
     * predicate is not bound at that position.
     * 
     * @param index
     *            The index.
     * 
     * @return Either the bound value -or- {@link #NULL} iff the index is
     *         unbound for the predicate for this access path.
     */
    @SuppressWarnings("unchecked")
    public long get(final int index) {

        final IVariableOrConstant<Long> t = predicate.get(index);

        return t.isVar() ? NULL : t.get();

    }

    protected SPOTupleSerializer getTupleSerializer() {

        if (tupleSer == null) {

            tupleSer = (SPOTupleSerializer) ndx.getIndexMetadata()
                    .getTupleSerializer();

        }

        return tupleSer;

    }

    public SPOAccessPath init() {

        final IKeyBuilder keyBuilder = getTupleSerializer().getKeyBuilder();

        setFromKey(((SPOKeyOrder) keyOrder).getFromKey(keyBuilder, predicate));

        setToKey(((SPOKeyOrder) keyOrder).getToKey(keyBuilder, predicate));

        //        final SPOKeyOrder keyOrder = (SPOKeyOrder) this.keyOrder;
        //        final int keyArity = keyOrder.getKeyArity(); // use the key's "arity".
        //        
        //        { // do the from key
        //            
        //            keyBuilder.reset();
        //            boolean noneBound = true;
        //            for (int i = 0; i < keyArity; i++) {
        //                IVariableOrConstant<Long> term = 
        //                    predicate.get(keyOrder.getKeyOrder(i));
        //                long l;
        //                // Note: term MAY be null for the context position.
        //                if (term == null || term.isVar()) {
        //                    l = Long.MIN_VALUE;
        //                } else {
        //                    l = term.get();
        //                    noneBound = false;
        //                }
        //                keyBuilder.append(l);
        //            }
        //            final byte[] fromKey = noneBound ? null : keyBuilder.getKey();
        //            setFromKey(fromKey);
        //            
        //        }
        //
        //        { // do the to key
        //
        //            keyBuilder.reset();
        //            boolean noneBound = true;
        //            boolean foundLastBound = false;
        //            for (int i = 0; i < keyArity; i++) {
        //                IVariableOrConstant<Long> term = 
        //                    predicate.get(keyOrder.getKeyOrder(i));
        //                long l;
        //                // Note: term MAY be null for context.
        //                if (term == null || term.isVar()) {
        //                    l = Long.MIN_VALUE;
        //                } else {
        //                    l = term.get();
        //                    noneBound = false;
        //                    if (!foundLastBound) {
        //                        if (i == keyArity-1) {
        //                            l++;
        //                            foundLastBound = true;
        //                        } else {
        //                            IVariableOrConstant<Long> next = 
        //                                predicate.get(keyOrder.getKeyOrder(i+1));
        //                            // Note: next can be null for quads (context pos).
        //                            if (next == null || next.isVar()) {
        //                                l++;
        //                                foundLastBound = true;
        //                            }
        //                        }
        //                    }
        //                }
        //                keyBuilder.append(l);
        //            }
        //            final byte[] toKey = noneBound ? null : keyBuilder.getKey();
        //            setToKey(toKey);
        //           
        //        }

        super.init();

        return this;

    }

    /**
     * Resolved lazily if not specified to the ctor.
     */
    synchronized
    public SPORelation getRelation() {

        if (relation == null) {

            relation = (SPORelation) indexManager.getResourceLocator().locate(
                    predicate.getOnlyRelationName(), timestamp);

        }

        return relation;

    }

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

    @Override
    public SPOPredicate getPredicate() {

        return (SPOPredicate) super.getPredicate();

    }

    /**
     * Return a new {@link SPOAccessPath} where the context position has been
     * bound to the specified constant. The context position MUST be a variable.
     * All instances of that variable will be replaced by the specified
     * constant. This is used to constrain an access path to each graph in the
     * set of default graphs when evaluating a SPARQL query against the
     * "default graph".
     * <p>
     * Note: The added constraint may mean that a different index provides more
     * efficient traversal. For scale-out, this means that the data may be on
     * different index partition.
     * 
     * @param c
     *            The context term identifier.
     * 
     * @return The constrained {@link IAccessPath}.
     */
    public SPOAccessPath bindContext(final long c) {

        return bindPosition(3, c);

    }

    /**
     * Return a new {@link SPOAccessPath} where the given position has been bound to the specified constant. The given position MUST be a variable. All
     * instances of that variable will be replaced by the specified constant.
     * <p>
     * Note: The added constraint may mean that a different index provides more efficient traversal. For scale-out, this means that the data may be on different
     * index partition.
     * 
     * @param position
     *            The position to replace.
     * @param v
     *            The constant value to which the variable at the given position is to be set
     * 
     * @return The constrained {@link IAccessPath}.
     */
    public SPOAccessPath bindPosition(int position, final long v) {

        if (v == IRawTripleStore.NULL) {

            // or return EmptyAccessPath.
            throw new IllegalArgumentException();

        }

        final IVariableOrConstant<Long> var = getPredicate().get(position);

        /*
         * Constrain the access path by setting the given position on its
         * predicate.
         * 
         * Note: This option will always do better when you are running against
         * local data (LocalTripleStore).
         */

        final SPOPredicate p;

        if (position == 3 && var == null) {

            /*
             * The context position was never set on the original predicate, so
             * it is neither a variable nor a constant. In this case we just set
             * the context position to the desired constant.
             */

            p = getPredicate().setC(new Constant<Long>(v));

        } else if (var.isVar()) {

            /*
             * The context position is a variable. Replace all occurrences of
             * that variable in the predicate with the desired constant.
             */

            p = getPredicate().asBound(new ArrayBindingSet(//
                    new IVariable[] { (IVariable<Long>) var },//
                    new IConstant[] { new Constant<Long>(v) }//
                    ));
        } else {

            /*
             * The context position is already bound to a constant.
             */

            if (var.get().longValue() == v) {

                /*
                 * The desired constant is already specified for the context
                 * position.
                 */

                return this;

            }

            /*
             * A different constant is already specified for the context
             * position. This is an error since you are only allowed to add
             * constraint, not change an existing constraint.
             */

            throw new IllegalStateException();

        }

        /*
         * Let the relation figure out which access path is best given that
         * added constraint.
         */

        return (SPOAccessPath) this.getRelation().getAccessPath(p);

    }

    /**
     * Return a new {@link SPOAccessPath} where the given positions have been bound to the specified constants. The given positions MUST all be variables. All
     * instances of that variable will be replaced by the specified constant.
     * <p>
     * Note: The added constraints may mean that a different index provides more efficient traversal. For scale-out, this means that the data may be on
     * different index partition.
     * 
     * @param values
     *            The constant values used to constrain the accesspath.
     * 
     * @return The constrained {@link IAccessPath}.
     */
    public SPOAccessPath bindPosition(final long values[]) {
        IVariableOrConstant<Long> s = null, p = null, o = null, c = null;

        s = getPredicate().get(0);
        if (values[0] != 0) {
            if (s.isVar()) {
                s = new Constant<Long>(values[0]);
            }
        }

        p = getPredicate().get(1);
        if (values[1] != 0) {
            if (p.isVar()) {
                p = new Constant<Long>(values[1]);
            }
        }
        o = getPredicate().get(2);
        if (values[2] != 0) {
            if (o.isVar()) {
                o = new Constant<Long>(values[2]);
            }
        }
        c = getPredicate().get(3);
        if (values[3] != 0) {
            if (c == null || c.isVar()) {
                c = new Constant<Long>(values[3]);
            }
        }
        /*
         * Constrain the access path by setting the given positions on its
         * predicate.
         * 
         * Note: This option will always do better when you are running against
         * local data (LocalTripleStore).
         */

        final SPOPredicate pred = getPredicate().reBound(s, p, o, c);

        /*
         * Let the relation figure out which access path is best given that
         * added constraint.
         */

        SPOAccessPath spoa = (SPOAccessPath) this.getRelation().getAccessPath(pred);
        return spoa;
    }

}
