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
 * Created on Aug 30, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.htree.HTree;
import com.bigdata.io.SerializerUtil;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.ChunkedWrappedIterator;
import com.bigdata.striterator.Chunkerator;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Utility methods to support hash joins using an {@link HTree}. There are two
 * core steps, plus one additional step if the join is optional. The hash join
 * logically has a <em>Left Hand Side</em> (LHS) and a Right Hand Side (RHS).
 * The RHS is used to build up a hash index which is then probed for each LHS
 * solution. The LHS is generally an access path scan, which is done once. A
 * hash join therefore provides an alternative to a nested index join in which
 * we visit the access path once, probing the hash index for solutions which
 * join.
 * <dl>
 * <dt>Accept solutions</dt>
 * <dd>This step builds the hash index, also known as the RHS (Right Hand Side).
 * </dd>
 * <dt>hash join</dt>
 * <dd>The hash join considers each left solution in turn and outputs solutions
 * which join. If optionals are required, this step also builds an hash index
 * (the <i>joinSet</i>) over the right solutions which did join.</dd>
 * <dt>Output optionals</dt>
 * <dd>The RHS hash index is scanned and the <i>joinSet</i> is probed to
 * identify right solutions which did not join with any left solution. Those
 * solutions are output as "optionals".</dd>
 * </dl>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class HashJoinUtility {

    static private final transient Logger log = Logger
            .getLogger(HashJoinUtility.class);

    /**
     * Note: If joinVars is an empty array, then the solutions will all hash to
     * ONE (1).
     */
    static private final int ONE = 1;
    
    /**
     * Return the hash code which will be used as the key given the ordered
     * as-bound values for the join variables.
     * 
     * @param joinVars
     *            The join variables.
     * @param bset
     *            The bindings whose as-bound hash code for the join variables
     *            will be computed.
     * 
     * @return The hash code.
     * 
     * @throws JoinVariableNotBoundException
     *             if there is no binding for a join variable.
     */
    static int hashCode(final IVariable<?>[] joinVars, final IBindingSet bset)
            throws JoinVariableNotBoundException {

        int h = ONE;

        for (IVariable<?> v : joinVars) {
            
            final IConstant<?> c = bset.get(v);

            if (c == null) {

                // Reject any solution which does not have a binding for a join
                // variable.

                throw new JoinVariableNotBoundException(v.getName());
                
            }

//            final int ch = (int) c.hashCode() ^ (c.hashCode() >>> 32);
//
//            h = 31 * h + ch;
            h ^= c.hashCode();
            
        }
        
//        System.err.println("hashCode=" + h + ", joinVars="
//                + Arrays.toString(joinVars) + " : " + bset);

        return h;

    }

    /**
     * Buffer solutions on an {@link HTree}.
     * 
     * @param itr
     *            The source from which the solutions will be drained.
     * @param joinVars
     *            The join variables (required). There must be at least one join
     *            variable.
     * @param stats
     *            The statistics to be updated as the solutions are buffered on
     *            the hash index.
     * @param htree
     *            The hash index.
     * @param optional
     *            <code>true</code> iff the join is optional. When
     *            <code>true</code>, solutions which do not have a binding for
     *            one or more of the join variables will be inserted into the
     *            hash index anyway using <code>hashCode:=1</code>. This allows
     *            the solutions to be discovered when we scan the hash index and
     *            the set of solutions which did join to identify the optional
     *            solutions.
     * 
     * @return The #of solutions that were buffered.
     * 
     *         FIXME Does anything actually rely on the
     *         {@link JoinVariableNotBoundException}? It would seem that this
     *         exception could only be thrown if the joinvars[] was incorrectly
     *         formulated as it should only include "known bound" variables.
     */
    public static long acceptSolutions(
            final ICloseableIterator<IBindingSet[]> itr,
            final IVariable<?>[] joinVars, final BOpStats stats,
            final HTree htree,
            final boolean optional) {

        long n = 0L;
        
        while (itr.hasNext()) {

            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

            for (IBindingSet bset : a) {

                int hashCode = ONE; // default (used iff join is optional).
                try {

                    hashCode = HashJoinUtility.hashCode(joinVars, bset);

                } catch (JoinVariableNotBoundException ex) {

                    if (!optional) {
                        
                        // Drop solution;

                        if (log.isDebugEnabled())
                            log.debug(ex);

                        continue;

                    }
                    
                }

                // Insert binding set under hash code for that key.
                htree.insert(hashCode, SerializerUtil.serialize(bset));
                
            }

            n += a.length;
            
        }
        
        return n;

    }

    /**
     * Glue class for hash code and binding set used when the hash code is for
     * just the join variables rather than the entire binding set.
     */
    private static class BS implements Comparable<BS> {

        final private int hashCode;

        final private IBindingSet bset;

        BS(final int hashCode, final IBindingSet bset) {
            this.hashCode = hashCode;
            this.bset = bset;
        }

        @Override
        public int compareTo(final BS o) {
            if (this.hashCode < o.hashCode)
                return -1;
            if (this.hashCode > o.hashCode)
                return 1;
            return 0;
        }
        
        public String toString() {
            return getClass().getName() + "{hashCode=" + hashCode + ",bset="
                    + bset + "}";
        }
        
    }
    
    /**
     * Do a hash join between a stream of source solutions (left) and a hash
     * index (right). For each left solution, the hash index (right) is probed
     * for possible matches (solutions whose as-bound values for the join
     * variables produce the same hash code). Possible matches are tested for
     * consistency and the constraints (if any) are applied. Solutions which
     * join are written on the caller's buffer.
     * 
     * @param leftItr
     *            A stream of solutions to be joined against the hash index
     *            (left).
     * @param outputBuffer
     *            Where to write the solutions which join.
     * @param joinVars
     *            The join variables (required). Solutions which do not have
     *            bindings for the join variables will NOT join. If an empty
     *            array is specified then all solutions will have a hash code of
     *            ONE (1) and the join will degrade to a full N x M comparison.
     *            Only solutions which are consistent with one another and with
     *            the optional constraints will actually join, but the join will
     *            do much more work to find those solutions.
     * @param selectVars
     *            The variables to be retained (optional, all a retained if no
     *            specified).
     * @param constraints
     *            Constraints on the solutions (optional, may be
     *            <code>null</code>).
     * @param rightSolutions
     *            A hash index already built over some multiset of solutions
     *            (right).
     * @param joinSet
     *            A hash index to be populated with (right) solutions which
     *            join. This is only required when <code>optional:=true</code>.
     *            The resulting hash index is used to detect the optional
     *            solutions in a separate step.
     * @param optional
     *            <code>true</code> iff the optional solutions must also be
     *            output, in which case the <i>joinSet</i> is required and will
     *            be populated by this method.
     * @param leftIsPipeline
     *            <code>true</code> iff <i>left</i> is a solution from upstream
     *            in the query pipeline. Otherwise, <i>right</i> is the upstream
     *            solution.
     * 
     * @see https://sourceforge.net/apps/trac/bigdata/ticket/233 (Inline access
     *      path).
     * 
     * @see BOpContext#solutions(com.bigdata.striterator.IChunkedIterator,
     *      com.bigdata.bop.IPredicate, IVariable[], BaseJoinStats)
     */
    static public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final HTree rightSolutions,//
            final HTree joinSet,//
            final boolean optional,//
            final boolean leftIsPipeline//
            ) {

        try {

            // TODO parameter from operator annotations.
            final int chunkSize = ChunkedWrappedIterator.DEFAULT_CHUNK_SIZE;
            
            final Iterator<IBindingSet[]> it = new Chunkerator<IBindingSet>(
                    leftItr, chunkSize, IBindingSet.class);
            
            while (it.hasNext()) {

                final IBindingSet[] leftSolutions = it.next();

                final BS[] a = new BS[leftSolutions.length];

                int n = 0; // The #of non-dropped source solutions.
                
                for (int i = 0; i < a.length; i++) {

                    // Compute hash code from bindings on the join vars.
                    int hashCode;
                    try {
                        hashCode = HashJoinUtility.hashCode(joinVars,
                                leftSolutions[i]);
                        a[n++] = new BS(hashCode, leftSolutions[i]);
                    } catch (JoinVariableNotBoundException ex) {
                        // Drop solution
                        if (log.isDebugEnabled())
                            log.debug(ex);
                        continue;
                    }
                }

                /*
                 * Sort by the computed hash code. This not only orders the
                 * accesses into the HTree but it also allows us to handle all
                 * source solutions which have the same hash code with a single
                 * scan of the appropriate collision bucket in the HTree.
                 */
                Arrays.sort(a, 0, n);

                int fromIndex = 0;
                
                while (fromIndex < n) {

                    // The next hash code to be processed.
                    final int hashCode = a[fromIndex].hashCode;

                    // scan for the first hash code which is different.
                    int toIndex = n; // assume upper bound.
                    for (int i = fromIndex + 1; i < n; i++) {
                        if (a[i].hashCode != hashCode) {
                            toIndex = i;
                            break;
                        }
                    }
                    // #of left solutions having the same hash code.
                    final int bucketSize = toIndex - fromIndex;

                    /*
                     * Note: all source solutions in [fromIndex:toIndex) have
                     * the same hash code.
                     */
                    {

                        // visit all source solutions having the same hash code
                        @SuppressWarnings("unchecked")
                        final ITupleIterator<IBindingSet> titr = rightSolutions
                                .lookupAll(hashCode);

                        while (titr.hasNext()) {

                            final ITuple<IBindingSet> t = titr.next();

                            /*
                             * Note: The map entries must be the full source
                             * binding set, not just the join variables, even
                             * though the key and equality in the key is defined
                             * in terms of just the join variables.
                             * 
                             * Note: Solutions which have the same hash code but
                             * whose bindings are inconsistent will be rejected
                             * by bind() below.
                             */

                            final IBindingSet rightSolution = t.getObject();

                            for (int i = fromIndex; i < toIndex; i++) {

                                final IBindingSet leftSolution = a[i].bset;
                                
                                // Join.
                                final IBindingSet outSolution = BOpContext
                                        .bind(leftSolution, rightSolution,
                                                leftIsPipeline, constraints,
                                                selectVars);

                                if (outSolution == null) {

                                    // Join failed.
                                    continue;

                                }

                                if (log.isDebugEnabled())
                                    log.debug("Output solution: " + outSolution);

                                // Accept this binding set.
                                outputBuffer.add(outSolution);

                                if (optional) {

                                    saveInJoinSet(joinSet, rightSolution);

                                }

                            }

                        } // next rightSolution with the same hash code.

                    } // end block of leftSolutions having the same hash code.

                    fromIndex = toIndex;
                    
                } // next slice of source solutions with the same hash code.

            } // while(itr.hasNext()

        } finally {

            leftItr.close();

        }

    } // handleJoin

    /**
     * Add to 2nd hash tree of all solutions which join.
     * <p>
     * Note: the hash key is based on the entire solution for this htree.
     * 
     * TODO This code does allows duplicate solutions into the joinSet. There is
     * code below which does not permit this. We might need to refactor that
     * code into a utility method (together with how we obtain the appropriate
     * hash code) and use it to filter out duplicates for the joinSet and/or the
     * rightSolutions.
     */
    static private void saveInJoinSet(final HTree joinSet,
            final IBindingSet rightSolution) {

        joinSet.insert(rightSolution);
        
//        final int joinSetHashCode = rightSolution.hashCode();
//        
//        // visit all joinSet solutions having the same hash code
//        @SuppressWarnings("unchecked")
//        final ITupleIterator<IBindingSet> xitr = joinSet
//                .lookupAll(joinSetHashCode);
//
//        boolean found = false;
//        while (!found && xitr.hasNext() ) {
//
//            final ITuple<IBindingSet> xt = xitr.next();
//
//            /*
//             * Note: The map entries must be the full source
//             * binding set, not just the join variables, even
//             * though the key and equality in the key is defined
//             * in terms of just the join variables.
//             * 
//             * Note: Solutions which have the same hash code but
//             * whose bindings are inconsistent will be rejected
//             * by bind() below.
//             */
//            final IBindingSet aSolution = xt.getObject();
//            
//            if (rightSolution.equals(aSolution)) {
//
//                if (log.isDebugEnabled())
//                    log.debug("Solution already in joinSet: "
//                            + rightSolution);
//                
//                found = true;
//                
//                break;
//                
//            }
//
//        }
//
//        if (!found) {
//         
//            joinSet.insert(rightSolution);
//            
//            if (log.isDebugEnabled())
//                log.debug("Solution added to joinSet: "
//                        + rightSolution);
//            
//        }

    }
    
    /**
     * Identify and output the optional solutions. Optionals are identified
     * using a <i>joinSet</i> containing each right solution which joined with
     * at least one left solution. The total set of right solutions is then
     * scanned once. For each right solution, we probe the <i>joinSet</i>. If
     * the right solution did not join, then it is output now as an optional
     * join.
     * 
     * @param outputBuffer
     *            Where to write the optional solutions.
     * @param rightSolutions
     *            The hash index (right).
     * @param joinSet
     *            The set of distinct right solutions which joined. This set is
     *            maintained iff the join is optional.
     */
    static public void outputOptionals(
            final IBuffer<IBindingSet> outputBuffer,
            final HTree rightSolutions, //
            final HTree joinSet//
            ) {

        // Visit all source solutions.
        @SuppressWarnings("unchecked")
        final ITupleIterator<IBindingSet> sitr = rightSolutions
                .rangeIterator();
        
        while(sitr.hasNext()) {
            
            final ITuple<IBindingSet> t = sitr.next();
            
            final IBindingSet rightSolution = t.getObject();

            // The hash code is based on the entire solution for the
            // joinSet.
            final int hashCode = rightSolution.hashCode();
            
            // Probe the join set for this source solution.
            @SuppressWarnings("unchecked")
            final ITupleIterator<IBindingSet> jitr = joinSet
                    .lookupAll(hashCode);

            if (!jitr.hasNext()) {

                /*
                 * Since the source solution is not in the join set,
                 * output it as an optional solution.
                 */
                outputBuffer.add(rightSolution);

            }

        }

    } // handleOptionals.
    
}
