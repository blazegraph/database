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
 * Created on Oct 17, 2011
 */

package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.counters.CAT;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Utility class supporting hash join against a Java hash collection (as opposed
 * to the {@link HTree}).
 * <p>
 * acceptSolutions: A solutions set is fully materialized in a hash table. The
 * {@link Key}s of the hash table are the as-bound join variable(s). The values
 * in the hash table are {@link Bucket}s, each of which is the list of solutions
 * having a specific value for the as-bound join variables.
 * <p>
 * handleJoin: For each source solution materialized, the hash table is probed
 * using the as-bound join variables for that source solution. If there is a hit
 * in the hash table, then operator then outputs the cross product of the source
 * solution with the solutions list in the {@link Bucket} found under that
 * {@link Key} in the hash table, applying any optional CONSTRAINTS. A join hit
 * counter is carried for each solution in the hash index. The join hit counter
 * is used to support optional joins.
 * <p>
 * outputOptionals: Once the source solutions have been exhausted, all solutions
 * in the hash index having a join hit counter of ZERO (0) are output as
 * "optional" solutions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JVMHashJoinUtility {

    private static final Logger log = Logger.getLogger(JVMHashJoinUtility.class);
    
    /**
     * Buffer solutions on a JVM hash collection.
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
     */
    public static long acceptSolutions(//
            final ICloseableIterator<IBindingSet[]> itr,//
            final IVariable<?>[] joinVars,//
            final BOpStats stats,//
            final Map<Key, Bucket> map,//
            final boolean optional) {
        
        final IBindingSet[] all = BOpUtility.toArray(itr, stats);

        if (log.isDebugEnabled())
            log.debug("Materialized: " + all.length
                    + " source solutions.");

        for (IBindingSet bset : all) {

            final Key key = makeKey(joinVars, bset, optional);

            if (key == null) {
                // Drop solution.
                continue;
            }
            
            Bucket b = map.get(key);
            
            if(b == null) {
                
                map.put(key, b = new Bucket(bset));
                
            } else {
                
                b.add(bset);
                
            }

        }

        if (log.isDebugEnabled())
            log.debug("There are : " + map.size()
                    + " distinct combinations of the join vars: "
                    + Arrays.toString(joinVars));

        final long naccepted = all.length;
        
        return naccepted;
        
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
     * @param optional
     *            <code>true</code> iff the optional solutions must also be
     *            output.
     * @param leftIsPipeline
     *            <code>true</code> iff <i>left</i> is a solution from upstream
     *            in the query pipeline. Otherwise, <i>right</i> is the upstream
     *            solution.
     */
    static public void hashJoin(//
            final ICloseableIterator<IBindingSet> leftItr,//
            final IBuffer<IBindingSet> outputBuffer,//
            final IVariable<?>[] joinVars,//
            final IVariable<?>[] selectVars,//
            final IConstraint[] constraints,//
            final Map<Key,Bucket> rightSolutions,//
            final boolean optional,//
            final boolean leftIsPipeline//
    ) {

        try {

            while (leftItr.hasNext()) {

                final IBindingSet left = leftItr.next();

                if (log.isDebugEnabled())
                    log.debug("Considering " + left);

                final Key key = JVMHashJoinUtility.makeKey(joinVars, left,
                        optional);

                if (key == null) {
                    // Drop solution.
                    continue;
                }

                // Probe the hash map.
                final Bucket b = rightSolutions.get(key);

                if (b == null)
                    continue;

                for (SolutionHit right : b) {

                    if (log.isDebugEnabled())
                        log.debug("Join with " + right);

                    // See if the solutions join. 
                    final IBindingSet outSolution = //
                    BOpContext.bind(//
                            right.solution,// 
                            left,// 
                            leftIsPipeline,//
                            constraints,//
                            selectVars//
                            );

                    if (outSolution == null) {
                        // Join failed.
                        continue;
                    }

                    if (log.isDebugEnabled())
                        log.debug("Output solution: " + outSolution);

                    right.nhits.increment();

                    // Accept this binding set.
                    outputBuffer.add(outSolution);

                }

            }

        } finally {

            leftItr.close();

        }

    } // handleJoin
    
    /**
     * Identify and output the optional solutions. Optionals are identified
     * using a counter associated with each right solution. The counter will be
     * non-zero IFF the right solution joined with at least one left solution.
     * The total set of right solutions is scanned once. Any right solution
     * having a join hit counter of ZERO (0) is output.
     * 
     * @param outputBuffer
     *            Where to write the optional solutions.
     * @param rightSolutions
     *            The hash index (right).
     */
    static public void outputOptionals(
            final IBuffer<IBindingSet> outputBuffer,
            final Map<Key, Bucket> rightSolutions // 
    ) {

        /*
         * Note: when NO subquery solutions joined for a given source binding
         * set AND the subquery is OPTIONAL then we output the _original_
         * binding set to the sink join task(s) and DO NOT apply the
         * CONSTRAINT(s).
         */

        for(Bucket b : rightSolutions.values()) {
            
            for(SolutionHit hit : b) {

                if (hit.nhits.get() > 0)
                    continue;

                final IBindingSet bs = hit.solution;

                if (log.isDebugEnabled())
                    log.debug("Optional solution: " + bs);

                outputBuffer.add(bs);

            }
            
        }

    } // outputOptionals
    
    /**
     * Return an array of constants corresponding to the as-bound values of the
     * join variables for the given solution.
     * 
     * @param joinVars
     *            The join variables.
     * @param bset
     *            The solution.
     * @param optional
     *            <code>true</code> iff the hash join is optional.
     * 
     * @return The as-bound values for the join variables for that solution. 
     */
    static private Key makeKey(final IVariable<?>[] joinVars,
            final IBindingSet bset, final boolean optional) {

        final IConstant<?>[] vals = new IConstant<?>[joinVars.length];

        for (int i = 0; i < joinVars.length; i++) {

            final IVariable<?> v = joinVars[i];

            vals[i] = bset.get(v);

        }

        int hashCode = HashJoinUtility.ONE;
        try {
            
            hashCode = HashJoinUtility.hashCode(joinVars, bset);
            
        } catch (JoinVariableNotBoundException ex) {
            
            if (!optional) {
                
                // Drop solution;
                
                if (log.isDebugEnabled())
                    log.debug(ex);

                return null;

            }
//            throw new RuntimeException(ex + " : joinvars="
//                    + Arrays.toString(joinVars) + ", bset=" + bset, ex);
        }
        // final int hashCode = java.util.Arrays.hashCode(vals);

        return new Key(hashCode, vals);

    }

    /**
     * Wrapper for the keys in the hash table. This is necessary for the hash
     * table to compare the keys as equal and also provides a efficiencies in
     * the hash code and equals() methods.
     */
    public static class Key {
        
        private final int hash;

        private final IConstant<?>[] vals;

        private Key(final int hashCode, final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = hashCode;
        }

        public int hashCode() {
            return hash;
        }

        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Key)) {
                return false;
            }
            final Key t = (Key) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                if (vals[i] == t.vals[i])
                    continue;
                if (vals[i] == null)
                    return false;
                if (!vals[i].equals(t.vals[i]))
                    return false;
            }
            return true;
        }
    }
    
    /**
     * An input solution and a hit counter.
     */
    public static class SolutionHit {

        /**
         * The input solution.
         */
        final public IBindingSet solution;

        /**
         * The #of hits on that input solution when processing the join against
         * the subquery.
         */
        public final CAT nhits = new CAT();
        
        private SolutionHit(final IBindingSet solution) {
            
            if(solution == null)
                throw new IllegalArgumentException();
            
            this.solution = solution;
            
        }
        
        public String toString() {

            return getClass().getName() + "{nhits=" + nhits + ",solution="
                    + solution + "}";

        }
        
    } // class SolutionHit

    /**
     * A group of solutions having the same as-bound values for their join vars.
     * Each solution is paired with a hit counter so we can support OPTIONAL
     * semantics for the join.
     */
    public static class Bucket implements Iterable<SolutionHit>{

        /**
         * A set of solutions (and their hit counters) which have the same
         * as-bound values for the join variables.
         */
        private final List<SolutionHit> solutions = new LinkedList<SolutionHit>(); 

        public String toString() {
            return super.toString() + //
                    "{#solutions=" + solutions.size() + //
                    "}";
        }
        
        public Bucket(final IBindingSet solution) {

            add(solution);
            
        }

        public void add(final IBindingSet solution) {
         
            if (solution == null)
                throw new IllegalArgumentException();
            
            solutions.add(new SolutionHit(solution));
            
        }
        
        public Iterator<SolutionHit> iterator() {
            
            return Collections.unmodifiableList(solutions).iterator();
            
        }

    } // Bucket

}
