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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import com.bigdata.bop.BOpContext;
import com.bigdata.bop.BOpUtility;
import com.bigdata.bop.HTreeAnnotations;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IConstraint;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.PipelineOp;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.counters.CAT;
import com.bigdata.htree.HTree;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.ICloseableIterator;

/**
 * Utility class supporting hash join against a Java hash collection.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JVMHashJoinUtility implements IHashJoinUtility {

    private static final Logger log = Logger.getLogger(JVMHashJoinUtility.class);
    
    /**
     * Note: If joinVars is an empty array, then the solutions will all hash to
     * ONE (1).
     */
    private static final int ONE = 1;
    
    /**
     * Return the hash code which will be used as the key given the ordered
     * as-bound values for the join variables.
     * 
     * @param joinVars
     *            The join variables.
     * @param bset
     *            The bindings whose as-bound hash code for the join variables
     *            will be computed.
     * @param ignoreUnboundVariables
     *            If a variable without a binding should be silently ignored.
     * 
     * @return The hash code.
     * 
     * @throws JoinVariableNotBoundException
     *             if there is no binding for a join variable.
     */
    private static int hashCode(final IVariable<?>[] joinVars,
            final IBindingSet bset, final boolean ignoreUnboundVariables)
            throws JoinVariableNotBoundException {

        int h = ONE;

        for (IVariable<?> v : joinVars) {

            final IConstant<?> c = bset.get(v);

            if (c == null) {

                if(ignoreUnboundVariables)
                    continue;

                // Reject any solution which does not have a binding for a join
                // variable.

                throw new JoinVariableNotBoundException(v.getName());
                
            }

            h = 31 * h + c.hashCode();
            
        }
        
        if (log.isTraceEnabled())
            log.trace("hashCode=" + h + ", joinVars="
                    + Arrays.toString(joinVars) + " : " + bset);

        return h;

    }
    
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

        int hashCode = ONE;
        try {

            /*
             * Note: The original version of this class always throws an
             * exception for an unbound join variable out of its hashCode() impl
             * and then handles that exception here.
             */
            
            hashCode = hashCode(joinVars, bset, false/* ignoreUnboundVariables */);

        } catch (JoinVariableNotBoundException ex) {
            
            if (!optional) {
                
                // Drop solution;
                
                if (log.isDebugEnabled())
                    log.debug(ex);

                return null;

            }

        }
        
        return new Key(hashCode, vals);

    }

    /**
     * Wrapper for the keys in the hash table. This is necessary for the hash
     * table to compare the keys as equal and also provides a efficiencies in
     * the hash code and equals() methods.
     */
    private static class Key {
        
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
    private static class SolutionHit {

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
    private static class Bucket implements Iterable<SolutionHit>{

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
        
        /**
         * Add the solution to the bucket iff the solutions is not already
         * present in the bucket.
         * <p>
         * Note: There is already a hash index in place on the join variables
         * when we are doing a DISTINCT filter. Further, only the "join"
         * variables are "selected" and participate in a DISTINCT filter.
         * Therefore, if we have a hash collision such that two solutions would
         * be directed into the same {@link Bucket} then we can not improve
         * matters but must simply scan the solutions in the bucket to decide
         * whether the new solution duplicates a solution which is already
         * present.
         * 
         * @param solution
         *            The solution.
         * 
         * @return <code>true</code> iff the bucket was modified by this
         *         operation.
         */
        public boolean addDistinct(final IBindingSet solution) {

            if(solutions.isEmpty()) {

                // First solution.
                solutions.add(new SolutionHit(solution));
                
                return true;
                
            }

            final Iterator<SolutionHit> itr = solutions.iterator();
            
            while(itr.hasNext()) {
                
                final SolutionHit aSolution = itr.next();
                
                if(aSolution.solution.equals(solution)) {
                    
                    // Solution already in this bucket.
                    return false;
                    
                }
                
            }
            
            // This is a distinct solution.
            solutions.add(new SolutionHit(solution));
            
            return true;
            
        }
        
        public Iterator<SolutionHit> iterator() {
            
            return Collections.unmodifiableList(solutions).iterator();
            
        }

    } // Bucket

    /**
     * <code>true</code> until the state is discarded by {@link #release()}.
     */
    private final AtomicBoolean open = new AtomicBoolean(true);
    
    /**
     * <code>true</code> iff the join is OPTIONAL.
     */
    private final boolean optional;
    
    /**
     * <code>true</code> iff this is a DISTINCT filter.
     */
    private final boolean filter;
    
    /**
     * The join variables.
     */
    private final IVariable<?>[] joinVars;

    /**
     * The variables to be retained (optional, all variables are retained if
     * not specified).
     */
    private final IVariable<?>[] selectVars;

    /**
     * The join constraints (optional).
     */
    private final IConstraint[] constraints;

    /**
     * The hash index.
     * <p>
     * Note: There is no separate "joinSet". Instead, the {@link SolutionHit}
     * class provides a join hit counter.
     */
    private final AtomicReference<Map<Key, Bucket>> rightSolutionsRef = new AtomicReference<Map<Key, Bucket>>();
    
    /**
     * The #of solutions accepted into the hash index.
     */
    private final CAT rightSolutionCount = new CAT();
    
    /**
     * 
     * @param op
     *            The operator whose annotation will inform construction the
     *            hash index. The {@link HTreeAnnotations} may be specified for
     *            this operator and will control the initialization of the
     *            various {@link HTree} instances.
     * @param optional
     *            <code>true</code> iff the join is optional.
     * @param filter
     *            <code>true</code> iff the hash index is being used as a
     *            DISTINCT filter. Various annotations pertaining to JOIN
     *            processing are ignored when used as a DISTINCT filter.
     * 
     * @see JVMHashJoinAnnotations
     */
    public JVMHashJoinUtility(final PipelineOp op, final boolean optional,
            final boolean filter) {

        if (op == null)
            throw new IllegalArgumentException();

        // The join variables (required).
        this.joinVars = (IVariable<?>[]) op
                .getRequiredProperty(HashJoinAnnotations.JOIN_VARS);

        // The projected variables (optional and equal to the join variables iff
        // this is a DISTINCT filter).
        this.selectVars = filter ? joinVars : (IVariable<?>[]) op
                .getProperty(JoinAnnotations.SELECT);

        // The join constraints (optional).
        this.constraints = (IConstraint[]) op
                .getProperty(JoinAnnotations.CONSTRAINTS);

        // Iff the join has OPTIONAL semantics.
        this.optional = optional;
        
        // Iff this is a DISTINCT filter.
        this.filter = filter;

        /*
         * TODO Parameter for LinkedHashMap versus HashMap. HashMap is going to
         * be faster for insert and search. LinkedHashMap will be faster for the
         * iterator. Some join patterns do not require us to use an iterator at
         * all, in which case HashMap is the clear winner. (For example, a
         * non-optional hash join against an access path never uses the iterator
         * over the hash index.)
         * 
         * TODO There is an opportunity for CONCURRENT hash map for at least the
         * DISTINCT SOLUTIONS filter and perhaps for others as well. However, to
         * do this with the DISTINCT SOLUTIONS filter we would have to make the
         * mutation operations on a Bucket atomic. E.g., using the synchronized
         * keyword. This would give us what amounts to per-hash code striped
         * locks. Note: the JVMDistinctBindingSetsOp does not use this class
         * right now because it enjoys better concurrency than the
         * JVMHashJoinUtility.
         */
        rightSolutionsRef.set(new LinkedHashMap<Key, Bucket>(//
                op.getProperty(HashMapAnnotations.INITIAL_CAPACITY,
                        HashMapAnnotations.DEFAULT_INITIAL_CAPACITY),//
                op.getProperty(HashMapAnnotations.LOAD_FACTOR,
                        HashMapAnnotations.DEFAULT_LOAD_FACTOR)//
                ));
        
    }
    
    public boolean isOptional() {
        return optional;
    }

    public boolean isFilter() {
        return filter;
    }
    
    public IVariable<?>[] getJoinVars() {
        return joinVars;
    }
    
    public IVariable<?>[] getSelectVars() {
        return selectVars;
    }
    
    public IConstraint[] getConstraints() {
        return constraints;
    }

    @Override
    public boolean isEmpty() {

        return getRightSolutionCount() == 0;
        
    }

    private Map<Key,Bucket> getRightSolutions() {
        
        return rightSolutionsRef.get();
        
    }
    
    @Override
    public long getRightSolutionCount() {
        /*
         * Note: This needs to be explicitly tracked and reported. Since each
         * entry in the rightSolutions map is a collision bucket, we can not
         * just report the size of the hash index. Instead we have to track and
         * report the #of solutions entered into the hash index in
         * acceptSolutions() and filterSolutions().
         */
        return rightSolutionCount.get();
    }

    @Override
    public void release() {

        if (open.compareAndSet(true/* expect */, false/* update */)) {
            // Already closed.
            return;
        }

        rightSolutionsRef.set(null);
        
    }

    @Override
    public long acceptSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats) {

        final Map<Key,Bucket> map = getRightSolutions();
        
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
        
        rightSolutionCount.add(naccepted);
        
        return naccepted;

    }

    @Override
    public long filterSolutions(ICloseableIterator<IBindingSet[]> itr,
            BOpStats stats, IBuffer<IBindingSet> sink) {

        final Map<Key, Bucket> map = getRightSolutions();
        
        final IBindingSet[] all = BOpUtility.toArray(itr, stats);

        if (log.isDebugEnabled())
            log.debug("Materialized: " + all.length
                    + " source solutions.");

        for (IBindingSet bset : all) {

            /*
             * Note: For a DISTINCT SOLUTIONS filter, we only consider the
             * variables that are being projected. Further, all variables are
             * used when computing the hash code. Therefore "joinVars" ==
             * "selectedVars" for a DISTINCT SOLUTIONS filter.
             */
            bset.copy(joinVars); // only consider the selected variables.

            /*
             * Note: Solutions are NOT dropped if a variable is not bound in a
             * given solution. The variable is simply not used when computing
             * the hash code. Specifying optional:=true here causes makeKey() to
             * have this behavior.
             */
            final Key key = makeKey(joinVars, bset, true/* optional */);

            assert key != null;

            Bucket b = map.get(key);
            
            if(b == null) {
                
                map.put(key, b = new Bucket(bset));
                
            } else {
                
                if(b.addDistinct(bset)) {
                    
                    // Write on the output sink.
                    sink.add(bset);
                    
                }
                
            }

        }

        if (log.isDebugEnabled())
            log.debug("There are : " + map.size()
                    + " distinct combinations of the join vars: "
                    + Arrays.toString(joinVars));

        final long naccepted = all.length;
        
        rightSolutionCount.add(naccepted);

        return naccepted;

    }

    @Override
    public void hashJoin(ICloseableIterator<IBindingSet> leftItr,
            IBuffer<IBindingSet> outputBuffer, boolean leftIsPipeline) {
        
        hashJoin2(leftItr, outputBuffer, leftIsPipeline, constraints);
        
    }

    /**
     * {@inheritDoc}
     * <p>
     * For each source solution materialized, the hash table is probed using the
     * as-bound join variables for that source solution. If there is a hit in
     * the hash table, then operator then outputs the cross product of the
     * source solution with the solutions list in the {@link Bucket} found under
     * that {@link Key} in the hash table, applying any optional CONSTRAINTS. A
     * join hit counter is carried for each solution in the hash index. The join
     * hit counter is used to support optional joins.
     */
    @Override
    public void hashJoin2(final ICloseableIterator<IBindingSet> leftItr,
            final IBuffer<IBindingSet> outputBuffer,
            final boolean leftIsPipeline, final IConstraint[] constraints) {

        final Map<Key,Bucket> rightSolutions = getRightSolutions();
          
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.size()
                    + ",#solutions=" + getRightSolutionCount());
        }

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

    }

    @Override
    public void outputOptionals(final IBuffer<IBindingSet> outputBuffer) {
        
        final Map<Key,Bucket> rightSolutions = getRightSolutions();
        
        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.size());
        }

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
        
    }

    @Override
    public void outputSolutions(final IBuffer<IBindingSet> out) {

        final Map<Key,Bucket> rightSolutions = getRightSolutions();

        final IVariable<?>[] selected = getSelectVars();

        if (log.isInfoEnabled()) {
            log.info("rightSolutions: #buckets=" + rightSolutions.size());
        }

        // source.
        final Iterator<Bucket> bucketIterator = rightSolutions.values()
                .iterator();

        while (bucketIterator.hasNext()) {

            final Bucket bucket = bucketIterator.next();

            for (SolutionHit solutionHit : bucket) {

                IBindingSet bset = solutionHit.solution;

                if (selected != null) {

                    // Drop variables which are not projected.
                    bset = bset.copy(selected);

                }

                out.add(bset);

            }

        }       

    }

}
