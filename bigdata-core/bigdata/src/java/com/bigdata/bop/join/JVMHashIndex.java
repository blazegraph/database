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
 * Created on Apr 29, 2013
 */
package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.solutions.JVMDistinctBindingSetsOp;
import com.bigdata.counters.CAT;

/**
 * A hash index for {@link IBindingSet}s that supports duplicate solutions and
 * hit counts. The hit counts are used to detect {@link IBindingSet}s that do
 * not join for OPTIONAL, MINUS, and related kinds of "negation" joins.
 * <p>
 * Note: The {@link JVMDistinctBindingSetsOp} does not use this class right now
 * because it enjoys better concurrency than the {@link JVMHashIndex}. Also see
 * {@link JVMDistinctFilter}, which is the backing implementation for the
 * {@link JVMDistinctBindingSetsOp}.
 * 
 * @see JVMDistinctFilter
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JVMHashIndex {

    private static final Logger log = Logger.getLogger(JVMHashIndex.class);

    /**
     * Note: If joinVars is an empty array, then the solutions will all hash to
     * ONE (1).
     */
    private static final int ONE = 1;

//    /**
//     * Return the hash code which will be used as the key given the ordered
//     * as-bound values for the join variables.
//     * 
//     * @param joinVars
//     *            The join variables.
//     * @param bset
//     *            The bindings whose as-bound hash code for the join variables
//     *            will be computed.
//     * @param ignoreUnboundVariables
//     *            If a variable without a binding should be silently ignored.
//     * 
//     * @return The hash code.
//     * 
//     * @throws JoinVariableNotBoundException
//     *             if there is no binding for a join variable.
//     */
//    private static int hashCode(final IVariable<?>[] joinVars,
//            final IBindingSet bset, final boolean ignoreUnboundVariables)
//            throws JoinVariableNotBoundException {
//
//        int h = ONE;
//
//        for (IVariable<?> v : joinVars) {
//
//            final IConstant<?> c = bset.get(v);
//
//            if (c == null) {
//
//                if (ignoreUnboundVariables)
//                    continue;
//
//                // Reject any solution which does not have a binding for a join
//                // variable.
//
//                throw new JoinVariableNotBoundException(v.getName());
//
//            }
//
//            h = 31 * h + c.hashCode();
//
//        }
//
//        if (log.isTraceEnabled())
//            log.trace("hashCode=" + h + ", joinVars="
//                    + Arrays.toString(joinVars) + " : " + bset);
//
//        return h;
//
//    }

    /**
     * Return an array of constants corresponding to the as-bound values of the
     * join variables for the given solution.
     * 
     * @param bset
     *            The solution.
     * 
     * @return The as-bound values for the {@link #keyVars} for that solution
     *         -or- <code>null</code> if one or more join variables is not bound
     *         by the solution and {@link #indexSolutionsHavingUnboundJoinVars}
     *         is <code>false</code>.
     * 
     * @see #keyVars
     * @see #indexSolutionsHavingUnboundJoinVars
     */
    private Key makeKey(//final IVariable<?>[] keyVars,
            final IBindingSet bset
//            final boolean indexSolutionsHavingUnboundJoinVars
            ) {

        final IConstant<?>[] vals = new IConstant<?>[keyVars.length];

        for (int i = 0; i < keyVars.length; i++) {

            final IVariable<?> v = keyVars[i];

            vals[i] = bset.get(v);

        }

        int h = ONE;

//        for (IVariable<?> v : keyVars) {
//
//            final IConstant<?> c = bset.get(v);

        for (int i = 0; i < keyVars.length; i++) {

            final IVariable<?> v = keyVars[i];
            
            final IConstant<?> c = vals[i];
            
            if (c == null) {

                if (!indexSolutionsHavingUnboundJoinVars) {

                    /*
                     * Drop solution having an unbound join variable.
                     */

                    if (log.isDebugEnabled())
                        log.debug("Join variable is not bound: var=" + v
                                + ", solution=" + bset);

                    return null;

                }

                continue;
                
            }

            h = 31 * h + c.hashCode();

        }

        if (log.isTraceEnabled())
            log.trace("hashCode=" + h + ", joinVars="
                    + Arrays.toString(keyVars) + " : " + bset);

        return new Key(h, vals);

    }

    /**
     * Wrapper for the keys in the hash table. This is necessary for the hash
     * table to compare the keys as equal and also provides efficiencies in the
     * hash code and equals() methods.
     */
    public static class Key {

        private final int hash;

        private final IConstant<?>[] vals;

        private Key(final int hashCode, final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = hashCode;
        }
        
        @Override
        public int hashCode() {
            return hash;
        }

        @Override
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
     * An solution and a hit counter as stored in the {@link JVMHashIndex}.
     */
    public static class SolutionHit {

        /**
         * The input solution.
         */
        final public IBindingSet solution;

        /**
         * The #of hits on that solution. This may be used to detect solutions
         * that did not join. E.g., by scanning and reporting out all solutions
         * where {@link #nhits} is ZERO (0L).
         */
        public final CAT nhits = new CAT();

        private SolutionHit(final IBindingSet solution) {

            if (solution == null)
                throw new IllegalArgumentException();

            this.solution = solution;

        }

        @Override
        public String toString() {

            return getClass().getName() + "{nhits=" + nhits + ",solution="
                    + solution + "}";

        }

    } // class SolutionHit

    /**
     * A group of solutions having the same as-bound values for the join vars.
     * Each solution is paired with a hit counter so we can support OPTIONAL
     * semantics for the join.
     */
    public static class Bucket implements Iterable<SolutionHit>,
            Comparable<Bucket> {

        /** The hash code for this collision bucket. */
        private final int hashCode;

        /**
         * A set of solutions (and their hit counters) which have the same
         * as-bound values for the join variables.
         */
        private final List<SolutionHit> solutions = new LinkedList<SolutionHit>();

        @Override
        public String toString() {
            return super.toString()
                    + //
                    "{hashCode=" + hashCode + ",#solutions=" + solutions.size()
                    + "}";
        }

        public Bucket(final int hashCode, final IBindingSet solution) {

            this.hashCode = hashCode;

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

            if (solutions.isEmpty()) {

                // First solution.
                solutions.add(new SolutionHit(solution));

                return true;

            }

            final Iterator<SolutionHit> itr = solutions.iterator();

            while (itr.hasNext()) {

                final SolutionHit aSolution = itr.next();

                if (aSolution.solution.equals(solution)) {

                    // Solution already in this bucket.
                    return false;

                }

            }

            // This is a distinct solution.
            solutions.add(new SolutionHit(solution));

            return true;

        }

        @Override
        final public Iterator<SolutionHit> iterator() {

            // return Collections.unmodifiableList(solutions).iterator();
            return solutions.iterator();

        }

        // @SuppressWarnings("unchecked")
        // public Iterator<IBindingSet> bindingSetIterator() {
        //
        // return new Striterator(solutions.iterator()).addFilter(new Resolver()
        // {
        //
        // @Override
        // protected Object resolve(Object obj) {
        // return ((SolutionHit)obj).solution;
        // }
        // });
        //
        // }

        /**
         * Orders the buckets based on their hash codes.
         */
        @Override
        final public int compareTo(final Bucket o) {
            if (hashCode > o.hashCode)
                return 1;
            if (hashCode < o.hashCode)
                return -1;
            return 0;
        }
        
        @Override
        final public int hashCode() {

            return hashCode;
            
        }

        /**
         * Return <code>true</code> iff this {@link Bucket} is empty (if there
         * are no solutions in the bucket).
         */
        final public boolean isEmpty() {

            return solutions.isEmpty();

        }

    } // Bucket

    /**
     * The join variables (required, but may be empty). The order of the entries
     * is used when forming the as-bound keys for the hash table. Duplicate
     * elements and null elements are not permitted. If no join variables are
     * specified, then the join will consider the N x M cross product, filtering
     * for solutions which join. This is very expensive when compared to a hash
     * join. Whenever possible you should identify one or more variables which
     * must be bound for the join and specify those as the join variables.
     */
    private final IVariable<?>[] keyVars;

    /**
     * When <code>true</code>, we allow solutions to be stored in the hash index
     * that have unbound variables for the {@link #keyVars}. When
     * <code>false</code>, such solutions are dropped.
     * <p>
     * Note: This must be <code>true</code> for DISTINCT, OPTIONAL, and NOT
     * EXISTS / MINUS since in each case we do not want to drop solutions
     * lacking a binding for some {@link #keyVars}. For DISTINCT, this is
     * because we want to project all solutions, regardless of unbound
     * variables. For OPTIONAL and NOT EXISTS / MINUS, this is because we must
     * index all solutions since we will report only those solutions that do not
     * join. Once all solutions that do join have been identified, the solutions
     * that do not join are identified by a scan of the hash index looking for
     * {@link SolutionHit#nhits} equals ZERO (0L).
     */
    private final boolean indexSolutionsHavingUnboundJoinVars;

    /**
     * The backing map - this is NOT thread safe.
     */
    private final Map<Key, Bucket> map;

    /**
     * @param keyVars
     *            The variables that are used to form the keys in the hash index
     *            (required, but may be empty). The order of the entries is used
     *            when forming the as-bound keys for the hash table. Duplicate
     *            elements and null elements are not permitted. If no join
     *            variables are specified, then the join will consider the N x M
     *            cross product, filtering for solutions which join. This is
     *            very expensive when compared to a hash join. Whenever possible
     *            you should identify one or more variables which must be bound
     *            for the join and specify those as the join variables.
     * @param indexSolutionsHavingUnboundJoinVars
     *            When <code>true</code>, we allow solutions to be stored in the
     *            hash index that have unbound variables for the
     *            {@link #keyVars}. When <code>false</code>, such solutions are
     *            dropped (they are not added to the index).
     * @param map
     *            The backing map. A {@link HashMap} should be faster for insert
     *            and search. A {@link LinkedHashMap} should be faster for
     *            scans. Some join patterns do not require us to use scans, in
     *            which case {@link HashMap} is the clear winner. (For example,
     *            a non-optional hash join against an access path never uses the
     *            iterator over the hash index.)
     */
    public JVMHashIndex(final IVariable<?>[] keyVars,
            final boolean indexSolutionsHavingUnboundJoinVars,
            final Map<Key, Bucket> map) {

        if (keyVars == null) {
       
            /*
             * A ZERO LENGTH joinVars[] means that all solutions will be in the
             * same hash bucket. This can arise due to poor assignment of join
             * variables or simply because there are no available join variables
             * (full cross product join). Such joins are very expensive.
             */
            
            throw new IllegalArgumentException();

        }
        
        if (map == null) {
        
            throw new IllegalArgumentException();
            
        }

        this.map = map;

        this.indexSolutionsHavingUnboundJoinVars = indexSolutionsHavingUnboundJoinVars;

        this.keyVars = keyVars;

    }

    /**
     * Add the solution to the index.
     * 
     * @param bset
     *            The {@link IBindingSet}.
     * 
     * @return The {@link Key} iff the solution was added to the index and
     *         <code>null</code> iff the solution was not added (because a
     *         {@link Key} could not be formed for the solution given the
     *         specified {@link #keyVars}).
     */
    public Key add(final IBindingSet bset) {

        final Key key = makeKey(bset);

        if (key == null) {

            // Drop solution.
            return null;

        }

        /*
         * TODO There is an opportunity for CONCURRENT hash map for at least the
         * DISTINCT SOLUTIONS filter and perhaps for others as well. However, to
         * do this with the DISTINCT SOLUTIONS filter we would have to make the
         * mutation operations on a Bucket atomic. E.g., using the synchronized
         * keyword. This would give us what amounts to per-hash code striped
         * locks.
         * 
         * Note: This pattern could be made thread safe. If the get() fails, use
         * a putIfAbsent() in a data race to create and insert the new bucket.
         * If the thread looses the data race, then it must use the other
         * thread's bucket and add its solution to that bucket.
         * 
         * The Bucket.addDistinct() could also be made thread safe by using the
         * monitor for the Bucket (or its Solutions List). This is necessary for
         * correctness, but note that we do not use addDistinct() and instead
         * rely on the more efficient JVMDistinctFilter. The JVMDistinctFilter
         * it is more efficient because it based on a ConcurrentHashMap does not
         * require any explicit synchronization.
         * 
         * TODO This change would allow us to execute the JVMHashIndexOp
         * concurrently which could provide a substantial throughput gain.
         * However, we still are faced with the requirement to decide atomically
         * when the HashIndexOp is done (the isLastPass() test). It is possible
         * to decide when no more solutions will be available. If the thread
         * that executes the last pass awaits a latch to count down to ONE, then
         * it will known that it is (a) the last invocation, and (b) that all
         * other invocations are complete. This pattern would have to be
         * supported in the QueryEngine and PipelineOp since the latch would
         * have to be incremented by the QueryEngine in a critical section when
         * the new ChunkTask is created and then decremented in a critical
         * section when the ChunkTask ends. If the latch is then exposed to the
         * BOpContext, the operator can decide that it is the last invocation
         * and that no other task is running (or will run) for that operator and
         * then execute the post-processing step (flooding the solutions in the
         * hash index to the downstream operator in the query plan). [Actually,
         * we might not have to do that for the JVMHashIndexOp since we do not
         * have to checkpoint the JVMHashIndex and could incrementally pass
         * along the indexed solutions to the downstream operator, but this
         * would also mean that outputSolutions() would need to use sharedState
         * for its DISTINCT FILTER on the solutions flowing into the sub-group.
         * All of this could be done, but it might require us to specialize the
         * JVMHashIndexOp. We would also have to update AST2BOpUtility to
         * generate the appropriate annotations.]
         */
        Bucket b = map.get(key);

        if (b == null) {

            map.put(key, b = new Bucket(key.hash, bset));

        } else {

            b.add(bset);

        }

        return key;

    }

    /**
     * Add the solution to the index iff the solution is not already present in
     * the index.
     * 
     * @param bset
     *            The solution.
     * 
     * @return <code>true</code> iff the index was modified by this operation.
     */
    public boolean addDistinct(final IBindingSet bset) {

        final Key key = makeKey(bset);

        assert key != null;

        Bucket b = map.get(key);

        if (b == null) {

            // New bucket holding just this solution.
            map.put(key, b = new Bucket(key.hash, bset));

            return true;

        } else {

            if (b.addDistinct(bset)) {

                // Existing bucket not having this solution.
                return true;

            }

            // Existing bucket with duplicate solution.
            return false;

        }

    }

    /**
     * Return the hash {@link Bucket} into which the given solution is mapped.
     * <p>
     * Note: The caller must apply an appropriate join constraint in order to
     * correctly reject solutions that (a) violate the join contract; and (b)
     * that are present in the hash bucket due to a hash collection rather than
     * because they have the same bindings for the join variables.
     * 
     * @param left
     *            The probe.
     *            
     * @return The hash {@link Bucket} into which the given solution is mapped
     *         -or- <code>null</code> if there is no such hash bucket.
     */
    public Bucket getBucket(final IBindingSet left) {

        final Key key = makeKey(left);

        if (key == null) {

            return null;

        }

        // Probe the hash map : May return [null]!
        return map.get(key);

    }

    /**
     * Visit all buckets in the hash index.
     */
    public Iterator<Bucket> buckets() {

        return map.values().iterator();

    }

    /**
     * The #of buckets in the hash index. Each bucket has a distinct hash code.
     * Hash collisions can cause solutions that are distinct in their
     * {@link #keyVars} to nevertheless be mapped into the same hash bucket.
     * 
     * @return The #of buckets in the hash index.
     */
    public int bucketCount() {

        return map.size();

    }

    /**
     * Export the {@link Bucket}s as an array.
     */
    public Bucket[] toArray() {

        // source.
        final Iterator<Bucket> bucketIterator = map.values()
                .iterator();

        final Bucket[] a = new Bucket[map.size()];

        int i = 0;

        while (bucketIterator.hasNext()) {

            a[i++] = bucketIterator.next();

        }

        return a;

    }

}
