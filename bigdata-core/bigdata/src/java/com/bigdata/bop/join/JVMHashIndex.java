/**

Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

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
import java.util.Map;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.solutions.JVMDistinctBindingSetsOp;

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
public class JVMHashIndex implements IJVMHashIndex {

    private static final Logger log = Logger.getLogger(JVMHashIndex.class);

    /**
     * Note: If joinVars is an empty array, then the solutions will all hash to
     * ONE (1).
     */
    private static final int ONE = 1;

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
    public static Key makeKey(
          final IVariable<?>[] keyVars,
          final IBindingSet bset,
          final boolean indexSolutionsHavingUnboundJoinVars) {

        final IConstant<?>[] vals = new IConstant<?>[keyVars.length];

        for (int i = 0; i < keyVars.length; i++) {

            final IVariable<?> v = keyVars[i];

            vals[i] = bset.get(v);

        }

        int h = ONE;

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

    @Override
    public Key add(final IBindingSet bset) {

        final Key key = makeKey(keyVars, bset, indexSolutionsHavingUnboundJoinVars);

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

    @Override
    public boolean addDistinct(final IBindingSet bset) {

        final Key key = makeKey(keyVars, bset, indexSolutionsHavingUnboundJoinVars);

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

    @Override
    public Bucket getBucket(final IBindingSet left) {

        final Key key = makeKey(keyVars, left, indexSolutionsHavingUnboundJoinVars);

        if (key == null) {

            return null;

        }

        // Probe the hash map : May return [null]!
        return map.get(key);

    }

    @Override
    public Iterator<Bucket> buckets() {

        return map.values().iterator();

    }

    @Override
    public int bucketCount() {

        return map.size();

    }

    @Override
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
