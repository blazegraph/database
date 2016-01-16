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
 * Created on Apr 26, 2013
 */
package com.bigdata.bop.join;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.bindingSet.ListBindingSet;
import com.bigdata.bop.engine.BOpStats;
import com.bigdata.relation.accesspath.IBuffer;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * Utility class for imposing a DISTINCT filter on {@link IBindingSet}. This
 * class is thread-safe. It is based on a {@link ConcurrentHashMap}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class JVMDistinctFilter implements IDistinctFilter {
    
    private static final Logger log = Logger.getLogger(JVMDistinctFilter.class);

    /**
     * Wrapper used for the as bound solutions in the {@link ConcurrentHashMap}.
     */
    private static class Solution {

        private final int hash;

        private final IConstant<?>[] vals;

        public Solution(final IConstant<?>[] vals) {
            this.vals = vals;
            this.hash = java.util.Arrays.hashCode(vals);
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Solution)) {
                return false;
            }
            final Solution t = (Solution) o;
            if (vals.length != t.vals.length)
                return false;
            for (int i = 0; i < vals.length; i++) {
                // @todo verify that this allows for nulls with a unit test.
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
     * The variables used to impose a distinct constraint.
     */
    private final IVariable<?>[] vars;

    /**
     * A concurrent map whose keys are the bindings on the specified variables
     * (the keys and the values are the same since the map implementation does
     * not allow <code>null</code> values).
     * <p>
     * Note: The map is shared state and can not be discarded or cleared until
     * the last invocation!!!
     */
    private final ConcurrentHashMap<Solution, Solution> map;

    /**
     * 
     * @param vars
     *            The set of variables on which the DISTINCT filter will be
     *            imposed. Only these variables will be present in the
     *            "accepted" solutions. Any variable bindings not specified in
     *            this array will be dropped.
     * @param initialCapacity
     * @param loadFactor
     * @param concurrencyLevel
     */
    public JVMDistinctFilter(final IVariable<?>[] vars,
            final int initialCapacity, final float loadFactor,
            final int concurrencyLevel) {

        if (vars == null)
            throw new IllegalArgumentException();

        this.vars = vars;

        this.map = new ConcurrentHashMap<Solution, Solution>(initialCapacity,
                loadFactor, concurrencyLevel);

    }

    /* (non-Javadoc)
     * @see com.bigdata.bop.join.IDistinctFilter#clear()
     */
    @Override
    public void release() {

        map.clear();

    }

    @Override
    public IVariable<?>[] getProjectedVars() {

        return vars;
        
    }

    /**
     * If the bindings are distinct for the configured variables then return
     * those bindings.
     * 
     * @param bset
     *            The binding set to be filtered.
     * 
     * @return The distinct as bound values -or- <code>null</code> if the
     *         binding set duplicates a solution which was already accepted.
     */
    private IConstant<?>[] _accept(final IBindingSet bset) {

        final IConstant<?>[] r = new IConstant<?>[vars.length];

        for (int i = 0; i < vars.length; i++) {

            /*
             * Note: This allows null's.
             * 
             * @todo write a unit test when some variables are not bound.
             */
            r[i] = bset.get(vars[i]);

        }

        final Solution s = new Solution(r);

        if (log.isTraceEnabled())
            log.trace("considering: " + Arrays.toString(r));

        final boolean distinct = map.putIfAbsent(s, s) == null;

        if (distinct && log.isDebugEnabled())
            log.debug("accepted: " + Arrays.toString(r));

        return distinct ? r : null;

    }

    /* (non-Javadoc)
     * @see com.bigdata.bop.join.IDistinctFilter#accept(com.bigdata.bop.IBindingSet)
     */
    @Override
    public IBindingSet accept(final IBindingSet bset) {

        final IConstant<?>[] vals = _accept(bset);

        if (vals == null) {

            /*
             * This is a duplicate solution.
             */

            return null;

        }

        /*
         * This is a distinct solution. Copy only the variables used to select
         * distinct solutions into a new binding set and add that to the set of
         * [accepted] binding sets which will be emitted by this operator.
         */

        final ListBindingSet tmp = new ListBindingSet();

        for (int i = 0; i < vars.length; i++) {

            if (vals[i] != null)
                tmp.set(vars[i], vals[i]);

        }

        return tmp;

    }

    @Override
    public long filterSolutions(final ICloseableIterator<IBindingSet[]> itr,
            final BOpStats stats, final IBuffer<IBindingSet> sink) {

        long n = 0L;

        while (itr.hasNext()) {

            final IBindingSet[] a = itr.next();

            stats.chunksIn.increment();
            stats.unitsIn.add(a.length);

            for (IBindingSet bset : a) {

                /*
                 * Test to see if this solution is distinct from those already
                 * seen.
                 */
                if ((bset = accept(bset)) == null) {

                    // Drop duplicate solution.
                    continue;
                }

                /*
                 * This is a distinct solution.
                 */

                sink.add(bset);

                n++;

            }

        } // next chunk.

        return n;

    }

}
