package com.bigdata.rdf.graph.impl;

import java.util.Arrays;
import java.util.Iterator;

import org.openrdf.model.Value;

import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.util.IArraySlice;
import com.bigdata.rdf.graph.impl.util.IManagedArray;
import com.bigdata.rdf.graph.impl.util.ManagedArray;

/**
 * An implementation of a "static" frontier that grows and reuses the backing
 * vertex array.
 * <p>
 * Note: This implementation has package private methods that permit certain
 * kinds of mutation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class StaticFrontier2 implements IStaticFrontier {

    /**
     * The backing structure.
     */
    private final IManagedArray<Value> backing;
    
    /**
     * A slice onto the {@link #backing} structure for the current frontier.
     * This gets replaced when the frontier is changed.
     */
    private IArraySlice<Value> vertices;

    private boolean compact = true;
    
    StaticFrontier2() {

        /*
         * The managed backing array.
         */
        backing = new ManagedArray<Value>(Value.class);

        /*
         * Initialize with an empty slice. The backing [] will grow as
         * necessary.
         */
        vertices = backing.slice(0/* off */, 0/* len */);

    }

    @Override
    public boolean isCompact() {

        return compact;
        
    }
    
    public void setCompact(final boolean newValue) {
        
        this.compact = newValue;
        
    }
    
    @Override
    public int size() {

        return vertices.len();

    }

    @Override
    public boolean isEmpty() {

        return vertices.len() == 0;

    }

    @Override
    public Iterator<Value> iterator() {

        return vertices.iterator();

    }

    /**
     * Grow the backing array iff necessary. Regardless, the entries from end of
     * the current view to the first non-<code>null</code> are cleared. This is
     * done to faciltiate GC by clearing references that would otherwise remain
     * if/when the frontier contracted.
     * 
     * @param minCapacity
     *            The required minimum capacity.
     */
    public void resetAndEnsureCapacity(final int minCapacity) {

        final int len0 = size();

        backing.ensureCapacity(minCapacity);

        final int len1 = backing.len();

        if (len1 > len0) {

            final Value[] a = backing.array();

            for (int i = len0; i < len1; i++) {

                if (a[i] == null)
                    break;

                a[i] = null;

            }

        }

        /*
         * Replace the view. The caller will need to copy the data into the
         * backing array before it will appear in the new view.
         */
        this.vertices = backing.slice(0/* off */, minCapacity/* len */);
        
    }
    
    /**
     * Copy a slice into the backing array. This method is intended for use by
     * parallel threads. The backing array MUST have sufficient capacity. The
     * threads MUST write to offsets that are known to not overlap. NO checking
     * is done to ensure that the concurrent copy of these slices will not
     * overlap.
     * 
     * @param off
     *            The offset at which to copy the slice.
     * @param slice
     *            The slice.
     */
    public void copyIntoResetFrontier(final int off,
            final IArraySlice<Value> slice) {

        backing.put(off/* dstoff */, slice.array()/* src */,
                slice.off()/* srcoff */, slice.len()/* srclen */);

    }
    
    /**
     * Setup the same static frontier object for the new compact fronter (it is
     * reused in each round).
     */
    @Override
    public void resetFrontier(final int minCapacity, final boolean ordered,
            final Iterator<Value> itr) {

        copyScheduleIntoFrontier(minCapacity, itr);

        if (!ordered) {

            /*
             * Sort the current slice of the backing array.
             */

            Arrays.sort(backing.array(), 0/* fromIndex */, vertices.len()/* toIndex */);

        }

    }

    /**
     * Copy the data from the iterator into the backing array and update the
     * slice which provides our exposed view of the backing array.
     * 
     * @param minCapacity
     *            The minimum required capacity for the backing array.
     * @param itr
     *            The source from which we will repopulate the backing array.
     */
    private void copyScheduleIntoFrontier(final int minCapacity,
            final Iterator<Value> itr) {

        // ensure enough capacity for the new frontier.
        backing.ensureCapacity(minCapacity);

        // the actual backing array. should not changed since pre-extended.
        final Value[] a = backing.array();

        int nvertices = 0;

        while (itr.hasNext()) {

            final Value v = itr.next();

            a[nvertices++] = v;

        }

        /*
         * Null fill until the end of the last frontier. That will help out GC.
         * Otherwise those IV references are pinned and can hang around. We
         * could track the high water mark on the backing array for this
         * purpose.
         */
        for (int i = nvertices; i < a.length; i++) {
            if (a[i] == null)
                break;
            a[i] = null;
        }

        /*
         * Take a slice of the backing showing only the valid entries and use it
         * to replace the view of the backing array.
         */
        this.vertices = backing.slice(0/* off */, nvertices);

    }

    @Override
    public String toString() {

        return getClass().getName() + "{size=" + size() + ",compact="
                + isCompact() + ",capacity=" + backing.capacity() + "}";
        
    }
    
}