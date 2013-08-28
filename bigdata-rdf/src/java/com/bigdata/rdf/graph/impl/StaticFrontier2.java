package com.bigdata.rdf.graph.impl;

import java.util.Arrays;
import java.util.Iterator;

import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.graph.impl.util.IArraySlice;
import com.bigdata.rdf.graph.impl.util.IManagedArray;
import com.bigdata.rdf.graph.impl.util.ManagedArray;
import com.bigdata.rdf.internal.IV;

/**
 * An implementation of a "static" frontier that grows and reuses the backing
 * vertex array.
 * <p>
 * Note: This implementation has package private methods that permit certain
 * kinds of mutation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("rawtypes")
public class StaticFrontier2 implements IStaticFrontier {

    /**
     * The backing structure.
     */
    private final IManagedArray<IV> backing;
    
    /**
     * A slice onto the {@link #backing} structure for the current frontier.
     * This gets replaced when the frontier is changed.
     */
    private IArraySlice<IV> vertices;

    StaticFrontier2() {

        /*
         * The managed backing array.
         */
        backing = new ManagedArray<IV>(IV.class);

        /*
         * Initialize with an empty slice. The backing [] will grow as
         * necessary.
         */
        vertices = backing.slice(0/* off */, 0/* len */);

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
    public Iterator<IV> iterator() {

        return vertices.iterator();

    }

    // private void clear() {
    //
    // vertices.clear();
    //
    // }

    // private void schedule(IV v) {
    //
    // vertices.add(v);
    //
    // }

    /**
     * Setup the same static frontier object for the new compact fronter (it is
     * reused in each round).
     */
    @Override
    public void resetFrontier(final int minCapacity, final boolean ordered,
            final Iterator<IV> itr) {

        // ensure enough capacity for the new frontier.
        backing.ensureCapacity(minCapacity);

        // the actual backing array. should not changed since pre-extended.
        final IV[] a = backing.array();

        int nvertices = 0;

        while (itr.hasNext()) {

            final IV v = itr.next();

            a[nvertices++] = v;

        }

        if (!ordered) {

            // Sort.
            Arrays.sort(a, 0/* fromIndex */, nvertices/* toIndex */);
            
        }
        
        // take a slice of the backing showing only the valid entries.
        final IArraySlice<IV> tmp = backing.slice(0/* off */, nvertices);

        // update the view.
        this.vertices = tmp;

    }

}