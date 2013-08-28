package com.bigdata.rdf.graph.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import com.bigdata.rdf.graph.IStaticFrontier;
import com.bigdata.rdf.internal.IV;

import cutthecrap.utils.striterators.ArrayIterator;

/**
 * Simple implementation of a "static" frontier.
 * <p>
 * Note: This implementation has package private methods that permit certain
 * kinds of mutation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @deprecated by {@link StaticFrontier2}, which is more efficient.
 */
@SuppressWarnings("rawtypes")
public class StaticFrontier implements IStaticFrontier {

    private final ArrayList<IV> vertices;

    StaticFrontier() {

        vertices = new ArrayList<IV>();

    }

    @Override
    public int size() {

        return vertices.size();

    }

    @Override
    public boolean isEmpty() {

        return vertices.isEmpty();

    }

    @Override
    public Iterator<IV> iterator() {

        return vertices.iterator();

    }

    private void ensureCapacity(final int minCapacity) {

        vertices.ensureCapacity(minCapacity);

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

    @Override
    public void resetFrontier(final int minCapacity, final boolean ordered,
            Iterator<IV> itr) {

        if (!ordered) {

            final IV[] a = new IV[minCapacity];
            
            int i=0;
            while(itr.hasNext()) {
                a[i++] = itr.next();
            }
            
            // sort.
            Arrays.sort(a, 0/* fromIndex */, i/* toIndex */);

            // iterator over the ordered vertices.
            itr = new ArrayIterator<IV>(a, 0/* fromIndex */, i/* len */);

        }
        
        // clear the old frontier.
        vertices.clear();

        // ensure enough capacity for the new frontier.
        ensureCapacity(minCapacity);

        while (itr.hasNext()) {

            final IV v = itr.next();

            vertices.add(v);

        }

    }

}