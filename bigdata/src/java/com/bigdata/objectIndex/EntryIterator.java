package com.bigdata.objectIndex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Visits the {@link Entry}s of a {@link Leaf} in the external key ordering.
 * There is exactly one entry per key for a leaf node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class EntryIterator implements Iterator<Entry> {

    private final Leaf leaf;

    private int index = 0;

    public EntryIterator(Leaf leaf) {

        assert leaf != null;

        this.leaf = leaf;

    }

    public boolean hasNext() {

        return index < leaf.nkeys;

    }

    public Entry next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        return leaf.values[index++];
        
    }

    public void remove() {

        throw new UnsupportedOperationException();

    }

}