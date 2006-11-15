package com.bigdata.objectIndex;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Visits the direct children of a node in the external key ordering.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
class ChildIterator implements Iterator<AbstractNode> {

    private final Node node;

    private int index = 0;

    public ChildIterator(Node node) {

        assert node != null;

        this.node = node;

    }

    public boolean hasNext() {

        // Note: nchildren == nkeys+1 for a Node.
        return index <= node.nkeys;

    }

    public AbstractNode next() {

        if (!hasNext()) {

            throw new NoSuchElementException();

        }

        return node.getChild(index++);
    }

    public void remove() {

        throw new UnsupportedOperationException();

    }

}