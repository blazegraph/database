package com.bigdata.btree.filter;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;

/**
 * Return an iterator that traverses the tuples in the reverse of the
 * natural index order. The iterator is backed by the {@link ITupleCursor}
 * and operations on the iterator effect the state of the cursor and visa
 * versa.
 * <p>
 * Note: This implements the full {@link ITupleCursor} API so that we can
 * stack filters over this class as readily as over an {@link ITupleCursor}.
 */
public class Reverserator<E> implements ITupleCursor<E> {

    private final ITupleCursor<E> src;

    public Reverserator(ITupleCursor<E> src) {

        if (src == null)
            throw new IllegalArgumentException();

        this.src = src;

    }

    public ITuple<E> next() {

        return src.prior();

    }

    public boolean hasNext() {

        return src.hasPrior();

    }

    public void remove() {

        src.remove();

    }

    public byte[] currentKey() {
        return src.currentKey();
    }

    public ITuple<E> first() {
        return src.last();
    }

    public byte[] getFromKey() {
        return src.getFromKey();
    }

    public IIndex getIndex() {
        return src.getIndex();
    }

    public byte[] getToKey() {
        return src.getToKey();
    }

    public boolean hasPrior() {
        return src.hasNext();
    }

    public boolean isCursorPositionDefined() {
        return src.isCursorPositionDefined();
    }

    public boolean isDeletedTupleVisitor() {
        return src.isDeletedTupleVisitor();
    }

    public ITuple<E> last() {
        return src.first();
    }

    public ITuple<E> nextTuple() {
        return src.priorTuple();
    }

    public ITuple<E> prior() {
        return src.next();
    }

    public ITuple<E> priorTuple() {
        return src.nextTuple();
    }

    public ITuple<E> seek(byte[] key) {
        return src.seek(key);
    }

    public ITuple<E> seek(Object key) {
        return src.seek(key);
    }

    public ITuple<E> tuple() {
        return src.tuple();
    }

}