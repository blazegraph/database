package com.bigdata.btree.filter;

import java.util.Iterator;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleCursor;
import com.bigdata.btree.ITupleIterator;


/**
 * Filter allows mutation of the visited {@link ITuple}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <E>
 */
abstract public class TupleUpdater<E> extends TupleFilter<E> {

    private static final long serialVersionUID = 8825811070321638652L;

    public TupleUpdater() {

        super();

    }

    public TupleUpdater(Object state) {

        super(state);

    }

    @SuppressWarnings("unchecked")
    public ITupleIterator<E> filter(Iterator src) {

        return new Updaterator((ITupleCursor<E>) src);

    }

    /**
     * You may implement this method to update the state of the visited
     * tuple in the backing index.
     * <p>
     * Note: If you modify <i>tuple</i> then that modification will be
     * visitible to the consumer of the iterator.
     * 
     * @param ndx
     *            The index on which the {@link ITupleCursor} is reading.
     * @param tuple
     *            The tuple that is being visited.
     */
    abstract protected void update(IIndex ndx, ITuple<E> tuple);

    protected class Updaterator extends TupleFilter.Filterator {

        public Updaterator(ITupleIterator<E> src) {

            super(src);

        }

        protected void visit(ITuple tuple) {

            final IIndex ndx = ((ITupleCursor) src).getIndex();

            update(ndx, tuple);

        }

    }

}
