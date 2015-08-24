package com.bigdata.relation.accesspath;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.Tuple;

import cutthecrap.utils.striterators.Resolver;

/**
 * Resolve an {@link ITuple} to its {@link Tuple#getObject()}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <R>
 *            The generic type of the resolved object.
 */
public class TupleObjectResolver<R> extends Resolver {

    /**
     * 
     */
    private static final long serialVersionUID = -6892381461832033057L;

    public TupleObjectResolver() {

    }

    /**
     * Resolve tuple to element type.
     */
    @SuppressWarnings("unchecked")
    @Override
    protected R resolve(final Object arg0) {

        final ITuple tuple = (ITuple) arg0;

        return (R) tuple.getObject();

    }

}
