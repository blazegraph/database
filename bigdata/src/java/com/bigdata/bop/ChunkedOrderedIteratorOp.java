package com.bigdata.bop;

import com.bigdata.relation.accesspath.IAccessPath;
import com.bigdata.striterator.IChunkedOrderedIterator;

/**
 * Interface for evaluating operations producing chunks of elements (tuples
 * materialized from some index of a relation).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 * @see IAccessPath
 * @see IChunkedOrderedIterator
 */
public interface ChunkedOrderedIteratorOp<E> extends BOp {

    /**
     * Well known annotations.
     */
    public interface Annotations extends BOp.Annotations, BufferAnnotations {


    }

}
