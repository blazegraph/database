package com.bigdata.relation.rule.eval;

import java.util.Iterator;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.striterator.ChunkedOrderedStriterator;
import com.bigdata.striterator.ChunkedResolvingIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;


/**
 * Base class for {@link IBuffer} whose target is a mutation (insert, delete, or
 * update) of some {@link IMutableRelation}.
 * 
 * <strong>Note that this buffers {@link ISolution}s NOT relation elements.</strong>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R>
 *            The generic type of the [R]elation elements.
 * 
 * @see IMutableRelation
 */
abstract public class AbstractSolutionBuffer<R> extends
        AbstractArrayBuffer<ISolution<R>> {

    private final IMutableRelation<R> relation;
    
    protected IMutableRelation<R> getRelation() {
        
        return relation;
        
    }

    /**
     * @param capacity
     */
    protected AbstractSolutionBuffer(int capacity,
            IMutableRelation<R> relation, IElementFilter<ISolution<R>> filter) {

        super(capacity, filter);

        if (relation == null)
            throw new IllegalArgumentException();

        this.relation = relation;

    }

    /**
     * Delegates to {@link #flush(IChunkedOrderedIterator)}.
     */
    @Override
    final protected long flush(int n, ISolution<R>[] a) {

        final IChunkedOrderedIterator<ISolution<R>> itr = new ChunkedArrayIterator<ISolution<R>>(
                n, a, null/* keyOrder(unknown) */);

        return flush(itr);
        
    }

    /**
     * Concrete implementations must process the {@link ISolution}s, causing
     * the appropriate mutation on the target {@link IRelation}.
     * <p>
     * Note: The {@link ISolution}s generally appear in an arbitrary order.
     * They can either be resolved to {@link IRelation} elements or be sorted
     * into ordered {@link ISolution} chunks using a {@link SolutionComparator}.
     * 
     * @param itr
     *            The iterator (the order of the elements is unknown).
     * 
     * @return The #of elements that were modified in the backing relation when
     *         the buffer was flushed
     */
    abstract protected long flush(IChunkedOrderedIterator<ISolution<R>> itr);
    
    /**
     * Buffer writes on {@link IMutableRelation#insert(IChunkedIterator)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class InsertSolutionBuffer<E> extends AbstractSolutionBuffer<E> {

        /**
         * @param capacity
         * @param relation
         */
        public InsertSolutionBuffer(int capacity, IMutableRelation<E> relation,
                IElementFilter<ISolution<E>> filter) {

            super(capacity, relation, filter);

        }

        @Override
        protected long flush(IChunkedOrderedIterator<ISolution<E>> itr) {
            
            final IChunkedOrderedIterator<E> itr2 = new ChunkedResolvingIterator<E, ISolution<E>>(
                    itr) {

                @Override
                protected E resolve(ISolution<E> e) {

                    return e.get();

                }

            };

            return getRelation().insert(itr2);
            
        }
        
    }
    
    /**
     * Buffer writes on {@link IMutableRelation#delete(IChunkedOrderedIterator)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class DeleteSolutionBuffer<E> extends AbstractSolutionBuffer<E> {

        /**
         * @param capacity
         * @param relation
         */
        public DeleteSolutionBuffer(int capacity, IMutableRelation<E> relation,
                IElementFilter<ISolution<E>> filter) {

            super(capacity,relation, filter);

        }

        @Override
        protected long flush(IChunkedOrderedIterator<ISolution<E>> itr) {

//            final IChunkedOrderedIterator<E> itr2 = new ChunkedOrderedStriterator(
//                    itr)
//                    .addFilter(new com.bigdata.striterator.Resolver<Iterator<ISolution<E>>, ISolution<E>, E>() {
//
//                        protected E resolve(ISolution<E> e) {
//
//                            return e.get();
//
//                        }
//                    });
            
            final IChunkedOrderedIterator<E> itr2 = new ChunkedResolvingIterator<E, ISolution<E>>(
                    itr) {

                @Override
                protected E resolve(ISolution<E> e) {

                    return e.get();

                }

            };
            
            return getRelation().delete( itr2 );
            
        }
        
    }
    
//    /**
//     * Buffer writes on
//     * {@link IMutableRelation#update(IChunkedOrderedIterator, ITransform)} when it is
//     * {@link #flush() flushed}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <E>
//     */
//    public static class UpdateSolutionBuffer<E> extends
//            AbstractSolutionBuffer<E> {
//
//        private final ITransform<E> transform;
//
//        /**
//         * @param capacity
//         * @param relation
//         */
//        public UpdateSolutionBuffer(int capacity, IMutableRelation<E> relation,
//                ITransform<E> transform) {
//
//            super(capacity, relation);
//
//            if (transform == null)
//                throw new IllegalArgumentException();
//
//            this.transform = transform;
//
//        }
//
//        @Override
//        protected long flush(IChunkedOrderedIterator<ISolution<E>> itr) {
//
//            final IChunkedOrderedIterator<E> itr2 = new ChunkedResolvingIterator<E, ISolution<E>>(
//                    itr) {
//
//                @Override
//                protected E resolve(ISolution<E> e) {
//
//                    return e.get();
//
//                }
//
//            };
//
//            return getRelation().update(itr2, transform);
//
//        }
//
//    }

}
