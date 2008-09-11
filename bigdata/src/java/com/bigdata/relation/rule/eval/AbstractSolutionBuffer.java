package com.bigdata.relation.rule.eval;

import com.bigdata.relation.IMutableRelation;
import com.bigdata.relation.IRelation;
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.relation.accesspath.ChunkConsumerIterator;
import com.bigdata.relation.accesspath.IBuffer;
import com.bigdata.striterator.ChunkedResolvingIterator;
import com.bigdata.striterator.IChunkedIterator;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.ICloseableIterator;


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
        AbstractArrayBuffer<ISolution<R>[]> {

    private final IMutableRelation<R> relation;
    
    protected IMutableRelation<R> getRelation() {
        
        return relation;
        
    }

    /**
     * @param capacity
     */
    protected AbstractSolutionBuffer(int capacity, IMutableRelation<R> relation) {

        super(capacity, null/*filter*/);

        if (relation == null)
            throw new IllegalArgumentException();

        this.relation = relation;

    }

    /**
     * Delegates to {@link #flush(IChunkedOrderedIterator)}.
     */
    @Override
    final protected long flush(int n, ISolution<R>[][] a) {

        final IChunkedOrderedIterator<ISolution<R>> itr = new ChunkConsumerIterator<ISolution<R>>(
                new ArrayIterator<ISolution<R>[]>(n, a), null/* keyOrder(unknown) */);

        return flush(itr);
        
    }
    
    /**
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    private static class ArrayIterator<E> implements ICloseableIterator<E> {

        private final int n;
        private final E[] a;
        
        private int i = 0;
        
        public ArrayIterator(int n, E[] a) {
            
            this.n = n;
            
            this.a = a;
            
        }
        
        public void close() {
            // NOP.
        }

        public boolean hasNext() {
            return i < n;
        }

        public E next() {
            return a[i++];
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
        
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
        public InsertSolutionBuffer(int capacity, IMutableRelation<E> relation) {

            super(capacity, relation);

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
        public DeleteSolutionBuffer(int capacity, IMutableRelation<E> relation) {

            super(capacity,relation);

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
