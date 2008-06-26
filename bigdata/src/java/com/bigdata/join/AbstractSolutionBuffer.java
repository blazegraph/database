package com.bigdata.join;

import com.bigdata.join.IMutableRelation.ITransform;

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
            IMutableRelation<R> relation) {

        super(capacity);

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
    public static class InsertBuffer<E> extends AbstractSolutionBuffer<E> {

        /**
         * @param capacity
         * @param relation
         */
        protected InsertBuffer(int capacity, IMutableRelation<E> relation) {

            super(capacity,relation);

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

            /*
             * FIXME For the RDF DB, also write the solutions on the optional
             * justifications index (if it is defined).
             */
            return getRelation().insert(itr2);
            
        }
        
    }
    
    /**
     * Buffer writes on {@link IMutableRelation#remove(IChunkedOrderedIterator)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class RemoveBuffer<E> extends AbstractSolutionBuffer<E> {

        /**
         * @param capacity
         * @param relation
         */
        protected RemoveBuffer(int capacity, IMutableRelation<E> relation) {

            super(capacity,relation);

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
            
            return getRelation().remove( itr2 );
            
        }
        
    }
    
    /**
     * Buffer writes on
     * {@link IMutableRelation#update(IChunkedOrderedIterator, ITransform)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class UpdateBuffer<E> extends AbstractSolutionBuffer<E> {

        private final ITransform<E> transform;
        
        /**
         * @param capacity
         * @param relation
         */
        protected UpdateBuffer(int capacity, IMutableRelation<E> relation, ITransform<E> transform) {

            super(capacity, relation);

            if (transform == null)
                throw new IllegalArgumentException();

            this.transform = transform;
            
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
            
            return getRelation().update(itr2, transform);

        }

    }

}
