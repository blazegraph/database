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
abstract public class AbstractMutableRelationArrayBuffer<R> extends
        AbstractArrayBuffer<ISolution<R>> {

    private final IMutableRelation<R> relation;

    protected IMutableRelation<R> getRelation() {
        
        return relation;
        
    }
    
    /**
     * @param capacity
     */
    protected AbstractMutableRelationArrayBuffer(int capacity,
            IMutableRelation<R> relation) {

        super(capacity);

        if (relation == null)
            throw new IllegalArgumentException();

        this.relation = relation;

    }

    /**
     * Delegates to {@link #flush(IChunkedIterator)}.
     */
    @Override
    final protected long flush(int n, ISolution<R>[] a) {

        final IChunkedIterator<ISolution<R>> itr = new ChunkedArrayIterator<ISolution<R>>(
                a, n, null/* keyOrder(unknown) */);

        return flush(itr);
        
    }

    /**
     * Concrete implementations must process the {@link ISolution}s, causing
     * the appropriate mutation on the target {@link IRelation}.
     * <p>
     * Note: The {@link ISolution}s generally appear in an arbitrary order and
     * need to be sorted into ordered chunks using a {@link SolutionComparator}.
     * 
     * @param itr
     * 
     * @return The #of elements that were modified in the backing relation when
     *         the buffer was flushed
     */
    abstract protected long flush(IChunkedIterator<ISolution<R>> itr);
    
    /**
     * Buffer writes on {@link IMutableRelation#insert(IChunkedIterator)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class InsertBuffer<E> extends AbstractMutableRelationArrayBuffer<E> {

        /**
         * @param capacity
         * @param relation
         */
        protected InsertBuffer(int capacity, IMutableRelation<E> relation) {

            super(capacity,relation);

        }

        @Override
        protected long flush(IChunkedIterator<ISolution<E>> itr) {
            
            return getRelation().insert(itr);
            
        }
        
    }
    
    /**
     * Buffer writes on {@link IMutableRelation#remove(IChunkedIterator)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class RemoveBuffer<E> extends AbstractMutableRelationArrayBuffer<E> {

        /**
         * @param capacity
         * @param relation
         */
        protected RemoveBuffer(int capacity, IMutableRelation<E> relation) {

            super(capacity,relation);

        }

        @Override
        protected long flush(IChunkedIterator<ISolution<E>> itr) {
            
            return getRelation().remove(itr);
            
        }
        
    }
    
    /**
     * Buffer writes on
     * {@link IMutableRelation#update(IChunkedIterator, ITransform)} when it is
     * {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <E>
     */
    public static class UpdateBuffer<E> extends AbstractMutableRelationArrayBuffer<E> {

        private final ITransform transform;
        
        /**
         * @param capacity
         * @param relation
         */
        protected UpdateBuffer(int capacity, IMutableRelation<E> relation, ITransform transform) {

            super(capacity, relation);

            if (transform == null)
                throw new IllegalArgumentException();

            this.transform = transform;
            
        }

        @Override
        protected long flush(IChunkedIterator<ISolution<E>> itr) {

            return getRelation().update(itr, transform);

        }

    }

}
