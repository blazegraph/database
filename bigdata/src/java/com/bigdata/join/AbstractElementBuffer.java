package com.bigdata.join;


/**
 * Base class for {@link IBuffer} of {@link IRelation} elements whose target is
 * a mutation (insert, delete, or update) of some {@link IMutableRelation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @param <R>
 *            The generic type of the [R]elation elements.
 * 
 * @see IMutableRelation
 */
abstract public class AbstractElementBuffer<R> extends AbstractArrayBuffer<R> {

    private final IMutableRelation<R> relation;

    private final IKeyOrder<R> keyOrder;
    
    protected IMutableRelation<R> getRelation() {
        
        return relation;
        
    }
    
    /**
     * The natural order in which the elements will appear in the buffer -or-
     * <code>null</code> if you do not have a <em>strong</em> guarentee for
     * that order (from the ctor).
     */
    protected IKeyOrder<R> getKeyOrder() {
        
        return keyOrder;
    }
    
    /**
     * @param capacity
     *            The buffer capacity.
     * @param relation
     *            The target relation.
     * @param keyOrder
     *            The natural order in which the elements will appear in the
     *            buffer -or- <code>null</code> if you do not have a
     *            <em>strong</em> guarentee for that order.
     */
    protected AbstractElementBuffer(int capacity,
            IMutableRelation<R> relation, IKeyOrder<R> keyOrder) {

        super(capacity);

        if (relation == null)
            throw new IllegalArgumentException();

        this.relation = relation;

        this.keyOrder = keyOrder; // MAY be null.
        
    }

    /**
     * Delegates to {@link #flush(IChunkedIterator)}.
     */
    @Override
    final protected long flush(int n, R[] a) {

        final IChunkedOrderedIterator<R> itr = new ChunkedArrayIterator<R>(n,
                a, null/* keyOrder(unknown) */);

        return flush(itr);
        
    }

    /**
     * Concrete implementations must process the elements, causing the
     * appropriate mutation on the target {@link IRelation}.
     * <p>
     * Note: The elements generally appear in an arbitrary order and need to be
     * sorted into ordered chunks using {@link IKeyOrder#getComparator()} for
     * the desired natural order(s).
     * 
     * @param itr
     * 
     * @return The #of elements that were modified in the backing relation when
     *         the buffer was flushed
     */
    abstract protected long flush(IChunkedOrderedIterator<R> itr);
    
    /**
     * Buffer writes on {@link IMutableRelation#insert(IChunkedOrderedIterator)}
     * when it is {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <R>
     */
    public static class InsertBuffer<R> extends AbstractElementBuffer<R> {

        /**
         * Ctor variant when the element will be written into the buffer in an
         * unknown order.
         * 
         * @param capacity
         * @param relation
         */
        public InsertBuffer(int capacity, IMutableRelation<R> relation) {

            this(capacity, relation,null/*keyOrder*/);

        }

        /**
         * Ctor variant used when you have a <em>strong</em> guarantee of the
         * order in which the elements will be written into the buffer.
         * 
         * @param capacity
         * @param relation
         * @param keyOrder
         */
        public InsertBuffer(int capacity, IMutableRelation<R> relation, IKeyOrder<R> keyOrder) {

            super(capacity, relation, keyOrder);

        }

        @Override
        protected long flush(IChunkedOrderedIterator<R> itr) {

            return getRelation().insert(itr);
            
        }
        
    }
    
    /**
     * Buffer writes on {@link IMutableRelation#delete(IChunkedOrderedIterator)}
     * when it is {@link #flush() flushed}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <R>
     */
    public static class DeleteBuffer<R> extends AbstractElementBuffer<R> {

        /**
         * Ctor variant when the element will be written into the buffer in an
         * unknown order.
         * 
         * @param capacity
         * @param relation
         */
        protected DeleteBuffer(int capacity, IMutableRelation<R> relation) {

            this(capacity,relation,null/*keyOrder*/);

        }

        /**
         * Ctor variant used when you have a <em>strong</em> guarantee of the
         * order in which the elements will be written into the buffer.
         * 
         * @param capacity
         * @param relation
         * @param keyOrder
         */
        protected DeleteBuffer(int capacity, IMutableRelation<R> relation,IKeyOrder<R> keyOrder) {

            super(capacity,relation,keyOrder);

        }
        
        @Override
        protected long flush(IChunkedOrderedIterator<R> itr) {
            
            return getRelation().delete(itr);
            
        }
        
    }
    
//    /**
//     * Buffer writes on
//     * {@link IMutableRelation#update(IChunkedIterator, ITransform)} when it is
//     * {@link #flush() flushed}.
//     * 
//     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
//     * @version $Id$
//     * @param <R>
//     */
//    public static class UpdateBuffer<R> extends AbstractElementBuffer<R> {
//
//        private final ITransform<R> transform;
//        
//        /**
//         * Ctor variant when the element will be written into the buffer in an
//         * unknown order.
//         * 
//         * @param capacity
//         * @param relation
//         * @param transform
//         */
//        protected UpdateBuffer(int capacity, IMutableRelation<R> relation, ITransform<R> transform) {
//
//            this(capacity, relation, transform, null/*keyOrder*/);
//            
//        }
//
//        /**
//         * Ctor variant used when you have a <em>strong</em> guarantee of the
//         * order in which the elements will be written into the buffer.
//         * 
//         * @param capacity
//         * @param relation
//         * @param transform
//         * @parma keyOrder
//         */
//        protected UpdateBuffer(int capacity, IMutableRelation<R> relation,
//                ITransform<R> transform, IKeyOrder<R> keyOrder) {
//
//            super(capacity, relation, keyOrder);
//
//            if (transform == null)
//                throw new IllegalArgumentException();
//
//            this.transform = transform;
//            
//        }
//
//        @Override
//        protected long flush(IChunkedOrderedIterator<R> itr) {
//
//            return getRelation().update(itr, transform);
//
//        }
//
//    }

}
