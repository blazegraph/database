package com.bigdata.bop.ap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpBase;
import com.bigdata.bop.BOpContext;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.spo.DistinctSPOIterator;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.DistinctFilter;
import com.bigdata.striterator.IChunkConverter;
import com.bigdata.striterator.MergeFilter;

/**
 * A DISTINCT operator based for elements in a relation. The operator is based
 * on a hash table. New elements are constructed for each original element in
 * which only the distinct fields are preserved. If the new element is distinct
 * then it is passed by the filter.
 * <p>
 * The filter is capable of changing the type of the accepted elements.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 * 
 * @param <E>
 *            The generic type of the source elements for the filter.
 * @param <F>
 *            The generic type of the elements passed by the filter.
 * 
 * @todo support changing the generic type as part of the filter. this is
 *       similar to the {@link IChunkConverter}.
 * 
 * @todo Reconcile with {@link IChunkConverter}, {@link DistinctFilter} (handles
 *       solutions) and {@link MergeFilter} (handles comparables),
 *       {@link DistinctSPOIterator}, etc.
 */
public class DistinctElementFilter<E> extends BOpBase implements
        IElementFilter<E> {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOp.Annotations {

        /**
         * The initial capacity of the {@link ConcurrentHashMap} used to impose
         * the distinct constraint.
         * 
         * @see #DEFAULT_INITIAL_CAPACITY
         */
        String INITIAL_CAPACITY = DistinctElementFilter.class.getName()
                + ".initialCapacity";

        int DEFAULT_INITIAL_CAPACITY = 16;

        /**
         * The load factor of the {@link ConcurrentHashMap} used to impose the
         * distinct constraint.
         * 
         * @see #DEFAULT_LOAD_FACTOR
         */
        String LOAD_FACTOR = DistinctElementFilter.class.getName()
                + ".loadFactor";

        float DEFAULT_LOAD_FACTOR = .75f;

        /**
         * The concurrency level of the {@link ConcurrentHashMap} used to impose
         * the distinct constraint.
         * 
         * @see #DEFAULT_CONCURRENCY_LEVEL
         */
        String CONCURRENCY_LEVEL = DistinctElementFilter.class.getName()
                + ".concurrencyLevel";

        int DEFAULT_CONCURRENCY_LEVEL = 16;

        /**
         * The set of fields whose values must be distinct.
         * 
         * @todo abstract base class to allow easy override for specific element
         *       types such as {@link SPO}.
         */
        String FIELDS = DistinctElementFilter.class.getName() + ".fields";

        /**
         * An optional constraint on the runtime type of the elements which are
         * acceptable to this filter.
         * 
         * @see IElementFilter#canAccept(Object)
         * 
         * @todo I am not convinced that we need this. It parallels something
         *       which was introduced into the {@link IElementFilter} interface,
         *       but I suspect that we do not need that either.
         */
        String CLASS_CONSTRAINT = DistinctElementFilter.class.getName()
                + ".classConstraint";
        
    }

    /**
     * Required deep copy constructor.
     */
    public DistinctElementFilter(final DistinctElementFilter<E> op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public DistinctElementFilter(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

        final int[] fields = getFields();

        if (fields == null)
            throw new IllegalArgumentException();

        if (fields.length == 0)
            throw new IllegalArgumentException();

    }

    /**
     * @see Annotations#INITIAL_CAPACITY
     */
    public int getInitialCapacity() {

        return getProperty(Annotations.INITIAL_CAPACITY,
                Annotations.DEFAULT_INITIAL_CAPACITY);

    }

    /**
     * @see Annotations#LOAD_FACTOR
     */
    public float getLoadFactor() {

        return getProperty(Annotations.LOAD_FACTOR,
                Annotations.DEFAULT_LOAD_FACTOR);

    }

    /**
     * @see Annotations#CONCURRENCY_LEVEL
     */
    public int getConcurrencyLevel() {

        return getProperty(Annotations.CONCURRENCY_LEVEL,
                Annotations.DEFAULT_CONCURRENCY_LEVEL);

    }

    public int[] getFields() {

        return (int[]) getProperty(Annotations.FIELDS);
        
    }
    
    public FutureTask<Void> eval(final BOpContext<IBindingSet> context) {

//        return new FutureTask<Void>(new DistinctTask<E>(this, context));
        throw new UnsupportedOperationException();
        
    }

    public boolean accept(E e) {
        // TODO Auto-generated method stub
        return false;
    }
    
    public boolean canAccept(Object o) {
        // @todo by annotation giving an optional type constraint.
        return true;
    }
    
//    /**
//     * Task executing on the node.
//     */
//    static private class DistinctTask<E> implements Callable<Void> {
//
//        private final BOpContext<IBindingSet> context;
//
//        /**
//         * A concurrent map whose keys are the bindings on the specified
//         * variables (the keys and the values are the same since the map
//         * implementation does not allow <code>null</code> values).
//         */
//        private /*final*/ ConcurrentHashMap<E, E> map;
//
//        /**
//         * The variables used to impose a distinct constraint.
//         */
//        private final int[] fields;
//        
//        DistinctTask(final DistinctElementFilter<E> op,
//                final BOpContext<IBindingSet> context) {
//
//            this.context = context;
//
//            this.fields = op.getFields();
//
//            this.map = new ConcurrentHashMap<E, E>(
//                    op.getInitialCapacity(), op.getLoadFactor(),
//                    op.getConcurrencyLevel());
//
//        }
//
//        /**
//         * Construct an element are distinct for the configured variables then return
//         * those bindings.
//         * 
//         * @param bset
//         *            The binding set to be filtered.
//         * 
//         * @return The distinct as bound values -or- <code>null</code> if the
//         *         binding set duplicates a solution which was already accepted.
//         */
//        private E accept(final E e) {
//
//            final E e2 = newElement(e);
//            
//            final boolean distinct = map.putIfAbsent(e2, e2) == null;
//
//            return distinct ? e2 : null;
//
//        }
//
//        public Void call() throws Exception {
//
//            final BOpStats stats = context.getStats();
//
//            final IAsynchronousIterator<IBindingSet[]> itr = context
//                    .getSource();
//
//            final IBlockingBuffer<IBindingSet[]> sink = context.getSink();
//
//            try {
//
//                while (itr.hasNext()) {
//                    
//                    final IBindingSet[] a = itr.next();
//
//                    stats.chunksIn.increment();
//                    stats.unitsIn.add(a.length);
//
//                    final List<IBindingSet> accepted = new LinkedList<IBindingSet>();
//
//                    int naccepted = 0;
//
//                    for (IBindingSet bset : a) {
//
////                        System.err.println("considering: " + bset);
//
//                        final IConstant<?>[] vals = accept(bset);
//
//                        if (vals != null) {
//
////                            System.err.println("accepted: "
////                                    + Arrays.toString(vals));
//
//                            /*
//                             * @todo This may cause problems since the
//                             * ArrayBindingSet does not allow mutation with
//                             * variables not declared up front. In that case use
//                             * new HashBindingSet( new ArrayBindingSet(...)).
//                             */
//                            
//                            accepted.add(new ArrayBindingSet(vars, vals));
//
//                            naccepted++;
//
//                        }
//
//                    }
//
//                    if (naccepted > 0) {
//
//                        final IBindingSet[] b = accepted
//                                .toArray(new IBindingSet[naccepted]);
//                        
////                        System.err.println("output: "
////                                + Arrays.toString(b));
//
//                        sink.add(b);
//
//                        stats.unitsOut.add(naccepted);
//                        stats.chunksOut.increment();
//
//                    }
//
//                }
//
//                // done.
//                return null;
//                
//            } finally {
//
//                sink.flush();
//                sink.close();
//
//                // discard the map.
//                map = null;
//
//            }
//
//        }
//
//    }

}
