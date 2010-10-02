package com.bigdata.bop.ap.filter;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.bop.BOp;
import com.bigdata.bop.HashMapAnnotations;
import com.bigdata.striterator.IChunkConverter;
import com.bigdata.striterator.MergeFilter;

import cutthecrap.utils.striterators.Filter;
import cutthecrap.utils.striterators.Filterator;

/**
 * A DISTINCT operator based for elements in a relation. The operator is based
 * on an in-memory hash table.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: DistinctElementFilter.java 3466 2010-08-27 14:28:04Z
 *          thompsonbry $
 * 
 * @todo Extract a common interface or metadata for all DISTINCT element filters
 *       (in memory hash map, persistence capable hash map, distributed hash
 *       map).
 * 
 * @todo Reconcile with {@link IChunkConverter},
 *       {@link com.bigdata.striterator.DistinctFilter} (handles solutions) and
 *       {@link MergeFilter} (handles comparables),
 *       {@link com.bigdata.rdf.spo.DistinctSPOIterator}, etc.
 */
public class DistinctFilter extends BOpFilterBase {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public interface Annotations extends BOpFilter.Annotations,
            HashMapAnnotations {

    }

    /**
     * A instance using the default configuration for the in memory hash map.
     */
    public static DistinctFilter newInstance() {
        return new DistinctFilter(NOARGS, NOANNS);
    }
    
    /**
     * Required deep copy constructor.
     */
    public DistinctFilter(final DistinctFilter op) {
        super(op);
    }

    /**
     * Required shallow copy constructor.
     */
    public DistinctFilter(final BOp[] args,
            final Map<String, Object> annotations) {

        super(args, annotations);

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

    @Override
    final protected Iterator filterOnce(Iterator src, final Object context) {
        
        return new Filterator(src, context, new DistinctFilterImpl());
        
    }

    private class DistinctFilterImpl extends Filter {

        private static final long serialVersionUID = 1L;

        /**
         * Note: Iterators are single threaded so we do not need to use a
         * {@link ConcurrentHashMap} here.
         */
        @SuppressWarnings("unchecked")
        private final LinkedHashSet members;

        public DistinctFilterImpl() {
            
            members = new LinkedHashSet(getInitialCapacity(), getLoadFactor());

        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean isValid(Object obj) {

            return members.add(obj);

        }

    }

}
