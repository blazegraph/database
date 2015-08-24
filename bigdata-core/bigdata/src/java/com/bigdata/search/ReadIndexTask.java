package com.bigdata.search;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.util.BytesUtil;

/**
 * Procedure reads on the terms index, aggregating data on a per-{@link Hit}
 * basis.
 * <p>
 * The procedure uses an {@link IRangeQuery#rangeIterator(byte[], byte[])} to
 * perform a key range scan for a specific term. The range iterator will
 * automatically issue queries, obtaining a "chunk" of results at a time. Those
 * results are aggregated on the {@link Hit} collection, which is maintained in
 * a thread-safe hash map.
 * <p>
 * Note: An {@link ISimpleSplitHandler} imposes the constraint that index
 * partitions may only fall on a term boundary, hence all tuples for any given
 * term will always be found on the same index partition.
 * 
 * @param <V>
 *            The generic type of the document identifier.
 *            
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ReadIndexTask<V extends Comparable<V>> extends AbstractIndexTask<V> 
	implements Callable<Object> {

    final private static Logger log = Logger.getLogger(ReadIndexTask.class);

    private final IHitCollector<V> hits;
    private final ITupleIterator<?> itr;

    /**
     * This instance is reused until it is consumed by a successful insertion
     * into {@link #hits} using
     * {@link ConcurrentHashMap#putIfAbsent(Object, Object)}. Once successfully
     * inserted, the {@link Hit#setDocId(long) docId} is set on the {@link Hit}
     * and a new instance is assigned to {@link #tmp}.
     */
    private Hit<V> tmp;// = new Hit<V>();

    /**
     * Setup a task that will perform a range scan for entries matching the
     * search term.
     * 
     * @param termText
     *            The term text for the search term.
     * @param termNdx
     * 			  The index of this term within the overall search.
     * @param numTerms
     * 			  The overall number of search terms.
     * @param prefixMatch
     *            When <code>true</code> any term having <i>termText</i> as a
     *            prefix will be matched. Otherwise the term must be an exact
     *            match for the <i>termText</i>.
     * @param queryTermWeight
     *            The weight for the search term.
     * @param searchEngine
     *            The search engine.
     * @param hits
     *            The map where the hits are being aggregated.
     */
    public ReadIndexTask(final String termText, 
    		final int termNdx, final int numTerms,
    		final boolean prefixMatch, final double queryTermWeight, 
    		final FullTextIndex<V> searchEngine, final IHitCollector<V> hits) {

    	super(termText, termNdx, numTerms, prefixMatch, queryTermWeight, searchEngine);
    	
        if (hits == null)
            throw new IllegalArgumentException();
        
        this.hits = hits;
     
        if (log.isDebugEnabled())
            log.debug("termText=[" + termText + "], prefixMatch=" + prefixMatch
                    + ", queryTermWeight=" + queryTermWeight + "\nfromKey="
                    + BytesUtil.toString(fromKey) + "\n  toKey="
                    + BytesUtil.toString(toKey));

        /*
         * TODO filter by document and/or field. all we need to do is pass in
         * an array of the fields that should be accepted and formulate and pass
         * along an ITupleFilter which accepts only those fields. we can also
         * filter by document in the same manner.
         */
        itr = searchEngine.getIndex()
                .rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);
        
        tmp = new Hit<V>(numTerms);

    }
    
    /**
     * @return The #of fields with a hit on the search term as a
     *         {@link Long}.
     */
    public Long call() throws Exception {

        try {

            final long nhits = run();
        
            return nhits;
        
        } catch (Throwable t) {

            throw launderThrowable(t);
            
        }
        
    }
    
    private long run()  {
        
        long nhits = 0;

        if (log.isDebugEnabled())
            log.debug("queryTerm=" + queryTerm + ", termWeight="
                    + queryTermWeight);

//        final Thread t = Thread.currentThread();
        
        while (itr.hasNext()) {

            // don't test for interrupted on each result -- too much work.
            if (nhits % 1000 == 0 && Thread.interrupted()) {

//                if (log.isInfoEnabled())
                log.warn("Interrupted: queryTerm=" + queryTerm + ", nhits="
                        + nhits);

                return nhits;
                
            }
            
            // next entry
            final ITuple<?> tuple = itr.next();
            
            // decode the tuple.
            final ITermDocRecord<V> rec = (ITermDocRecord<V>) tuple.getObject();

            // the document identifier.
            final V docId = rec.getDocId();
            
            /*
             * Extract the term frequency and normalized term-frequency (term
             * weight) for this document.
             */
//            final ITermMetadata md = recordBuilder.decodeValue(tuple);

            final double termWeight = rec.getLocalTermWeight();
            
            // Note: Log test shows up in profiler.
//            if (log.isDebugEnabled()) {
//            final int termFreq = rec.termFreq();
//                log.debug("hit: term=" + queryTerm + ", docId=" + docId
//                        + ", termFreq=" + termFreq + ", termWeight="
//                        + termWeight + ", product="
//                        + (queryTermWeight * termWeight));
//        }
            
            /*
             * Play a little magic to get the docId in the hit set without race
             * conditions.
             */
            final Hit<V> hit;
            {
                Hit<V> oldValue = hits.putIfAbsent(docId, tmp);
                if (oldValue == null) {
                    hit = tmp;
                    hit.setDocId(docId);
                    tmp = new Hit<V>(numQueryTerms);
                } else {
                    hit = oldValue;
                }
            }
            
            hit.add( queryTermNdx, queryTermWeight * termWeight );
            
            nhits++;
            
        }

        return nhits;
        
    }
    
    /**
     * Log an error and wrap the exception iff necessary.
     * 
     * @param t
     *            The thrown error.
     * 
     * @return The laundered exception.
     * 
     * @throws Exception
     */
    private RuntimeException launderThrowable(final Throwable t)
            throws Exception {
        try {
            // log an error
//    		log.error(t, t);
        } finally {
            // ignore any problems here.
        }
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof Exception) {
            throw (Exception) t;
        } else
            throw new RuntimeException(t);
    }

}
