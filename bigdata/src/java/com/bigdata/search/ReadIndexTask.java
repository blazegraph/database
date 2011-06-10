package com.bigdata.search;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.keys.IKeyBuilder;

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
public class ReadIndexTask<V extends Comparable<V>> implements Callable<Object> {

    final private static Logger log = Logger.getLogger(ReadIndexTask.class);

    private final String queryTerm;
//    private final boolean prefixMatch;
//    private final int exactMatchLength;
    private final double queryTermWeight;
    private final IRecordBuilder<V> recordBuilder;
    private final ConcurrentHashMap<V, Hit<V>> hits;
    private final ITupleIterator itr;

    /**
     * This instance is reused until it is consumed by a successful insertion
     * into {@link #hits} using
     * {@link ConcurrentHashMap#putIfAbsent(Object, Object)}. Once successfully
     * inserted, the {@link Hit#setDocId(long) docId} is set on the {@link Hit}
     * and a new instance is assigned to {@link #tmp}.
     */
    private Hit<V> tmp = new Hit<V>();

    /**
     * Setup a task that will perform a range scan for entries matching the
     * search term.
     * 
     * @param termText
     *            The term text for the search term.
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
    public ReadIndexTask(final String termText, final boolean prefixMatch,
            final double queryTermWeight, final FullTextIndex<V> searchEngine,
            final ConcurrentHashMap<V, Hit<V>> hits) {

        if (termText == null)
            throw new IllegalArgumentException();
        
        if (searchEngine == null)
            throw new IllegalArgumentException();
        
        if (hits == null)
            throw new IllegalArgumentException();
        
        this.queryTerm = termText;

//        this.prefixMatch = prefixMatch;
        
        this.queryTermWeight = queryTermWeight;

//        this.fieldsEnabled = searchEngine.isFieldsEnabled();
        
        this.recordBuilder = searchEngine.getRecordBuilder();

        this.hits = hits;
     
        final IKeyBuilder keyBuilder = searchEngine.getKeyBuilder();

//        if (!prefixMatch) {
//            /*
//             * Figure out how many bytes are in the Unicode sort key for the
//             * termText. In order to be an exact match, the visited tuples may
//             * not have more than this many bytes before the start of the docId
//             * field. (It is not possible for them to have fewer bytes since the
//             * Unicode sort key prefix length will be the same for both the
//             * fromKey and the toKey. The Unicode sort key for the toKey is
//             * formed by adding one to the LSB position).
//             */
//            
//            keyBuilder
//                    .appendText(termText, true/* unicode */, false/* successor */);
//            
//            exactMatchLength = keyBuilder.getLength();
//            
//        } else {
//            
//            // ignored.
//            exactMatchLength = -1;
//            
//        }
        
        /*
         * FIXME This would appear to start in the middle of the docId and
         * fieldId value space since I would assume that Long.MIN_VALUE is the
         * first docId.
         */
//        final byte[] fromKey = recordBuilder.getKey(keyBuilder, termText,
//                false/* successor */, Long.MIN_VALUE/* docId */,
//                Integer.MIN_VALUE/* fieldId */);

        final byte[] fromKey = recordBuilder.getFromKey(keyBuilder, termText);
        
        final byte[] toKey;
        
        if (prefixMatch) {
            /*
             * Accepts anything starting with the search term. E.g., given
             * "bro", it will match "broom" and "brown" but not "break".
             */
            toKey = recordBuilder.getToKey(keyBuilder, termText);
//            toKey = recordBuilder.getKey(keyBuilder, termText,
//                true/* successor */, Long.MIN_VALUE/* docId */,
//                Integer.MIN_VALUE/* fieldId */);
        } else {
            /*
             * Accepts only those entries that exactly match the search term.
             */
            toKey = recordBuilder.getToKey(keyBuilder, termText);
//            toKey = recordBuilder.getKey(keyBuilder, termText,
//                    false/* successor */, 
//                    Long.MAX_VALUE/* docId */, Integer.MAX_VALUE/* fieldId */);
        }

        if (log.isDebugEnabled())
            log.debug("termText=[" + termText + "], prefixMatch=" + prefixMatch
                    + ", queryTermWeight=" + queryTermWeight + "\nfromKey="
                    + BytesUtil.toString(fromKey) + "\ntoKey="
                    + BytesUtil.toString(toKey));
        
        /*
         * @todo filter by field. all we need to do is pass in an array of the
         * fields that should be accepted and formulate and pass along an
         * ITupleFilter which accepts only those fields. Note that the docId is
         * in the last position of the key.
         */
        itr = searchEngine.getIndex()
                .rangeIterator(fromKey, toKey, 0/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

    }
    
    /**
     * @return The #of fields with a hit on the search term as a
     *         {@link Long}.
     */
    public Long call() throws Exception {

        long nhits = 0;

        if (log.isDebugEnabled())
            log.debug("queryTerm=" + queryTerm + ", termWeight="
                    + queryTermWeight);

        final Thread t = Thread.currentThread();
        
        while (itr.hasNext()) {

            // don't test for interrupted on each result -- too much work.
            if (nhits % 100 == 0 && t.isInterrupted()) {

                if (log.isInfoEnabled())
                    log.info("Interrupted: queryTerm=" + queryTerm + ", nhits="
                            + nhits);

                return nhits;
                
            }
            
            // next entry
            final ITuple<?> tuple = itr.next();
            
//            // key is {term,docId,fieldId}
////            final byte[] key = tuple.getKey();
////            
////            // decode the document identifier.
////            final long docId = KeyBuilder.decodeLong(key, key.length
////                    - Bytes.SIZEOF_LONG /*docId*/ - Bytes.SIZEOF_INT/*fieldId*/);
//
//            final ByteArrayBuffer kbuf = tuple.getKeyBuffer();
//
//            /*
//             * The byte offset of the docId in the key.
//             * 
//             * Note: This is also the byte length of the match on the unicode
//             * sort key, which appears at the head of the key.
//             */
//            final int docIdOffset = kbuf.limit() - Bytes.SIZEOF_LONG /* docId */
//                    - (fieldsEnabled ? Bytes.SIZEOF_INT/* fieldId */: 0);

//            if (!prefixMatch && docIdOffset != exactMatchLength) {
//             
//                /*
//                 * The Unicode sort key associated with this tuple is longer
//                 * than the given token - hence it can not be an exact match.
//                 */
//                
//                continue;
//                
//            }
            
            // decode the document identifier.
//            final long docId = KeyBuilder.decodeLong(kbuf.array(), docIdOffset);
            final V docId = recordBuilder.getDocId(tuple);
            
            /*
             * Extract the term frequency and normalized term-frequency (term
             * weight) for this document.
             */
            final ITermMetadata md = recordBuilder.decodeValue(tuple);

            final int termFreq = md.termFreq();
            
            final double termWeight = md.getLocalTermWeight();
            
            if (log.isDebugEnabled())
                log.debug("hit: term=" + queryTerm + ", docId=" + docId
                        + ", termFreq=" + termFreq + ", termWeight="
                        + termWeight + ", product="
                        + (queryTermWeight * termWeight));
            
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
                    tmp = new Hit<V>();
                } else {
                    hit = oldValue;
                }
            }
            
            hit.add( queryTerm, queryTermWeight * termWeight );
            
            nhits++;
            
        }

        return nhits;
        
    }
    
}