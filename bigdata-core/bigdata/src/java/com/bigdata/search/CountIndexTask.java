package com.bigdata.search;

import org.apache.log4j.Logger;

import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ISimpleSplitHandler;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;
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
 * @version $Id: CountIndexTask.java 6018 2012-02-13 09:48:15Z mrpersonick $
 */
public class CountIndexTask<V extends Comparable<V>> extends AbstractIndexTask<V> {

    final private static Logger log = Logger.getLogger(CountIndexTask.class);

    private final long rangeCount;
    

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
     */
    public CountIndexTask(final String termText, final int termNdx, final int numTerms, 
    		final boolean prefixMatch,
            final double queryTermWeight, final FullTextIndex<V> searchEngine) {

    	super(termText, termNdx, numTerms, prefixMatch, queryTermWeight, searchEngine);
    	
        if (log.isDebugEnabled())
            log.debug("termText=[" + termText + "], prefixMatch=" + prefixMatch
                    + ", queryTermWeight=" + queryTermWeight + "\nfromKey="
                    + BytesUtil.toString(fromKey) + "\n  toKey="
                    + BytesUtil.toString(toKey));

        rangeCount = searchEngine.getIndex().rangeCount(fromKey, toKey);

    }

    /**
     * Return the range count for this task.
     */
	public long getRangeCount() {
		
		return rangeCount;
		
	}
    
}
