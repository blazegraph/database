package com.bigdata.search;

import org.apache.log4j.Logger;

import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.SuccessorUtil;

/**
 * Set up the from and to keys for the {@link ReadIndexTask} and 
 * {@link CountIndexTask}.
 * 
 * @param <V>
 *            The generic type of the document identifier.
 */
public abstract class AbstractIndexTask<V extends Comparable<V>> {

    final private static Logger log = Logger.getLogger(AbstractIndexTask.class);

    protected final String queryTerm;
    protected final int queryTermNdx;
    protected final int numQueryTerms;
    protected final double queryTermWeight;
    protected final byte[] fromKey;
    protected final byte[] toKey;

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
    public AbstractIndexTask(final String termText, 
    		final int termNdx, final int numTerms, 
    		final boolean prefixMatch, final double queryTermWeight, 
    		final FullTextIndex<V> searchEngine) {

        if (termText == null)
            throw new IllegalArgumentException();
        
        if (searchEngine == null)
            throw new IllegalArgumentException();
        
        this.queryTerm = termText;
        
        this.queryTermNdx = termNdx;
        
        this.numQueryTerms = numTerms;

        this.queryTermWeight = queryTermWeight;

        final IKeyBuilder keyBuilder = searchEngine.getIndex()
                .getIndexMetadata().getKeyBuilder();

//        {// = recordBuilder.getFromKey(keyBuilder, termText);
//            keyBuilder.reset();
//            keyBuilder
//                    .appendText(termText, true/* unicode */, false/* successor */);
//            fromKey = keyBuilder.getKey();
//        }
//
//        if (prefixMatch) {
//            /*
//             * Accepts anything starting with the search term. E.g., given
//             * "bro", it will match "broom" and "brown" but not "break".
//             * 
//             * Note: This uses the successor of the Unicode sort key, so it will
//             * scan all keys starting with that prefix until the sucessor of
//             * that prefix.
//             */
//            keyBuilder.reset();
//            keyBuilder.appendText(termText, true/* unicode */, true/*successor*/);
//            toKey = keyBuilder.getKey();
//        } else {
//            /*
//             * Accepts only those entries that exactly match the search term.
//             * 
//             * Note: This uses the fixed length successor of the fromKey. That
//             * gives us a key-range scan which only access keys having the same
//             * Unicode sort key.
//             */
//            toKey = SuccessorUtil.successor(fromKey.clone());
//        }

        /*
		 * Changed this to lop off the last three bytes (the pad plus
		 * run-length) for prefix search. Adding this three byte suffix to the
		 * prefix was causing problems for searches whose prefix ended with a
		 * numeric less than 7, because this codes to a byte less than the pad
		 * byte.
		 * 
		 * TODO We eventually need to change the pad byte to code to zero, but
		 * this will break binary compatibility.
		 */
        
        { // from key
	        keyBuilder.reset();
	        keyBuilder
	                .appendText(termText, true/* unicode */, false/* successor */);
	        final byte[] tmp = keyBuilder.getKey();
	        
	        if (prefixMatch) {
	        	fromKey = new byte[tmp.length-3];
	        	System.arraycopy(tmp, 0, fromKey, 0, fromKey.length);
	        } 
	        else
	        {// = recordBuilder.getFromKey(keyBuilder, termText);
	            fromKey = tmp;
	        }
        }

        { // to key
	        /*
	         * Accepts only those entries that exactly match the search term.
	         * 
	         * Note: This uses the fixed length successor of the fromKey. That
	         * gives us a key-range scan which only access keys having the same
	         * Unicode sort key.
	         */
	        toKey = SuccessorUtil.successor(fromKey.clone());
        }

    }

}
