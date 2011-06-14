package com.bigdata.rdf.lexicon;

import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.counters.CAT;

/**
 * Class for reporting various timings for writes on the lexicon indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WriteTaskStats {
    
    /**
     * The #of distinct terms lacking a pre-assigned term identifier in [a].
     */
    int ndistinct;

    /** time to convert unicode terms to byte[] sort keys. */
    long keyGenTime = 0;

    /** time to sort terms by assigned byte[] keys. */
    long keySortTime = 0;

    /** time to insert terms into indices. */
    long indexTime = 0;

    /** time on the terms index. */
    long termsIndexTime;

    /** time to insert terms into the text indexer. */
    long fullTextIndexTime;

    /** The total size of all hash collisions buckets examined). */
    final CAT totalBucketSize = new CAT();

    /** The size of the largest hash collision bucket encountered. */
    final AtomicInteger maxBucketSize = new AtomicInteger();
    
    /** The #of terms that could not be resolved (iff readOnly == true). */
    final AtomicInteger nunknown = new AtomicInteger();

    public String toString() {
    	final StringBuilder sb = new StringBuilder();
    	sb.append(getClass().getSimpleName());
    	sb.append("{ndistinct="+ndistinct);
    	sb.append(",keyGenTime="+keyGenTime+"ms");
    	sb.append(",keySortTime="+keySortTime+"ms");
    	sb.append(",indexTime="+indexTime+"ms");
    	sb.append(",termsIndexTime="+termsIndexTime+"ms");
    	sb.append(",fullTextIndexTime="+fullTextIndexTime+"ms");
    	sb.append(",totalBucketSize="+totalBucketSize);
    	sb.append(",maxBucketSize="+maxBucketSize);
    	sb.append(",nunknown="+nunknown);
    	sb.append("}");
    	return sb.toString();
    }
    
}
