package com.bigdata.rdf.lexicon;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.counters.CAT;

/**
 * Class for reporting various timings for writes on the lexicon indices.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class WriteTaskStats {
    
    /**
     * The #of distinct terms lacking a pre-assigned term identifier. If writes
     * were permitted, then this is also the #of terms written onto the index.
     */
    final AtomicLong ndistinct = new AtomicLong();

    /** time to convert unicode terms to byte[] sort keys. */
    final CAT keyGenTime = new CAT();

    /** time to sort terms by assigned byte[] keys. */
    final CAT keySortTime = new CAT();

    /** time to insert terms into indices. */
    final AtomicLong indexTime = new AtomicLong();

    /** time on the forward index. */
    long forwardIndexTime;

    /** time on the reverse index. */
    long reverseIndexTime;

    /** time on the terms index. */
    long termsIndexTime;

    /** time to insert terms into the text indexer. */
    final AtomicLong fullTextIndexTime = new AtomicLong();

    /** The total size of all hash collisions buckets examined). */
    final CAT totalBucketSize = new CAT();

    /** The size of the largest hash collision bucket encountered. */
    final AtomicInteger maxBucketSize = new AtomicInteger();
    
    /** The #of terms that could not be resolved (iff readOnly == true). */
    final AtomicInteger nunknown = new AtomicInteger();

    public String toString() {
    	final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append("{ndistinct=" + ndistinct);
        sb.append(",keyGenTime=" + keyGenTime + "ms");
        sb.append(",keySortTime=" + keySortTime + "ms");
        sb.append(",indexTime=" + indexTime + "ms");
        sb.append(",t2idIndexTime=" + forwardIndexTime + "ms");
        sb.append(",id2tIndexTime=" + reverseIndexTime + "ms");
        sb.append(",termsIndexTime=" + termsIndexTime + "ms");
        sb.append(",fullTextIndexTime=" + fullTextIndexTime + "ms");
        sb.append(",totalBucketSize=" + totalBucketSize);
        sb.append(",maxBucketSize=" + maxBucketSize);
        sb.append(",nunknown=" + nunknown);
        sb.append("}");
        return sb.toString();
    }

}
