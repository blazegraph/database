package com.bigdata.rdf.lexicon;

import java.util.concurrent.atomic.AtomicInteger;

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
    long sortTime = 0;

    /** time to insert terms into indices. */
    long indexTime = 0;

    /** time on the forward index. */
    long forwardIndexTime;

    /** time on the reverse index. */
    long reverseIndexTime;

    /** time to insert terms into the text indexer. */
    long fullTextIndexTime;

    /* The #of terms that could not be resolved (iff readOnly == true). */
    final AtomicInteger nunknown = new AtomicInteger();

}
