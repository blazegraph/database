package com.bigdata.resources;

/**
 * The different actions that we can take to handle an index partition during
 * overflow processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @see OverflowSubtaskEnum
 */
public enum OverflowActionEnum {
    
    /**
     * Copy all tuples on the old journal to the new journal during
     * synchronous overflow processing.
     */
    Copy,

    /**
     * Copy the tuples from the last commit point on the old journal into an
     * index segment, and may incorporate tuples from zero or more additional
     * sources in the view. Builds are done both in order to improve read
     * performance and to release dependencies on older journals.
     * <p>
     * The #of sources to use in a build is choosen in order to keep the build
     * operation fast while also minimizing the #of sources that are accumulated
     * in the view over time and thereby reducing the frequency with which a
     * compacting merge must be performed.
     * <p>
     * If all sources in the view are used, then a build has the same semantics
     * as a {@link #Merge}, but it is still reported as a build since actions
     * selected as builds tend to be lighter weight even when all sources are
     * still used (for example, consider the first overflow event, where there
     * is only one source in the view - while that could be called a merge, the
     * practice is to call it a build).
     */
    Build,

    /**
     * Compacting merge of the sources for the index partition into a single
     * index segment. Compacting merges are done to improve read performance
     * and to keep index partition views from including too many distinct
     * index segment sources.
     */
    Merge,

    /**
     * Move the index partition to another data service. Note that moves may be
     * initiated either to redistribute the load more equitably among the data
     * services in the federation or to bring the left/right sibling of an index
     * partition onto the same data service as its right/left sibling so that
     * they may be joined.
     */
    Move,

    /**
     * Join left- and right- index partition siblings which have underflowed.
     */
    Join,

    /**
     * Split an index partition that has overflowed into 2 or more siblings.
     */
    Split,
    
    /**
     * Split an index partition receiving a lot of writes on the tail of the key
     * range into 2 siblings where the left-sibling has most of the key range
     * and the right-sibling has the tail of the key range.
     */
    TailSplit;
    
}
