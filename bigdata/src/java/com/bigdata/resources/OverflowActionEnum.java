package com.bigdata.resources;

/**
 * The different actions that we can take to handle an index partition
 * during overflow processing.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public enum OverflowActionEnum {
    
    /**
     * Copy all tuples on the old journal to the new journal during
     * synchronous overflow processing.
     */
    Copy,

    /**
     * Copy the tuples from the last commit point on the old journal into an
     * index segment. Builds are done both in order to improve read
     * performance and to release dependencies on older journals.
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
    Split;
    
}
