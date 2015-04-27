package com.bigdata.search;

import java.util.Iterator;

/**
 * Visits external fulltext index search results in order of decreasing relevance.
 * 
 * @author <a href="mailto:ms@metaphacts.com">Michael Schmidt</a>
 * @version $Id$
 */
public class FulltextSearchHiterator<A extends IFulltextSearchHit>
implements Iterator<A> {

	/**
	 * The index into the array of hits wrapped by this iterator.
	 */
	private int rank = 0;
	
	/**
	 * The array of hits wrapped by this iterator.
	 */
	private final A[] hits;
	
	/**
	 * 
	 * @param hits
	 */
	public FulltextSearchHiterator(final A[] hits) {

		if (hits == null)
			throw new IllegalArgumentException();

		this.hits = hits;

	}
	
	public boolean hasNext() {
		
		return rank < hits.length;
		
	}
	
	public A next() {
		
		return hits[rank++];
		
	}
	
    /**
     * @throws UnsupportedOperationException
     */
    public void remove() {
        
        throw new UnsupportedOperationException();
        
    }

    public String toString() {
        
        return "FulltextSearchHiterator{nhits=" + hits.length + "} : "
                + hits;
        
    }
    
}
