package com.bigdata.search;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Single-token implementation of {@link IHitCollector} backed by a
 * simple array of hits.
 *  
 * @author mikepersonick
 *
 * @param <V>
 *            The generic type of the document identifier.
 */
public class SingleTokenHitCollector<V extends Comparable<V>> implements IHitCollector<V> {

	protected static final transient Logger log = Logger.getLogger(SingleTokenHitCollector.class);
	
	/**
	 * The pre-allocated array (allocated using the range count of the search.
	 */
	final Hit<V>[] hits;
	
	/**
	 * The current index into the array (also representes the true number of
	 * hits, which could be different (less) than the allocated number due
	 * to delete records).
	 */
	int i = 0;
	
	public SingleTokenHitCollector(final CountIndexTask<V> task) {
		
		final long rangeCount = task.getRangeCount();
		if (rangeCount > Integer.MAX_VALUE) {
			throw new RuntimeException("too many hits");
		}
		
		final int i = (int) rangeCount;
		
		if (log.isInfoEnabled()) {
			log.info("array size: " + i);
		}
		
		this.hits = new Hit[i];
		
	}
	
	@Override
	public Hit<V> putIfAbsent(V v, Hit<V> hit) {
		
		hits[i++] = hit;
		
		return null;
		
	}

	/**
	 * Return the hits array, right-sizing it if necessary.
	 */
	@Override
	public Hit<V>[] getHits() {
		
		if (i == hits.length) {
			
			return hits;
			
		} else {
			
			final Hit<V>[] tmp = new Hit[i];
			
			System.arraycopy(hits, 0, tmp, 0, i);
			
			return tmp;
			
		}
		
	}

}
