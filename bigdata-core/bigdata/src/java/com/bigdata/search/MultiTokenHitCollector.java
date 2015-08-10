package com.bigdata.search;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Multi-token implementation of {@link IHitCollector} backed by a
 * {@link ConcurrentHashMap}.
 *  
 * @author mikepersonick
 *
 * @param <V>
 *            The generic type of the document identifier.
 */
public class MultiTokenHitCollector<V extends Comparable<V>> implements IHitCollector<V> {

	protected static final transient Logger log = Logger.getLogger(MultiTokenHitCollector.class);

	final ConcurrentHashMap<V/* docId */, Hit<V>> hits;
	
	public MultiTokenHitCollector(final Collection<CountIndexTask<V>> tasks) {
		
		int initialCapacity = 0;
		for (CountIndexTask<V> task : tasks) {
			
			final long rangeCount = task.getRangeCount();
			if (rangeCount > Integer.MAX_VALUE) {
				throw new RuntimeException("too many hits");
			}
			
			final int i = (int) rangeCount;
			
			/*
			 * find the max
			 */
			if (i > initialCapacity) {
				initialCapacity = i;
			}
			
		}
		
		/*
		 * Note: The actual concurrency will be the #of distinct query
		 * tokens.
		 */
    	final int concurrencyLevel = tasks.size();

    	if (log.isInfoEnabled()) {
    		log.info("initial capacity: " + initialCapacity);
    	}
    	
		hits = new ConcurrentHashMap<V, Hit<V>>(initialCapacity,
				.75f/* loadFactor */, concurrencyLevel);
		
	}
	
	@Override
	public Hit<V> putIfAbsent(V v, Hit<V> hit) {
		
		return hits.putIfAbsent(v, hit);
		
	}

	@Override
	public Hit<V>[] getHits() {
		
		return hits.values().toArray(new Hit[hits.size()]);
		
	}

}
