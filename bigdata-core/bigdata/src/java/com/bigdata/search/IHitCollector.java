package com.bigdata.search;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Interface used to collect {@link Hit}s from the {@link ReadIndexTask}.
 * Switched over to this interface to allow for an optimization when only
 * one token is presented for search - in this case we now use a simple 
 * array of hits rather than a heavyweight ConcurrentHashMap.
 * 
 * @author mikepersonick
 *
 * @param <V>
 *            The generic type of the document identifier.
 */
public interface IHitCollector<V extends Comparable<V>> {

	/**
	 * Mimic the ConcurrentHashMap method.
	 * <p>
	 * See {@link ConcurrentHashMap#putIfAbsent(Object, Object)}.
	 * 
	 * @param v
	 * 		the document identifier for the hit
	 * @param hit
	 * 		the full text hit
	 * @return
	 * 		the old value or <code>null</code> if there was no old value
	 */
	Hit<V> putIfAbsent(final V v, final Hit<V> hit);
	
	/**
	 * Returns an array of hits collected by this instance.
	 */
	Hit<V>[] getHits();
	
}
