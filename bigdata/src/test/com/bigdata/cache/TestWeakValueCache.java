/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Apr 21, 2006
 */
package com.bigdata.cache;


/**
 * Tests suite for the {@link WeakValueCache}. This class tests the weak cache
 * and its integration with a delegate hard reference cache policy.
 * 
 * @todo verify entries cleared once weakly reachable, but not before.
 * 
 * @todo make sure that the dirty flag is correctly maintained as entries are
 *       cleared and removed from the weak value cache (i.e., that we do not
 *       falsely apply the old state of the flag when recycling cache entries).
 *       
 * @todo performance test suite for tuning access paths in the cache
 *       implementation, including the load factor on the weak reference and
 *       hard reference caches. Is it possible to write this test so that it can
 *       be reused when a persistence layer is integrated?
 *       
 * @todo integration test with abstraction for the specific persistence layer,
 *       but never the less testing the semantics of the weak reference cache
 *       with the integrated persistence layer. The abstract could consist of
 *       the means for configuring the persistence layer with a known cache
 *       configuration.
 * 
 * @version $Id$
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */

public class TestWeakValueCache extends AbstractCachePolicyTest {

    /**
     * 
     */
    public TestWeakValueCache() {
        super();
    }

    /**
     * @param name
     */
    public TestWeakValueCache(String name) {
        super(name);
    }

    /**
     * Test legal and illegal invocations of the constructors.
     */
    public void test_ctor() {
    	
    	// local fixtures.
    	LRUCache<Integer,Object> delegate = new LRUCache<Integer,Object>(5);
    	WeakCacheEntryFactory<Integer,Object> factory = new WeakCacheEntryFactory<Integer,Object>();
    	int initialCapacity = 1;
    	float loadFactor = 1f;
    	
    	try {
    		new WeakValueCache<Integer,Object>(null);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
		new WeakValueCache<Integer,Object>(delegate);

    	try {
    		new WeakValueCache<Integer,Object>(delegate,null);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
		new WeakValueCache<Integer,Object>(delegate,factory);
    	
    	/*
    	 * (initialCapacity,loadFactor,delegate,factory)
		 */
    	try {
    		new WeakValueCache<Integer,Object>(-1,.75f,delegate,factory);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    	try {
    		new WeakValueCache<Integer,Object>(initialCapacity,0f,delegate,factory);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    	try {
    		new WeakValueCache<Integer,Object>(initialCapacity,loadFactor,null,factory);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    	try {
    		new WeakValueCache<Integer,Object>(initialCapacity,loadFactor,delegate,null);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
		new WeakValueCache<Integer,Object>(initialCapacity,loadFactor,delegate,factory);

    }

    /**
	 * Test fixture factory.
	 * 
	 * @return A new {@link WeakValueCache} backed by a {@link LRUCache} with
	 *         the stated capacity.
	 */
    public ICachePolicy getCachePolicy(int capacity ) {
    	return new WeakValueCache<Long,String>(new LRUCache<Long,String>(capacity));
    }
    
    /**
	 * Test verifies that put() may not be used to replace the object in the
	 * cache under a given oid, but only to update the dirty flag associated
	 * with that entry (and to update the LRU cache ordering).
	 */
    public void test_put_mayNotModifyObject() {
    	String A = "A";
    	String B = "B";
    	WeakValueCache<Integer, String> cache = new WeakValueCache<Integer, String>(
                new LRUCache<Integer, String>(5));
    	cache.put(0,A,true);
    	cache.put(0,A,false);
    	try {
    		cache.put(0,B,true);
    		fail("Expecting exception.");
    	}
    	catch(IllegalStateException ex) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    }

    /**
     * Tests excercises the ability to set and get the cache listener.
     */
    public void test_cacheListener_getSet() {
    	WeakValueCache<Integer,String> cache = new WeakValueCache<Integer,String>(new LRUCache<Integer,String>(1));
    	assertNull(cache.getCacheListener());
    	cache.setListener(null);
    	assertNull(cache.getCacheListener());
    	ICacheListener<Integer,String> l = new MyCacheListenerThrowsException<Integer,String>();
    	cache.setListener( l );
    	assertEquals( l, cache.getCacheListener() );
    	cache.setListener(null);
    	assertNull(cache.getCacheListener());
    }
    
    /**
	 * Test verifies that cache evictions are fired once the inner LRU cache is
	 * full.
	 */
    public void test_cacheListener_objectEvicted() {
    	/*
		 * Setup weak cache backed by an LRU with a capacity of ONE (1).
		 */
    	WeakValueCache<Integer, String> cache = new WeakValueCache<Integer, String>(
                new LRUCache<Integer, String>(1));
        MyCacheListener<Integer, String> l = new MyCacheListener<Integer, String>();
    	l.denyEvents();
    	cache.setListener( l );
    	/*
		 * Hold hard references to the objects that we insert into the weak
		 * cache for this test.
		 */
    	final String A = "A";
    	final String B = "B";
    	/*
    	 * Add an object to the cache under a key.
    	 */
    	cache.put(0, A, false);
		assertSameEntryOrdering(
                "contents",
                new CacheEntry[] { new CacheEntry<Integer, String>(
				0, A, false) }, cache.entryIterator());
		/*
		 * Update the dirty flag for object under that key, so no eviction.
		 */
		cache.put(0, A, true);
		assertSameEntryOrdering(
                "contents",
                new CacheEntry[] { new CacheEntry<Integer, String>(0, A, true) },
                cache.entryIterator());
    	/*
    	 * Add an object under another key, causes eviction of [A].
    	 */
    	l.setExpectedEvent(0,A,true);
    	cache.put(1, B, false );
    	l.clearLastEvent();
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry<Integer, String>(
				1, B, false) }, cache.entryIterator());
    	/*
    	 * Update the dirty flag for the object under that key, so no eviction.
    	 */
    	l.denyEvents();
    	cache.put(1, B, true );
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry<Integer, String>(
				1, B, true) }, cache.entryIterator());
    	/*
    	 * Add an object under another key, causes eviction of [D].
    	 */
    	l.setExpectedEvent(1,B,true);
    	cache.put(0, A, false );
    	l.clearLastEvent();
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry<Integer, String>(
				0, A, false) }, cache.entryIterator());
    }

    /**
     * Test verifies that changes to the dirty flag are propagated to the hard
     * reference cache.
     */
    public void test_dirtyFlagPropagatesToHardReferenceCache() {

    	LRUCache<Integer,String> lru = new LRUCache<Integer,String>(3);
    	WeakValueCache<Integer,String> cache = new WeakValueCache<Integer,String>(lru);
    	
    	final String A = "A";
    	cache.put(0, A, false);
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry<Integer, String>(
				0, A, false) }, cache.entryIterator());

		cache.put(0, A, true );
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry<Integer, String>(
				0, A, true) }, cache.entryIterator());

    }
    
}
