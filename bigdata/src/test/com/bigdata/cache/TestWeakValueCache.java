/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
/*
 * Created on Apr 21, 2006
 */
package com.bigdata.cache;

import com.bigdata.cache.ICacheListener;
import com.bigdata.cache.ICachePolicy;
import com.bigdata.cache.LRUCache;
import com.bigdata.cache.WeakCacheEntryFactory;
import com.bigdata.cache.WeakValueCache;

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
    	LRUCache<Object> delegate = new LRUCache<Object>(5);
    	WeakCacheEntryFactory<Object> factory = new WeakCacheEntryFactory<Object>();
    	int initialCapacity = 1;
    	float loadFactor = 1f;
    	
    	try {
    		new WeakValueCache<Object>(null);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
		new WeakValueCache<Object>(delegate);

    	try {
    		new WeakValueCache<Object>(delegate,null);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
		new WeakValueCache<Object>(delegate,factory);
    	
    	/*
    	 * (initialCapacity,loadFactor,delegate,factory)
		 */
    	try {
    		new WeakValueCache<Object>(-1,.75f,delegate,factory);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    	try {
    		new WeakValueCache<Object>(initialCapacity,0f,delegate,factory);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    	try {
    		new WeakValueCache<Object>(initialCapacity,loadFactor,null,factory);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
    	try {
    		new WeakValueCache<Object>(initialCapacity,loadFactor,delegate,null);
    		fail("Expecting exception");
    	}
    	catch( IllegalArgumentException ex ) {
    		log.info("Ignoring expected exception: "+ex);
    	}
		new WeakValueCache<Object>(initialCapacity,loadFactor,delegate,factory);

    }

    /**
	 * Test fixture factory.
	 * 
	 * @return A new {@link WeakValueCache} backed by a {@link LRUCache} with
	 *         the stated capacity.
	 */
    public ICachePolicy getCachePolicy(int capacity ) {
    	return new WeakValueCache<String>(new LRUCache<String>(capacity));
    }
    
    /**
	 * Test verifies that put() may not be used to replace the object in the
	 * cache under a given oid, but only to update the dirty flag associated
	 * with that entry (and to update the LRU cache ordering).
	 */
    public void test_put_mayNotModifyObject() {
    	String A = "A";
    	String B = "B";
    	WeakValueCache<String> cache = new WeakValueCache<String>(new LRUCache<String>(5));
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
    	WeakValueCache<String> cache = new WeakValueCache<String>(new LRUCache<String>(1));
    	assertNull(cache.getCacheListener());
    	cache.setListener(null);
    	assertNull(cache.getCacheListener());
    	ICacheListener<String> l = new MyCacheListenerThrowsException<String>();
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
    	WeakValueCache<String> cache = new WeakValueCache<String>(new LRUCache<String>(1));
    	MyCacheListener<String> l = new MyCacheListener<String>();
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
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				0, A, false) }, cache.entryIterator());
		/*
		 * Update the dirty flag for object under that key, so no eviction.
		 */
		cache.put(0, A, true);
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				0, A, true) }, cache.entryIterator());
    	/*
    	 * Add an object under another key, causes eviction of [A].
    	 */
    	l.setExpectedEvent(0,A,true);
    	cache.put(1, B, false );
    	l.clearLastEvent();
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				1, B, false) }, cache.entryIterator());
    	/*
    	 * Update the dirty flag for the object under that key, so no eviction.
    	 */
    	l.denyEvents();
    	cache.put(1, B, true );
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				1, B, true) }, cache.entryIterator());
    	/*
    	 * Add an object under another key, causes eviction of [D].
    	 */
    	l.setExpectedEvent(1,B,true);
    	cache.put(0, A, false );
    	l.clearLastEvent();
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				0, A, false) }, cache.entryIterator());
    }

    /**
     * Test verifies that changes to the dirty flag are propagated to the hard
     * reference cache.
     */
    public void test_dirtyFlagPropagatesToHardReferenceCache() {

    	LRUCache<String> lru = new LRUCache<String>(3);
    	WeakValueCache<String> cache = new WeakValueCache<String>(lru);
    	
    	final String A = "A";
    	cache.put(0L, A, false);
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				0L, A, false) }, cache.entryIterator());

		cache.put(0L, A, true );
		assertSameEntryOrdering("contents", new CacheEntry[] { new CacheEntry(
				0L, A, true) }, cache.entryIterator());

    }
    
}
