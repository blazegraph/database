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
 * Created on May 23, 2006
 */

package com.bigdata.cache;

import java.util.Iterator;
import java.util.Vector;

import junit.framework.TestCase2;

/**
 * Abstract base class for cache policy test defines some test harness helper
 * methods and utility classes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractCachePolicyTest extends TestCase2 {

	/**
	 * 
	 */
	public AbstractCachePolicyTest() {
		super();
	}

	/**
	 * @param name
	 */
	public AbstractCachePolicyTest(String name) {
		super(name);
	}

    /**
     * Test helper used to generate expected data for testing cache behavior.
     * 
     * @version $Id$
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     */
    static final class CacheEntry<K,T> implements ICacheEntry<K, T>
    {

        private final K key;
        private final T value;
        private final boolean dirty;
        
        CacheEntry( K key, T value, boolean dirty ) {
            if( key == null ) {
                throw new IllegalArgumentException();
            }
            if( value == null ) {
                throw new IllegalArgumentException();
            }
            this.key = key;
            this.value = value;
            this.dirty = dirty;
        }
        
        public K getKey() {
            return key;
        }

        public T getObject() {
            return value;
        }
        
        public boolean isDirty() {
            return dirty;
        }

        public void setDirty(boolean dirty) {
            throw new UnsupportedOperationException();
        }
        
    }

    /**
     * Method verifies that the <i>actual </i> {@link Iterator}produces the
     * expected objects in the expected order. Objects are compared <strong>by
     * reference </strong>. Errors are reported if too few or too many objects
     * are produced, etc.
     * 
     * @see TestCase2#assertSameIterator(String, Object[], Iterator)
     */

    static public void assertSameIterator
	( String msg,
	  Object[] expected,
	  Iterator actual
	  )
    {

	int i = 0;

	while( actual.hasNext() ) {

	    if( i >= expected.length ) {

		fail( msg+": The iterator is willing to visit more than "+
		      expected.length+
		      " objects."
		      );

	    }

	    Object g = actual.next();
	    
	    assertTrue
		( msg+": Different objects at index="+i+
		  ": expected="+expected[ i ]+
		  ", actual="+g,
		  expected[ i ].equals( g )
		  );

	    i++;

	}

	if( i < expected.length ) {

	    fail( msg+": The iterator SHOULD have visited "+expected.length+
		  " objects, but only visited "+i+
		  " objects."
		  );

	}

    }

    /**
     * Verify that the cache iterator visit {@link ICacheEntry}instances that
     * are consistent with the expected entries in both order and data. The data
     * consistency requirements are: (a) same oid/key (compared by value); same
     * value object associated with that key (compared by reference); and same
     * dirty flag state.
     * 
     * @param msg
     *            Message.
     * @param expected
     *            Array of expected cache entry objects in expected order.
     * @param actual
     *            Iterator visiting {@link ICacheEntry}objects.
     */

    public void assertSameEntryOrdering(String msg,ICacheEntry expected[], Iterator actual ) {

	int i = 0;

	while( actual.hasNext() ) {

	    if( i >= expected.length ) {

		fail( msg+": The iterator is willing to visit more than "+
		      expected.length+
		      " objects."
		      );

	    }

	    ICacheEntry expectedEntry = expected[ i ];
	    ICacheEntry actualEntry = (ICacheEntry) actual.next();
	    
	    assertEquals
	        ( msg+": key differs at index="+i,
	                expectedEntry.getKey(),
	                actualEntry.getKey()
	                );

	    assertTrue
	        ( msg+": value references differ at index="+i+", expected="+expected+", actual="+actual,
	                expectedEntry.getObject() == actualEntry.getObject()
	                );	    

	    assertEquals
	        ( msg+": dirty flag differs at index="+i,
	                expectedEntry.isDirty(),
	                actualEntry.isDirty()
	                );

	    i++;

	}

	if( i < expected.length ) {

	    fail( msg+": The iterator SHOULD have visited "+expected.length+
		  " objects, but only visited "+i+
		  " objects."
		  );

	}

    }

    /**
	 * Dumps the contents of the cache on {@link System#err} using
	 * {@link ICachePolicy#entryIterator()}.
	 * 
	 */
    static void showCache(ICachePolicy cache) {
    	System.err.println("\nshowCache: "+cache.getClass());
    	System.err.println("\tsize="+cache.size());
    	System.err.println("\tcapacity="+cache.capacity());
    	Iterator itr = cache.entryIterator();
    	int i = 0;
    	while( itr.hasNext() ) {
    		ICacheEntry entry = (ICacheEntry) itr.next();
    		System.err.println("["+i+"]\tkey="+entry.getKey()+", value="+entry.getObject()+", dirty="+entry.isDirty());
    		i++;
    	}
    }
    
    /**
     * You set whether or not a cache event is expected and what the expected
     * data will be for that event. If an event occurs when none is expected
     * then an exception is thrown. If an event occurs with unexpected data then
     * an exception is thrown. Otherwise the listener silently accepts the
     * event.
     */
    public static class MyCacheListener<K,T> implements ICacheListener<K,T>
    {
        private boolean expectingEvent = false;
        private boolean haveEvent = false;
        private static class Event<K,T> {
            private K expectedOid = null;
            private T expectedObj = null;
            private boolean expectedDirty = false;        	
        }
        private Vector<Event<K,T>> events = new Vector<Event<K,T>>();
        
        /**
         * Verify that event data is consistent with our expectations.
         * 
         * @exception IllegalStateException
         *                If we already have an event.
         * @exception AssertionFailedException
         *                If we are not expecting an event.
         * @exception AssertionFailedException
         *                If the object identifier or object in the event are
         *                incorrect. The objects are compared by reference, not
         *                by equals().
         */
        public void objectEvicted(ICacheEntry<K,T> entry) {
        	if(!expectingEvent) {
        		throw new IllegalStateException("Not expecting event: "+entry);
        	}
            if( haveEvent ) {
                throw new IllegalStateException("Already have an event: "+entry);
            }
            haveEvent = true;
            if( events.size() == 0 ) {
            	throw new IllegalStateException("No expected events: "+entry);
            }
            Event e = (Event) events.remove(0); // pop off next event.
            assertEquals("oid",e.expectedOid,entry.getKey());
            assertTrue("obj",e.expectedObj == entry.getObject()); // compare by reference not equals().
            assertEquals("dirty",e.expectedDirty,entry.isDirty());
        }

        /**
		 * Sets the listener to expect an event with the given object identifier
		 * and object.
		 * 
		 * @param oid
		 *            The expected object identifier.
		 * @param obj
		 *            The expected object (comparison by reference).
		 * @param dirty
		 *            Iff the expected object will be marked as dirty.
		 * 
		 * @see #clearLastEvent()
		 * @see #denyEvents()
		 */
        public void setExpectedEvent(K oid,T obj,boolean dirty) {
        	clearExpectedEvents();
        	addExpectedEvent(oid,obj,dirty);
        }

        public void clearExpectedEvents() {
        	events.clear();
        	denyEvents();
        }
        
        public void addExpectedEvent( K oid, T obj, boolean dirty) {
            assert oid != null;
            assert obj != null;
        	Event<K,T> e = new Event<K,T>();
            e.expectedOid = oid;
            e.expectedObj = obj;
            e.expectedDirty = dirty;
            events.add(e);
            allowEvents();
        }
        
        /**
         * Causes an {@link IllegalStateException} to be thrown from the
         * listener if an event is received.
         * 
         * @see #setExpectedEvent(Object, Object, boolean)
         */
        public void denyEvents()
        {
            expectingEvent = false;
        }

        /**
         * Allows more events.  If an event had already been received, then it
         * is cleared now.
         */
        public void allowEvents() {
        	expectingEvent = true;
        	if( haveEvent ) {
        		clearLastEvent();
        	}
        }
        
        /**
         * Clear the last event so that a new event may be accepted. An
         * exception is thrown if no event has been received so that this method
         * may be used to test for the absence of an expected event.
         * 
         * @exception IllegalStateException
         *                if no event has been received.
         */
        public void clearLastEvent() {
            if( ! haveEvent ) {
                throw new IllegalStateException("no event");
            }
            haveEvent = false;
        }
    }

    /**
     * Implementation always throws an exception.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class MyCacheListenerThrowsException<K,T> implements ICacheListener<K,T>
    {
		public void objectEvicted(ICacheEntry<K,T> entry) {
			throw new UnsupportedOperationException();
		}
    }

    /**
	 * <p>
	 * Abstract fixture factory for the cache policy. The use of this fixture
	 * factory makes it possible for us to reuse the test suite for the LRU
	 * cache policy on the weak reference cache since the latter is required to
	 * delegate the ordering of the cache to an inner hard reference cache.
	 * </p>
	 * <p>
	 * Note: LRU cache policy tests that will be reused for the weak reference
	 * cache MUST hold hard references to the objects that are inserted into the
	 * cache to ensure that the cache entries for those objects are not cleared
	 * from the weak reference cache. The weak reference cache facility for
	 * clearing cache entries once the objects in those entries become weakly
	 * reachable must be tested by specialized test methods.
	 * </p>
	 * 
	 * @param capacity
	 *            the capacity of the hard reference cache policy.
	 * 
	 * @return The cache policy fixture.
	 */
	abstract public ICachePolicy getCachePolicy(int capacity);
    
    /**
	 * <p>
	 * Test verifies that LRU ordering is correctly maintained on a series of
	 * insert, update, and remove operations and that eviction notices are fired
	 * as necessary.
	 * </p>
	 * <p>
	 * The test method retains a hard reference to the objects inserted into the
	 * cache so that the cache entries for such objects can not be cleared when
	 * {@link #getCachePolicy(int)} returns a cache with weak reference
	 * semantics. This makes the behavior of the test deterministic. Since we
	 * are holding hard references, the size of the weak reference cache is not
	 * capped and should refelect all objects inserted into the cache and not
	 * yet removed from the cache by the test method.
	 * </p>
	 * 
	 * @see ICachePolicy
	 * @see WeakValueCache#size()
	 * @see WeakValueCache#iterator()
	 * @see WeakValueCache#entryIterator()
	 */
    public void test_maintainsLRUOrder()
    {
        final int CAPACITY = 4;
        ICachePolicy<Long,String> cache = getCachePolicy( CAPACITY );

        long[] oid = new long[] {
          1, 2, 3, 4, 5      
        };
        
        String[] obj = new String[] {
                new String("o1"),
                new String("o2"),
                new String("o3"),
                new String("o4"),
                new String("o5")
        };

        MyCacheListener<Long,String> listener = new MyCacheListener<Long,String>();
        listener.denyEvents(); // listener will deny events.        
        cache.setListener( listener ); // set listener on cache.
        
        /*
         * Insert objects until the cache is full. We verify the total cache
         * ordering after each insert as well as the cache size.
         * 
         * Note: The cache order and the iterator order are from the Least
         * Recently Used to the Most Recently Used. This means that the last
         * element put() into the cache always shows up on the right hand edge
         * of the array used to test the cache ordering. When an element is
         * evicted from the cache it is always the element on the left hand edge
         * of that array.
         * 
         * LRU <- - - - - -> MRU
         */

        cache.put( oid[0], obj[0], true );
        assertEquals("size", 1, cache.size() );
        assertSameIterator("ordering",new Object[]{obj[0]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
        		new CacheEntry<Long, String>( oid[0], obj[0], true)
        		}, cache.entryIterator());

        cache.put( oid[1], obj[1], true );
        assertEquals("size", 2, cache.size() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[1]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
        		new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true), }, cache.entryIterator());

        cache.put( oid[2], obj[2], true );
        assertEquals("size", 3, cache.size() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[1],obj[2]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[2], obj[2], true) }, cache.entryIterator());

        cache.put( oid[3], obj[3], true );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[1],obj[2],obj[3]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[3], obj[3], true) }, cache.entryIterator());

        /*
         * Insert another object into the cache. This should trigger a cache
         * eviction event. We set the expected data for that event on the
         * listener and then verify that the event was received.
         */
        listener.setExpectedEvent( oid[0], obj[0], true );
        cache.put( oid[4], obj[4], true );
        listener.clearLastEvent();
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[1],obj[2],obj[3],obj[4]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[3], obj[3], true),
				new CacheEntry<Long,String>(oid[4], obj[4], true)
				}, cache.entryIterator());

        // another over capacity event.
        listener.setExpectedEvent( oid[1], obj[1], true );
        cache.put( oid[0], obj[0], true );
        listener.clearLastEvent();
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[2],obj[3],obj[4],obj[0]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[3], obj[3], true),
				new CacheEntry<Long,String>(oid[4], obj[4], true),
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				}, cache.entryIterator());

        // touch a cache member and verify the updated ordering.
        listener.denyEvents();
        cache.put( oid[3], obj[3], true );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[2],obj[4],obj[0],obj[3]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[4], obj[4], true),
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[3], obj[3], true),
				}, cache.entryIterator());
        
        // touch the MRU cache member and verify NO update to ordering, but
        // the dirty flag is updated as specified.
        listener.denyEvents();
        cache.put( oid[3], obj[3], false );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[2],obj[4],obj[0],obj[3]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[4], obj[4], true),
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				}, cache.entryIterator());
        
        // touch the LRU cache member and verify update to ordering.
        listener.denyEvents();
        cache.put( oid[2], obj[2], true );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[4],obj[0],obj[3],obj[2]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[4], obj[4], true),
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				}, cache.entryIterator());
        
        // verify another cache eviction now that we have perturbed the order a bit.
        listener.setExpectedEvent( oid[4], obj[4], true );
        cache.put( oid[1], obj[1], true );
        listener.clearLastEvent();
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[3],obj[2],obj[1]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				}, cache.entryIterator());

        // remove a cache entry and verify the new ordering.
        listener.denyEvents();
        cache.remove( oid[3] );
        assertEquals("size", 3, cache.size() );
        assertEquals("capacity", CAPACITY, cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[2],obj[1]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[0], obj[0], true),
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				}, cache.entryIterator());

        // remove another cache entry and verify the new ordering.
        listener.denyEvents();
        cache.remove( oid[0] );
        assertEquals("size", 2, cache.size() );
        assertEquals("capacity", CAPACITY, cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[2],obj[1]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				}, cache.entryIterator());

        // add an entry back into the cache.
        listener.denyEvents();
        cache.put( oid[0], obj[0], false );
        assertEquals("size", 3, cache.size() );
        assertEquals("capacity", CAPACITY, cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[2],obj[1],obj[0]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[0], obj[0], false),
				}, cache.entryIterator());

        // add an entry back into the cache.
        listener.denyEvents();
        cache.put( oid[3], obj[3], false );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[2],obj[1],obj[0],obj[3]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[2], obj[2], true),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[0], obj[0], false),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				}, cache.entryIterator());

        // add an entry back into the cache, causing a cache eviction.
        listener.setExpectedEvent(oid[2], obj[2], true);
        cache.put( oid[4], obj[4], false );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[1],obj[0],obj[3],obj[4]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[0], obj[0], false),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				new CacheEntry<Long,String>(oid[4], obj[4], false),
				}, cache.entryIterator());

        // get MRU object from the cache and verify no update of ordering.
        cache.get( oid[4] );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[1],obj[0],obj[3],obj[4]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[0], obj[0], false),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				new CacheEntry<Long,String>(oid[4], obj[4], false),
				}, cache.entryIterator());

        // get LRU object from the cache and verify update of ordering.
        cache.get( oid[1] );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[3],obj[4],obj[1]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[0], obj[0], false),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				new CacheEntry<Long,String>(oid[4], obj[4], false),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				}, cache.entryIterator());

        // get LRU object from middle of the cache and verify update of ordering.
        cache.get( oid[3] );
        assertEquals("size", 4, cache.size() );
        assertEquals("capacity", cache.size(), cache.capacity() );
        assertSameIterator("ordering",new Object[]{obj[0],obj[4],obj[1],obj[3]},cache.iterator() );
        assertSameEntryOrdering("ordering", new ICacheEntry[] {
				new CacheEntry<Long,String>(oid[0], obj[0], false),
				new CacheEntry<Long,String>(oid[4], obj[4], false),
				new CacheEntry<Long,String>(oid[1], obj[1], true),
				new CacheEntry<Long,String>(oid[3], obj[3], false),
				}, cache.entryIterator());

        // clear the cache and verify state.
        listener.denyEvents();
        cache.clear();
        assertEquals("size", 0, cache.size() );
        assertEquals("capacity", CAPACITY, cache.capacity() );
        assertSameIterator("ordering",new Object[]{},cache.iterator() );
        
    }

}
