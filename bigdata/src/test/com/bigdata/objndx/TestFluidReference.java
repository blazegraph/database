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
 * Created on Nov 8, 2006
 */

package com.bigdata.objndx;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import com.bigdata.cache.HardReferenceCache;
import com.bigdata.cache.HardReferenceCache.HardReferenceCacheEvictionListener;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TestFluidReference extends TestCase {

    /**
     * 
     */
    public TestFluidReference() {
    }

    /**
     * @param arg0
     */
    public TestFluidReference(String arg0) {
    
        super(arg0);

    }

    /**
     * Test creates an object with {@link FluidIdentity} and some
     * {@link FluidReference}s for the object.  The {@link FluidIdentity} is
     * then assigned a key and the test verifies that the {@link FluidReference}s
     * are automatically assigned the same key.
     */
    public void test01() {
        
        PO<Integer> po0 = new PO<Integer>();
        assertFalse( "hasKey", po0.getFluidIdentity().hasKey() );
        assertEquals("backRefCount", 0, po0.getFluidIdentity().getBackReferenceCount());
        
        FluidReference<Integer, PO> ref0 = new FluidReference<Integer, PO>(po0);
        FluidReference<Integer, PO> ref1 = new FluidReference<Integer, PO>(po0);

        /*
         * Set the key and verify that all references were updated to the key.
         * 
         * Note: This explicitly compares keys based on their primitive data
         * type so as to avoid testing on the key references, which could easily
         * be different.  E.g., new Integer(12) != new Integer(12)
         */
        final Integer key = new Integer(12);
        po0.getFluidIdentity().setKey(key);
        assertTrue( key.intValue() == po0.getFluidIdentity().getKey().intValue() );
        assertTrue( key.intValue() == ref0.getKey().intValue() );
        assertTrue( key.intValue() == ref1.getKey().intValue() );
       
    }

    /**
     * Test demonstrates the use of a {@link FluidReference} to recover objects
     * with {@link FluidIdentity} from a {@link Store} following cache eviction.
     */
    public void test02() {
        
        /*
         * Setup the hard reference cache, including a listener that will be
         * notified when reference is evicted and a store on which we will write
         * dirty objects.
         */
        Store<Integer,PO<Integer>> store = new Store<Integer, PO<Integer>>();
        HardReferenceCacheEvictionListener<PO<Integer>> listener = new MyListener<Integer,PO<Integer>>(store);
        int capacity = 3;
        int nscan = 0;
        HardReferenceCache<PO<Integer>> cache = new HardReferenceCache<PO<Integer>>(listener,capacity,nscan);

        PO<Integer> o1 = new PO<Integer>();
        PO<Integer> o2 = new PO<Integer>();
        PO<Integer> o3 = new PO<Integer>();
        PO<Integer> o4 = new PO<Integer>();
        
        // fill the cache to capacity.
        cache.append(o1);
        cache.append(o2);
        cache.append(o3);

        // verify key is not assigned.
        assert ! o1.getFluidIdentity().hasKey();
        
        // verify object is dirty.
        assert o1.isDirty();

        // cause eviction of o1.
        cache.append(o4);

        // verify key is now assigned.
        assert o1.getFluidIdentity().hasKey();
        
        // verify object is clean.
        assert ! o1.isDirty();

        // verify recovery of object from store by key.
        assert store.read(o1.getFluidIdentity().getKey()) != null;
        
        // verify object recovered from the store is clean.
        assert ! store.read(o1.getFluidIdentity().getKey()).isDirty();
        
        // verify object recovered from the store has a distinct reference.
        assert store.read(o1.getFluidIdentity().getKey()) != o1;
        
        // verify object recovered from the store has an assigned key.
        assert store.read(o1.getFluidIdentity().getKey()).getFluidIdentity()
                .hasKey();
        
        // verify object recovered from the store has the correct key.
        assert o1.getFluidIdentity().getKey().equals(
                store.read(o1.getFluidIdentity().getKey()).getFluidIdentity()
                        .getKey());
        
    }

    /**
     * @todo test of btree data structures and cache operations.
     */
    public void test03() {
        
        /*
         * Setup the hard reference cache, including a listener that will be
         * notified when reference is evicted and a store on which we will write
         * dirty objects.
         */
        Store<Integer,PO<Integer>> store = new Store<Integer, PO<Integer>>();
        HardReferenceCacheEvictionListener<PO<Integer>> listener = new MyListener<Integer,PO<Integer>>(store);
        int capacity = 3;
        int nscan = 0;
        HardReferenceCache<PO<Integer>> cache = new HardReferenceCache<PO<Integer>>(listener,capacity,nscan);

        // object keys - set below.
        final Integer k1, k2, k3;
        
        /*
         * Setup the persistent objects and the data structures.
         */
        {
        
            PO<Integer> o1 = new PO<Integer>();
            PO<Integer> o2 = new PO<Integer>();
            PO<Integer> o3 = new PO<Integer>();
            
            /*
             * interconnect o1 as the parent of two leaves (o2,o3) and connect
             * o2 and o3 into a double-linked list of leaves.
             */
            o1.children.add(new FluidReference<Integer, PO<Integer>>(o2));
            o1.children.add(new FluidReference<Integer, PO<Integer>>(o3));
            o2.next = new FluidReference<Integer, PO<Integer>>(o3);
//            o3.prior = new FluidReference<Integer, PO<Integer>>(o2);

            // fill the cache to capacity.
            cache.append(o3);
            cache.append(o2);
            cache.append(o1);
           
            /*
             * Evict all objects in the cache, clearing hard references from the
             * cache and causing keys to be assigned.
             * 
             * Note: Keys have to be assigned in an order defining a directed
             * graph. For a B+Tree data structure, this means that we have to
             * assign keys to the leaves first. There is no way to do this with
             * both prior and next leaf references since that defines a graph
             * with cycles, so we only store the next reference. We handle the
             * key assignment order in this unit test by adding the nodes to the
             * cache in the order in which we want to assign the keys. However,
             * this is not a general purpose solution.
             */
            cache.evictAll(true);

            /*
             * Get the keys for the objects so that we can recover them again.
             */
            k1 = o1.getFluidIdentity().getKey();
            k2 = o1.getFluidIdentity().getKey();
            k3 = o2.getFluidIdentity().getKey();
            
            /*
             * Prompt garbage collection.
             * 
             * Note: Nothing guarentees that the weak references will actually
             * be cleared and we do not depend on those references in this test
             * to recover the referent when the key is defined.
             */
            System.gc();
            
        }
        
        /*
         * Verify that we can recover the objects using their _keys_.
         */
        { 
            
            PO<Integer> o1 = store.read(k1);
            PO<Integer> o2 = store.read(k2);
            PO<Integer> o3 = store.read(k3);
            
            assert o1.getFluidIdentity().getKey() == k1;
            assert o2.getFluidIdentity().getKey() == k2;
            assert o3.getFluidIdentity().getKey() == k3;
            
            assert ! o1.isDirty();
            assert ! o2.isDirty();
            assert ! o3.isDirty();

        }
        
        /*
         * Verify that we can recover o1 using its key and then recover o2 and
         * o3 from the references on o1.
         */
        {

            PO<Integer> o1 = store.read(k1);
            PO<Integer> o2 = o1.children.get(0).get();
            PO<Integer> o3 = o1.children.get(0).get();
            
            assert o1.getFluidIdentity().getKey() == k1;
            assert o2.getFluidIdentity().getKey() == k2;
            assert o3.getFluidIdentity().getKey() == k3;
            
            assert ! o1.isDirty();
            assert ! o2.isDirty();
            assert ! o3.isDirty();

            assert o2.next.get() == o3;
//            assert o3.prior.get() == o3;
            
        }
        
    }

    /**
     * Persistence store.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     * @param <T>
     */
    public static class Store<K,T extends PO<K>> {
        
        /**
         * Key factory.
         */
        private int nextKey;

        /**
         * "Persistence store" - access to objects by the key.
         */
        private final Map<K,byte[]> store = new HashMap<K,byte[]>();

        private byte[] serialize(T po) {
            
            try {
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(po);
                oos.flush();
                oos.close();
                byte[] bytes = baos.toByteArray();
                return bytes;

            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
        }
        
        private T deserialize(byte[] bytes) {

            try {

                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais);
                T po = (T) ois.readObject();
                return po;

            } catch (Exception ex) {

                throw new RuntimeException(ex);
            }

        }
        
        public K nextId() {
            
            /*
             * Note: The use of generics breaks down here since we need a
             * primitive data type to serve as the value space for the key
             * factory and a means to assign new keys within that value space.
             * This can not be defined "generically", but it rather data type
             * and store semantics specific.
             */
            K key = (K) new Integer(nextKey++);
            
            return key;
            
        }
        
        public T read(K key) {
            
            byte[] bytes = store.get(key);
            
            if (bytes == null)
                throw new IllegalArgumentException("Not found: key=" + key);

            T value = deserialize(bytes);
            
            value.id = new FluidIdentity<K, IIdentityAccess>();
            
            value.id.setKey(key); // set the key - no back references exist yet.
            
            value.setDirty(false); // just read from the store.
            
            return value;
            
        }

        public void write(K key,T value) {
            
            byte[] bytes = serialize(value);
            
            store.put(key, bytes);

            value.setDirty(false); // just wrote on the store.

        }

        // Note: Must also mark the object as invalid.
        public void delete(K key ) {
            
            store.remove( key );

        }
        
    }

    /**
     * Cache eviction listener.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <K>
     *            The key type.
     * @param <T>
     *            The value type.
     */
    public static class MyListener<K,T extends PO<K>> implements HardReferenceCacheEvictionListener<T> {

        /**
         * Persistence store.
         */
        private final Store<K,T> store;
        
        public MyListener(Store<K,T> store) {
            
            assert store != null;
            
            this.store = store;
            
        }
        
        public void evicted(HardReferenceCache<T> cache, T ref) {
            
            /*
             * Assign a persistent identitifer to the evicted reference.
             */

            final K key = store.nextId();
            
            ref.getFluidIdentity().setKey(key);

            store.write(key,ref);
            
        }
        
    }
    
    /**
     * An interface that declares how we access the {@link FluidIdentity} of an
     * object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IIdentityAccess<K,V> {
    
        public FluidIdentity<K,? extends V> getFluidIdentity();
        
    }
    
    /**
     * An interface that declares how we access the dirty state of an object.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface IDirty {
       
        public void setDirty(boolean dirty);
        
        public boolean isDirty();
        
    }
    
    /**
     * An object with a fluid identity.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class PO<K> implements IIdentityAccess, IDirty, Externalizable {
       
        transient private static final long serialVersionUID = 1L;

        /**
         * New objects are considered to be dirty. When an object is
         * deserialized from the store the dirty flag MUST be explicitly
         * cleared.
         */
        transient private boolean dirty = true;

        /**
         * The identity is transient.  It must be restored when the object is
         * deserialized from the store.
         */
        transient FluidIdentity<K, IIdentityAccess> id = new FluidIdentity<K, IIdentityAccess>();

        public FluidIdentity<K, IIdentityAccess> getFluidIdentity() {

            return id;
            
        }

        public boolean isDirty() {
            
            return dirty;
            
        }

        public void setDirty(boolean dirty) {
            
            this.dirty = dirty;
            
        }
        
        /**
         * The keys of the children are serialized, but not the
         * {@link FluidReference}s themselves.
         */
        transient ArrayList<FluidReference<K, PO<K>>> children = new ArrayList<FluidReference<K, PO<K>>>();

        /**
         * The key of the next leaf is serialized, but not the
         * {@link FluidReference} itself.
         * 
         * Note: There is no prior reference since that would form a cycle in
         * the BTree graph and we can not assign keys to a graph with cycles.
         */
        transient FluidReference<K, PO<K>> next;

        /**
         * Note: This is fat due to the use of generics since the key
         * type is not known.
         */
        public void writeExternal(ObjectOutput out) throws IOException {
            final int nchildren = children.size();
            out.writeInt(nchildren);
            for( int i=0; i<nchildren; i++ ) {
                out.writeObject(children.get(i).getKey());
            }
            if( next != null ) {
                out.writeObject(next.getKey());
            } else {
                out.writeObject(null);
            }
        }
        
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            int nchildren = in.readInt();
            children.ensureCapacity(nchildren);
            for( int i=0; i<nchildren; i++ ) {
                /* FIXME We can not restore the reference until and unless we
                 * read the node from the store.  Further, we can not create a
                 * FluidReference until and unless we have a reference.  And
                 * since we do NOT want to read from the store eagerly, we are
                 * stuck with only being able to restore the key and that means
                 * that we need a separate data structure to cache the mapping
                 * from the key to the node.
                 */
//                children.add(new FluidReference<K, PO<K>>)
                throw new UnsupportedOperationException();
            }
        }

    }
    
    /**
     * <p>
     * A weak reference to an object that has not yet been assigned a key,
     * typically a persistent identifier for the referenced object. The key
     * typically serves as a persistent identifier for the object. Once the key
     * is assigned the identity of the object has been fixed. This class is
     * designed to be used with a hard reference cache (to ensure that the
     * referent remains strongly reachable), a protocol for determining whether
     * or not the referenced object has been modified (is "dirty"), and a
     * persistence layer on which the state of dirty objects are written.
     * </p>
     * <p>
     * The use of this class is indicated when a key can not be assigned to an
     * object at its creation. This can occur with persistence by reachability
     * schemes, but also with low-level support for copy-on-write MVCC (in the
     * object index itself).
     * </p>
     * <p>
     * A persistent identifier is assigned to a node in the object index only
     * when the object has been serialized and written onto the store, but the
     * object itself becomes immutable once serialized. Therefore when an object
     * is newly allocated we need to have a hard reference. Yet, once the object
     * has been persisted, we need to convert hard references to weak references
     * so that garbage collect can prevent the entire data structure from being
     * wired into memory. At the same time, when the object is persisted we need
     * to be able to recover the object from its persistent identifier - an
     * identifier that is not assigned until the object is actually written onto
     * storage.
     * </p>
     * <p>
     * The design works by combining a {@link WeakReference} and a persistent
     * identifier (known as a key) within a {@link FluidReference}. Initially
     * the key is undefined and the object is only recoverable by its weak
     * reference. To ensure that the object is not simply swept by the garbage
     * collector, a hard reference to the object in placed on an LRU. If a dirty
     * object is evicted from the LRU, then we write it on the store and assign
     * its key. At the same time, we need to notify the objects holding
     * {@link FluidReference}s that the key has been assigned, which we manage
     * with back references. Once the key is assigned, the object is now
     * recoverable using that key from the weak reference cache (until its
     * reference is cleared) and from the persistence layer. A hard reference to
     * the object is held during the cache eviction operation to ensure that the
     * object remains strongly reachable during eviction. After eviction the
     * object may still be strongly reachable in which case the
     * {@link FluidReference} can still be resolved. However, if the object ever
     * becomes weakly reachable and its references get cleared, then it is still
     * recoverable using its assigned key.
     * </p>
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * @param <T>
     *            The key type.
     * @param <V>
     *            The referent (value) type.
     * 
     * @todo There is doubtless an optimization with the #of back references to
     *       be stored. For the btree, there will be at most 3 references (the
     *       use case is a leaf with a parent and both prior and next leaves).
     *       All other use cases have at most one back reference (the parent).
     *       This optimization could be handled by a subclass that sets the
     *       initial array capacity. 
     * 
     * @todo It would be perfectly possible to have a hard reference field on
     *       this class as well that is only cleared when the key is set.
     *       However, we need to have a hard reference LRU cache in any case to
     *       handle eviction onto the persistence store so there is already
     *       going to be a hard reference and we don't need to store it here.
     */
    public static class FluidReference<K,V extends IIdentityAccess> extends WeakReference<V> { //implements Externalizable {

//        /**
//         * 
//         */
//        private static final long serialVersionUID = -4212977263036363965L;
        
        /**
         * The key is assigned only when the referent is made persistent since
         * that is when the persistent identity is defined.
         * 
         * @serial
         */
        private K key;
        
        public FluidReference(V ref) {

            super(ref);
            
            // Set the back reference.
            ref.getFluidIdentity().addBackReference(this);
            
        }

        public FluidReference(V ref, ReferenceQueue<? super V> q) {

            super(ref,q);
            
            // Set the back reference.
            ref.getFluidIdentity().addBackReference(this);
            
        }

        /**
         * Set the key. This is invoked by
         * 
         * @param key
         *            The key.
         * 
         * @throws IllegalStateException
         *             If the key has already been set.
         */
        void setKey(K key) throws IllegalStateException {
        
            if( key == null ) throw new IllegalArgumentException();
            
            if( this.key != null ) throw new IllegalStateException();
            
            this.key = key;
            
        }

        /**
         * The key.
         * 
         * @return The key.
         * 
         * @exception IllegalStateException
         *                if the key has not been set.
         */
        public K getKey() {
            
            if( key == null ) throw new IllegalStateException();
            
            return key;
            
        }
        
        /**
         * True iff the key is defined.
         */
        public boolean hasKey() {
            
            return key != null;
            
        }

        /**
         * Resolve the referent. The referent is resolved using the weak
         * reference unless that reference has been cleared, in which case it is
         * resolved against the store.
         * 
         * @param store
         *            The store.
         * 
         * FIXME Neither this method or {@link Store} are using a weak reference
         * cache. Reads normally go through the cache and objects are inserted
         * into the cache on a miss once they have been read from the store.
         */
        public V get(Store<K,PO<K>> store) {
            
            V val = get();
            
            if( val == null ) {
                
                val = (V) store.read( getKey() );
                
            }
            
            return val;
            
        }

//        /**
//         * Write the key.
//         * 
//         * @todo This is wildly inefficient due to the use of generics. We need
//         *       to do something like test on the type of the key.
//         * 
//         * @exception IllegalStateException
//         *                if the key has not been assigned.
//         */
//        public void writeExternal(ObjectOutput out) throws IOException, IllegalStateException {
//            
//            if( key == null ) throw new IllegalStateException();
//            
//            out.writeObject(key);
//        }
//        
//        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//
//            key = (K) in.readObject();
//            
//        }

    }

    /**
     * An object that encapsulates either the persistent identity (key
     * value) or back references before the persisent identity is defined.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     * @version $Id$
     * @param <K> The key type.
     * @param <V> The reference (value) type.
     */
    public static class FluidIdentity<K,V extends IIdentityAccess> {

        private K key;
        private ArrayList<FluidReference<K,V>> backRefs;
        
        /**
         * @todo make this a parameter.
         */
        public static final int initialCapacity = 3;
        
        /**
         * Return the key.
         * 
         * @return The key.
         * 
         * @throws IllegalStateException
         *             if the persistent identity has not been defined.
         */
        public K getKey() throws IllegalStateException {
           
            if( key == null ) throw new IllegalStateException();
            
            return key;
            
        }

        /**
         * Set the key. This automatically sets the key on all back references.
         * 
         * @param key
         *            The key.
         * 
         * @throws IllegalStateException
         *             if the key has already been defined.
         */
        public void setKey(K key ) throws IllegalStateException {

            if( key == null ) throw new IllegalArgumentException();
            
            if( this.key != null ) throw new IllegalStateException();
            
            this.key = key;
            
            if (backRefs != null) {

                final int size = backRefs.size();

                for (int i = 0; i < size; i++) {

                    FluidReference<K, V> backRef = backRefs.get(i);

                    backRef.setKey(key);

                }

            }
            
        }
        
        public boolean hasKey() {
            
            return key != null;
            
        }

        /**
         * Add a back reference. The key is set on the back reference iff it is
         * defined. Otherwise the key will be set when it becomes defined.
         * 
         * @param backRef
         *            The back reference.
         */
        void addBackReference(FluidReference<K, V> backRef) {
            
            assert backRef != null;
            
            if( backRefs == null ) {
                
                backRefs = new ArrayList<FluidReference<K, V>>(initialCapacity);
                
            }
            
            // @todo verify reference not already in list.
            backRefs.add(backRef);
            
        }
        
        public int getBackReferenceCount() {
            
            return backRefs == null ? 0 : backRefs.size();
            
        }
        
        public Iterator<FluidReference<K,V>> backReferences() {
            
            if( backRefs == null ) {
                
                return Collections.EMPTY_LIST.iterator();
                
            }
            
            return backRefs.iterator();
            
        }

        // Note: There is no way to make this definition of equals symmetric.
//        /**
//         * Equality exists iff this is the same {@link FluidIdentity} or iff
//         * the key is defined and the key compares as equal with the specified
//         * object.
//         */
//        public boolean equals(Object o) {
//            
//            assert o != null;
//            
//            if( this == o ) return true; // same object.
//            
//            if( this.key != null && this.key.equals(o) ) {
//                
//                // key defined and keys compare as equal.
//                
//                return true;
//                
//            }
//            
//            return false;
//            
//        }
        
    }
    
}
