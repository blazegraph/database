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
package com.bigdata.journal;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.CognitiveWeb.extser.ShortPacker;

import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.IValueSerializer;
import com.bigdata.btree.UnicodeKeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * BTree mapping index names to the last metadata record committed for the named
 * index. The keys are Unicode strings using the default {@link Locale}. The
 * values are {@link Entry} objects recording the name of the index and the last
 * known {@link Addr address} of the {@link BTreeMetadata} record for the named
 * index.
 * <p>
 * Note: The {@link Journal} maintains an instance of this class that evolves
 * with each {@link Journal#commit()}. However, the journal also makes use of
 * historical states for the {@link Name2Addr} index in order to resolve the
 * historical state of a named index. Of necessity, the {@link Name2Addr}
 * objects used for this latter purpose MUST be distinct from the evolving
 * instance otherwise the current version of the named index would be resolved.
 * Note further that the historical {@link Name2Addr} states are accessed using
 * a canonicalizing mapping but that current evolving {@link Name2Addr} instance
 * is NOT part of that mapping.
 */
public class Name2Addr extends BTree {

    /*
     * @todo parameterize the {@link Locale} on the Journal and pass through to
     * this class. this will make it easier to configure non-default locales.
     * 
     * @todo refactor a default instance of the keybuilder that is wrapped in a
     * synchonization interface for multi-threaded use or move an instance onto
     * the store since it already has a single-threaded contract for its api?
     */
    private UnicodeKeyBuilder keyBuilder = new UnicodeKeyBuilder();

    /**
     * Cache of added/retrieved btrees by _name_. This cache is ONLY used by the
     * "live" {@link Name2Addr} instance. Only the indices found in this cache
     * are candidates for the commit protocol. The use of this cache also
     * minimizes the use of the {@link #keyBuilder} and therefore minimizes the
     * relatively expensive operation of encoding unicode names to byte[] keys.
     * 
     * FIXME This never lets go of an unisolated index once it has been looked
     * up and placed into this cache. Therefore, modify this class to use a
     * combination of a weak value cache (so that we can let go of the
     * unisolated named indices) and a commit list (so that we have hard
     * references to any dirty indices up to the next invocation of
     * {@link #handleCommit()}. This will require a protocol by which we notice
     * writes on the named index, probably using a listener API so that we do
     * not have to wrap up the index as a different kind of object.
     */
    private Map<String, IIndex> indexCache = new HashMap<String, IIndex>();
    
//    protected final Journal journal;
    
    public Name2Addr(IRawStore store) {

        super(store, DEFAULT_BRANCHING_FACTOR, UUID.randomUUID(),
                ValueSerializer.INSTANCE);

//        this.journal = store;
        
    }

    /**
     * Load from the store (de-serialization constructor).
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record for the index.
     */
    public Name2Addr(IRawStore store, BTreeMetadata metadata) {

        super(store, metadata);

//        this.journal = store;
        
    }

    /**
     * Extends the default behavior to cause each named btree to flush
     * itself to the store, updates the {@link Addr address} from which that
     * btree may be reloaded within its internal mapping, and finally
     * flushes itself and returns the address from which this btree may be
     * reloaded.
     */
    public long handleCommit() {

        /*
         * This iterator that visits only the entries for named btree that have
         * been touched since the journal was last opened. We can get away with
         * visiting this rather than all named btrees registered with the
         * journal since only trees that have been touched can have data for the
         * current commit.
         */
        Iterator<Map.Entry<String,IIndex>> itr = indexCache.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IIndex> entry = itr.next();
            
            String name = entry.getKey();
            
            IIndex btree = entry.getValue();
            
            // request commit.
            long addr = ((ICommitter)btree).handleCommit();
            
            // update persistent mapping.
            insert(getKey(name),new Entry(name,addr));
            
//            // place into the object cache on the journal.
//            journal.touch(addr, btree);
            
        }
        
        // and flushes out this btree as well.
        return super.handleCommit();
        
    }
    
    /**
     * Encodes a unicode string into a key.
     * 
     * @param name
     *            The name of the btree.
     *            
     * @return The corresponding key.
     */
    protected byte[] getKey(String name) {

        return keyBuilder.reset().append(name).getKey();

    }

    /**
     * Return the named index - this method tests a cache of the named btrees
     * and will return the same instance if the index is found in the cache.
     * 
     * @param name
     *            The index name.
     * 
     * @return The named index or <code>null</code> iff there is no index with
     *         that name.
     */
    public IIndex get(String name) {

        IIndex btree = indexCache.get(name);
        
        if (btree != null)
            return btree;

        final Entry entry = (Entry) super.lookup(getKey(name));

        if (entry == null) {

            return null;
            
        }

//        /*
//         * Reload the index from the store using the object cache to ensure a
//         * canonicalizing mapping.
//         */
//        btree = journal.getIndex(entry.addr);
        
        // re-load btree from the store.
        btree = BTree.load(this.store, entry.addr);
        
        // save name -> btree mapping in transient cache.
        indexCache.put(name,btree);

        // report event (loaded btree).
        ResourceManager.openUnisolatedBTree(name);

        // return btree.
        return btree;

    }
    
    /**
     * Return the {@link Addr address} from which the historical state of the
     * named index may be loaded.
     * <p>
     * Note: This is a lower-level access mechanism that is used by
     * {@link Journal#getIndex(String, ICommitRecord)} when accessing historical
     * named indices from an {@link ICommitRecord}.
     * 
     * @param name
     *            The index name.
     * 
     * @return The address or <code>0L</code> if the named index was not
     *         registered.
     */
    protected long getAddr(String name) {

        /*
         * Note: This uses a private cache to reduce the Unicode -> key
         * translation burden. We can not use the normal cache since that maps
         * the name to the index and we have to return the address not the index
         * in order to support a canonicalizing mapping in the Journal.
         */
        synchronized (addrCache) {

            Long addr = addrCache.get(name);

            if (addr == null) {

                final Entry entry = (Entry) super.lookup(getKey(name));

                if (entry == null) {

                    addr = 0L;
                    
                } else {
                    
                    addr = entry.addr;
                    
                }

                addrCache.put(name, addr);
                
            }

            return addr;

        }

    }
    /**
     * A private cache used only by {@link #getAddr(String)}.
     */
    private HashMap<String/* name */, Long/* Addr */> addrCache = new HashMap<String, Long>();

    /**
     * Add an entry for the named index.
     * 
     * @param name
     *            The index name.
     * 
     * @param btree
     *            The index.
     * 
     * @exception IllegalArgumentException
     *                if <i>name</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if <i>btree</i> is <code>null</code>.
     * @exception IllegalArgumentException
     *                if there is already an index registered under that name.
     */
    public void add(String name,IIndex btree) {
        
        if (name == null)
            throw new IllegalArgumentException();

        if (btree == null)
            throw new IllegalArgumentException();
        
        if( ! (btree instanceof ICommitter) ) {
            
            throw new IllegalArgumentException("Index does not implement: "
                    + ICommitter.class);
            
        }

        final byte[] key = getKey(name);
        
        if(super.contains(key)) {
            
            throw new IllegalArgumentException("already registered: "+name);
            
        }
        
        // flush btree to the store to get the metadata record address.
        final long addr = ((ICommitter)btree).handleCommit();
        
        // add an entry to the persistent index.
        super.insert(key,new Entry(name,addr));
        
//        // touch the btree in the journal's object cache.
//        journal.touch(addr, btree);
        
        // add name -> btree mapping to the transient cache.
        indexCache.put(name, btree);
        
    }

    /**
     * @param name
     *
     * @todo mark the index as invalid
     */
    public void dropIndex(String name) {

        if (name == null)
            throw new IllegalArgumentException();

        final byte[] key = getKey(name);
        
        if(!super.contains(key)) {
            
            throw new IllegalArgumentException("Not registered: "+name);
            
        }
        
        // remove the name -> btree mapping to the transient cache.
        indexCache.remove(name);

        // remove the entry from the persistent index.
        super.remove(key);

    }
    
    /**
     * An entry in the persistent index.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Entry {
       
        /**
         * The name of the index.
         */
        public final String name;
        
        /**
         * The {@link Addr address} of the last known {@link BTreeMetadata}
         * record for the index with that name.
         */
        public final long addr;
        
        public Entry(String name,long addr) {
            
            this.name = name;
            
            this.addr = addr;
            
        }
        
    }
    
    /**
     * The values are {@link Entry}s.
     *
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class ValueSerializer implements IValueSerializer {

        private static final long serialVersionUID = 6428956857269979589L;

        public static transient final IValueSerializer INSTANCE = new ValueSerializer();

        final public static transient short VERSION0 = 0x0;

        public ValueSerializer() {
        }

        public void putValues(DataOutputBuffer os, Object[] values, int n)
                throws IOException {

            os.packShort(VERSION0);

            for (int i = 0; i < n; i++) {

                Entry entry = (Entry) values[i];

                os.writeUTF(entry.name);

                Addr.pack(os, entry.addr);

//                if (packedLongs) {
//
//                    LongPacker.packLong(os, entry.addr);
//
//                } else {
//
//                    os.writeLong(entry.addr);
//
//                }
                
            }

        }
        
        public void getValues(DataInput is, Object[] values, int n)
                throws IOException {

            final short version = ShortPacker.unpackShort(is);
            
            if (version != VERSION0)
                throw new RuntimeException("Unknown version: " + version);

            for (int i = 0; i < n; i++) {

                final String name = is.readUTF();
                
                final long addr;
                
//                if (packedLongs) {
//
//                    addr = Long.valueOf(LongPacker.unpackLong(is));
//
//                } else {
//
//                    addr = Long.valueOf(is.readLong());
//
//                }
                
                addr = Addr.unpack(is);
                
                values[i] = new Entry(name,addr);

            }

        }

    }

}
