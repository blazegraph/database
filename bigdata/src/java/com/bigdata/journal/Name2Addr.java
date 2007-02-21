package com.bigdata.journal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import org.CognitiveWeb.extser.LongPacker;

import com.bigdata.objndx.BTree;
import com.bigdata.objndx.BTreeMetadata;
import com.bigdata.objndx.IIndex;
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * BTree mapping index names to the last metadata record committed for the named
 * index. The keys are Unicode strings using the default {@link Locale}. The
 * values are {@link Entry} objects recording the name of the index and the last
 * known {@link Addr address} of the {@link BTreeMetadata} record for the named
 * index.
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
    private KeyBuilder keyBuilder = new KeyBuilder();

    /**
     * Cache of added/retrieved btrees. Only btrees found in this cache are
     * candidates for the commit protocol. The use of this cache also minimizes
     * the use of the {@link #keyBuilder} and therefore minimizes the relatively
     * expensive operation of encoding unicode names to byte[] keys.
     * 
     * @todo use a weak value cache so that unused indices may be swept by the
     *       GC.
     */
    private Map<String,IIndex> name2BTree = new HashMap<String,IIndex>();

    public Name2Addr(IRawStore store) {

        super(store, DEFAULT_BRANCHING_FACTOR, ValueSerializer.INSTANCE);

    }

    /**
     * Load from the store.
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record for the index.
     */
    public Name2Addr(IRawStore store, BTreeMetadata metadata) {

        super(store, metadata);

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
        Iterator<Map.Entry<String,IIndex>> itr = name2BTree.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, IIndex> entry = itr.next();
            
            String name = entry.getKey();
            
            IIndex btree = entry.getValue();
            
            // request commit.
            long addr = ((ICommitter)btree).handleCommit();
            
            // update persistent mapping.
            insert(getKey(name),new Entry(name,addr));
            
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

        IIndex btree = name2BTree.get(name);
        
        if (btree != null)
            return btree;

        final Entry entry = (Entry) super.lookup(getKey(name));

        if (entry == null) {

            return null;
            
        }

        // re-load btree from the store.
        btree = BTreeMetadata.load(this.store, entry.addr);
        
        // save name -> btree mapping in transient cache.
        name2BTree.put(name,btree);

        // return btree.
        return btree;

    }

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
        
        // add name -> btree mapping to the transient cache.
        name2BTree.put(name, btree);
        
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
        name2BTree.remove(name);

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

        /**
         * Note: It is faster to use packed longs, at least on write with
         * test data (bulk load of wordnet nouns).
         */
        final static boolean packedLongs = true;

        public ValueSerializer() {
        }

        public void putValues(DataOutputStream os, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                Entry entry = (Entry) values[i];

                os.writeUTF(entry.name);

                if (packedLongs) {

                    LongPacker.packLong(os, entry.addr);

                } else {

                    os.writeLong(entry.addr);

                }
                
            }

        }
        
        public void getValues(DataInputStream is, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                final String name = is.readUTF();
                
                final long addr;
                
                if (packedLongs) {

                    addr = Long.valueOf(LongPacker.unpackLong(is));

                } else {

                    addr = Long.valueOf(is.readLong());

                }
                
                values[i] = new Entry(name,addr);

            }

        }

    }

}
