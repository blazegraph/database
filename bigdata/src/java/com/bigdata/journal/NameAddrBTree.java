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
import com.bigdata.objndx.IValueSerializer;
import com.bigdata.objndx.KeyBuilder;
import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * BTree mapping btree names to the last metadata record committed for the named
 * btree. The keys are Unicode strings using the default {@link Locale}. The
 * values are the last known {@link Addr address} of the named btree.
 * 
 * @todo parameterize the {@link Locale} on the Journal and pass through to this
 *       class. this will make it easier to configure non-default locales.
 */
public class NameAddrBTree extends BTree {

    private KeyBuilder keyBuilder = new KeyBuilder();

    /**
     * Cache of added/retrieved btrees. Only btrees found in this cache are
     * candidates for the commit protocol. The use of this cache also minimizes
     * the use of the {@link #keyBuilder} and therefore minimizes the relatively
     * expensive operation of encoding unicode names to byte[] keys.
     */
    private Map<String,BTree> name2BTree = new HashMap<String,BTree>();

    public NameAddrBTree(IRawStore store) {

        super(store, DEFAULT_BRANCHING_FACTOR, ValueSerializer.INSTANCE);

    }

    /**
     * Load from the store.
     * 
     * @param store
     *            The backing store.
     * @param metadataId
     *            The metadata record identifier for the index.
     */
    public NameAddrBTree(IRawStore store, long metadataId) {

        super(store, BTreeMetadata.read(store, metadataId));

    }

    /**
     * Extends the default behavior to cause each named btree to flush
     * itself to the store, updates the {@link Addr address} from which that
     * btree may be reloaded within its internal mapping, and finally
     * flushes itself and returns the address from which this btree may be
     * reloaded.
     */
    public long handleCommit() {

        Iterator<Map.Entry<String,BTree>> itr = name2BTree.entrySet().iterator();
        
        while(itr.hasNext()) {
            
            Map.Entry<String, BTree> entry = itr.next();
            
            String name = entry.getKey();
            
            BTree btree = entry.getValue();
            
            // request commit.
            long addr = btree.handleCommit();
            
            // update persistent mapping.
            insert(name,Long.valueOf(addr));
            
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

    public BTree get(String name) {

        BTree btree = name2BTree.get(name);
        
        if(btree!=null) return btree;
        
        long addr = lookup(name);
        
        if(addr == 0L) return null;
        
        btree = new BTree(this.store,BTreeMetadata.read(this.store, addr));
        
        // save name -> btree mapping in transient cache.
        name2BTree.put(name,btree);
        
        return btree;

    }
    
    protected long lookup(String name) {

        if(name==null) throw new IllegalArgumentException();

        Long addr = (Long) super.lookup(getKey(name));

        if(addr == null) return 0L;

        return addr.longValue();
        
    }

    public void add(String name,BTree btree) {
        
        if(name==null) throw new IllegalArgumentException();
        
        if(btree==null) throw new IllegalArgumentException();

        byte[] key = getKey(name);
        
        if(contains(key)) {
            
            throw new IllegalArgumentException("already registered: "+name);
            
        }
        
        // flush btree to the store to get the metadata record address.
        long addr = btree.write();
        
        // record the address in this tree.
        insert(key,Long.valueOf(addr));
        
        // add name -> btree mapping to the transient cache.
        name2BTree.put(name, btree);
        
    }
    
    /**
     * The value is a <code>long</code> integer that is the term
     * identifier.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
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

        public void getValues(DataInputStream is, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                if (packedLongs) {

                    values[i] = Long.valueOf(LongPacker.unpackLong(is));

                } else {

                    values[i] = Long.valueOf(is.readLong());

                }

            }

        }

        public void putValues(DataOutputStream os, Object[] values, int n)
                throws IOException {

            for (int i = 0; i < n; i++) {

                if (packedLongs) {

                    LongPacker.packLong(os, ((Long) values[i]).longValue());

                } else {

                    os.writeLong(((Long) values[i]).longValue());

                }

            }

        }

    }

}