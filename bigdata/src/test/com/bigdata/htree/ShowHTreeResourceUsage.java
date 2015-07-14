package com.bigdata.htree;

import java.util.HashMap;
import java.util.UUID;

import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.DefaultTupleSerializer;
import com.bigdata.btree.HTreeIndexMetadata;
import com.bigdata.btree.ITupleSerializer;
import com.bigdata.btree.keys.ASCIIKeyBuilderFactory;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.raba.codec.FrontCodedRabaCoder;
import com.bigdata.btree.raba.codec.SimpleRabaCoder;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.rwstore.sector.MemStore;
import com.bigdata.util.Bytes;

/**
 * The purpose of this class is to show that adding a large number of entries
 * into a MemStore backed HTree provides much less load of the Java heap, reducing
 * GC cost at the same time.
 * 
 * @author Martyn Cutcher
 *
 */
public class ShowHTreeResourceUsage {
	
	public static void main(String[] args) {
	
		ShowHTreeResourceUsage inst = new ShowHTreeResourceUsage();
		
		final int values = 2 * 1000000;
		inst.runTreeMap(values);
		inst.runHTree(values);
		//inst.runHTree(values);
		//inst.runTreeMap(values);
	}

	private void runTreeMap(final int values) {
		final Runtime runtime = Runtime.getRuntime();
		
        System.out.println("TreeMap init used: " + (runtime.totalMemory() - runtime.freeMemory()));
 
        final IKeyBuilder keyBuilder = new KeyBuilder();
		
         HashMap map = new HashMap();
        
        long start = System.currentTimeMillis();

        for (int i = 0; i < values; i++) {
            byte[] kv = keyBuilder.reset().append(i).getKey();
            map.put(kv, kv);
        }
		
        long end1 = System.currentTimeMillis();
        
        long gc1 = timeGC();
        long treeMapFreemem = runtime.totalMemory() - runtime.freeMemory();
        long treeMapTotal = runtime.totalMemory();
        long treeMapMax = runtime.maxMemory();
        long treeMapUsed = treeMapTotal - treeMapFreemem;
        
        final int mapsize = map.size();
        
        map = null;
        Runtime.getRuntime().gc();
        Runtime.getRuntime().gc();
        
        
        System.out.println("TreeMap: " + (end1-start) + "ms, gc: " + gc1 + "ms - Used: " + treeMapUsed + ", total: " + treeMapTotal + ", size(): " + mapsize);
        System.out.println("Final used: " + (runtime.totalMemory() - runtime.freeMemory()) + " GC: " + timeGC());
       
	}
	
	private void runHTree(final int values) {
		final Runtime runtime = Runtime.getRuntime();
		
        final IKeyBuilder keyBuilder = new KeyBuilder();
		
        System.out.println("HTree init used: " + (runtime.totalMemory() - runtime.freeMemory()));

        final MemStore store = new MemStore(DirectBufferPool.INSTANCE);

        final HTree htree = getHTree(store, 8, false/* rawRecords */, 50);
		
        long start2 = System.currentTimeMillis();

        for (int i = 0; i < values; i++) {
            byte[] kv = keyBuilder.reset().append(i).getKey();
            htree.insert(kv, kv);
        }
		
        long end2 = System.currentTimeMillis();
                
        long htreeFreemem = runtime.freeMemory();
        long htreeTotal = runtime.totalMemory();
        long htreeMax = runtime.maxMemory();
        long htreeUsed = htreeTotal - htreeFreemem;

        
        final BTreeCounters counters = htree.getBtreeCounters();
        
        System.out.println("HTree: " + (end2-start2) + "ms, gc: " + timeGC() + "ms - Used: " + htreeUsed + ", total: " + htreeTotal + ", leaves: " + htree.nleaves + ", entries: " + htree.nentries);
       
        store.destroy();
        
        System.out.println("HTree GC after destroy: " + timeGC());
	}

    private long timeGC() {
    	final long start = System.currentTimeMillis();
    	
        for (int i = 0; i < 50; i++) {
            Runtime.getRuntime().gc();
        }
        
		return System.currentTimeMillis() - start;
	}

	private HTree getHTree(final IRawStore store, final int addressBits,
            final boolean rawRecords, final int writeRetentionQueueCapacity) {

        /*
         * TODO This sets up a tuple serializer for a presumed case of 4 byte
         * keys (the buffer will be resized if necessary) and explicitly chooses
         * the SimpleRabaCoder as a workaround since the keys IRaba for the
         * HTree does not report true for isKeys(). Once we work through an
         * optimized bucket page design we can revisit this as the
         * FrontCodedRabaCoder should be a good choice, but it currently
         * requires isKeys() to return true.
         */
        final ITupleSerializer<?,?> tupleSer = new DefaultTupleSerializer(
                new ASCIIKeyBuilderFactory(Bytes.SIZEOF_INT),
                new FrontCodedRabaCoder(4),// Note: reports true for isKeys()!
                // new SimpleRabaCoder(),// keys
                new SimpleRabaCoder() // vals
                );
        
		final HTreeIndexMetadata metadata = new HTreeIndexMetadata(
				UUID.randomUUID());

        if (rawRecords) {
            metadata.setRawRecords(true);
            metadata.setMaxRecLen(0);
        }

        metadata.setAddressBits(addressBits);

        metadata.setTupleSerializer(tupleSer);

        /*
         * Note: A low retention queue capacity will drive evictions, which is
         * good from the perspective of stressing the persistence store
         * integration.
         */
        metadata.setWriteRetentionQueueCapacity(writeRetentionQueueCapacity);
        metadata.setWriteRetentionQueueScan(10); // Must be LTE capacity.

        return HTree.create(store, metadata);

    }

}
