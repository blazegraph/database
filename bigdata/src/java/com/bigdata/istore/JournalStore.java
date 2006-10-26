package com.bigdata.istore;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.CognitiveWeb.bigdata.OId;

import com.bigdata.journal.ExtensibleSerializer;
import com.bigdata.journal.Journal;
import com.bigdata.journal.TimestampFactory;
import com.bigdata.journal.Tx;

/**
 * Hacked test class exposing the {@link IStore} interface backed by a
 * {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Support notion of transactions on {@link IStore}. In fact, the problem
 *       is more like supporting unisolated operations.
 * 
 * @todo Refine the {@link IStore} API. There is a distinction between what is
 *       good for the store and what is good for applications. For example, are
 *       the raw operations on byte[], ByteBuffer, or Object using extSer?
 * 
 * @todo Integrate object caching.
 * 
 * @todo Add stream-based API. Pragmatic handling is to buffer up to some size
 *       and then to begin a non-transactional write on a large object somewhere
 *       in the bigdata cluster.
 * 
 * @todo Break down into threads for the application with migration to
 *       serialization thread on cache eviction and commit and persistence
 *       thread for store operations. CRUD operations need to use the cache
 *       first and must be enqueued when they read or write through the cache.
 * 
 * @todo Hook up and debug the btree integration.
 * 
 * @todo Hook this up to GOM and test performance with the various journal
 *       backends.
 * 
 * @todo Consider disallowing unisolated store operations (by removing the
 *       IStore interface or simply moving the CRUD operations into ITx).
 */
public class JournalStore implements IStore {

    /**
     * The backing store.
     */
    private final Journal journal;

    /**
     * The next persistent identifier to be assigned.
     *  
     * @todo This is not restart safe.
     */
    private long nextId = 1;

    // @todo probably synchronous or block up ids by tx, etc.
    protected long nextId() {

        return nextId++;

    }

    /**
     * 
     * @todo Make this more flexible in terms of a service vs a static
     *       instance (for journal only to support the object index) vs true
     *       extensibility (for an embedded database). The extser state
     *       should only be shared with the journal's internal extser state
     *       when the journal is used as an embedded database without an
     *       external read-optimized segment. In every other case (and
     *       probably simply in every case) the journal's internal extser
     *       state should be considered private.
     * 
     * @return
     */
    public ExtensibleSerializer getExtensibleSerializer() {

        return journal.getExtensibleSerializer();

    }

    public JournalStore(Journal journal) {

        this.journal = journal;

    }

    public void close() {

        journal.close();

    }

    /**
     * Convert int64 bigdata persistent identifier to an int32 within
     * segment persistent identifier.
     * 
     * @param id
     *            The int64 bigdata persistent identifier.
     *            
     * @return The int32 within segment persistent identifier.
     * 
     * @todo The conversions between int64 bigdata identifiers and int32
     *       within segment journal identifiers are hacked. The code
     *       presumes that the journal is segment0 and just uses casting. It
     *       does not verify that the int64 identifier actually addresses
     *       the segment covered by the journal. Further, there are no test
     *       cases. We should probably be using IOI and {@link OId}.
     */
    private int getId(long id) {

        return (int) id;

    }

    //        /**
    //         * Convert int32 within segment persistent identifier to a bigdata
    //         * persistent identifier.
    //         * 
    //         * @param id
    //         *            The int32 within segment persistent identifier.
    //         * 
    //         * @return The int64 bigdata persistent identifier.
    //         */
    //        private long getOId(int id) {
    //            
    //            return id;
    //            
    //        }

    public long insert(Object obj) {

        long id = nextId();

        try {

            journal.write(getId(id), ByteBuffer.wrap(getExtensibleSerializer()
                    .serialize(0, obj)));

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

        return id;

    }

    public Object read(long id) {

        try {

            return getExtensibleSerializer().deserialize(id,
                    journal.read(getId(id), null).array());

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void update(long id, Object obj) {

        try {

            journal.write(getId(id), ByteBuffer.wrap(getExtensibleSerializer()
                    .serialize(id, obj)));

        } catch (IOException ex) {

            throw new RuntimeException(ex);

        }

    }

    public void delete(long id) {

        journal.delete(getId(id));

    }

    /**
     * FIXME Abstract and integrate with a transaction service. When this is a
     * distributed database, this should start a distributed transaction.
     */
    public ITx startTx() {

        return new StoreTx(this, new Tx(journal, TimestampFactory
                .nextNanoTime()));

    }

    //      public InputStream getInputStream(long id) {
    //      
    //      ByteBuffer tmp = journal.read(getId(id), null);
    //      
    //      return new ByteArrayInputStream(tmp.array());
    //      
    //  }
    //
    //  public StoreOutputStream getOutputStream() {
    //
    //      throw new UnsupportedOperationException();
    //      
    //  }
    //
    //  public StoreOutputStream getUpdateStream(long id) {
    //
    //      throw new UnsupportedOperationException();
    //      
    //  }

    static class StoreTx implements ITx {

        private final JournalStore store;

        private final Tx tx;

        StoreTx(JournalStore store, Tx tx) {

            assert store != null;

            assert tx != null;

            this.store = store;

            this.tx = tx;

        }

        public void abort() {

            tx.abort();

        }

        public void commit() {

            tx.commit();

        }

        public void prepare() {

            tx.prepare();

        }

        public long insert(Object obj) {

            long id = store.nextId();

            try {

                tx.write(store.getId(id), ByteBuffer.wrap(store
                        .getExtensibleSerializer().serialize(0, obj)));

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            return id;

        }

        public Object read(long id) {

            try {

                return store.getExtensibleSerializer().deserialize(id,
                        tx.read(store.getId(id), null).array());

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        }

        public void update(long id, Object obj) {

            try {

                tx.write(store.getId(id), ByteBuffer.wrap(store
                        .getExtensibleSerializer().serialize(id, obj)));

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

        }

        public void delete(long id) {

            tx.delete(store.getId(id));

        }

    }

}
