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
package com.bigdata.istore;

import com.bigdata.journal.Journal;

/**
 * Hacked test class exposing the {@link IOM} interface backed by a
 * {@link Journal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo GOM uses per-object indices. Once they span a single journal they will
 *       have to be registered with the metadata index. However it would be nice
 *       to avoid that overhead when the index is small and can be kept "near"
 *       the generic object owing the index.
 *       
 * @todo Support notion of transactions on {@link IOM}. In fact, the problem is
 *       more like supporting unisolated operations.
 * 
 * @todo Refine the {@link IOM} API. There is a distinction between what is good
 *       for the store and what is good for applications. For example, are the
 *       raw operations on byte[], ByteBuffer, or Object using extSer?
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
 * @todo Hook up and debug the btree integration. Figure out how to handle the
 *       per-link set indices for GOM. One way is to use a single index for all
 *       link sets in a family and generate the keys so as to partition the
 *       indices. There are doubtless other solutions. Pay attention to both
 *       small and large indices.
 * 
 * @todo Hook this up to GOM and test performance with the various _journal
 *       backends.
 * 
 * @todo Consider disallowing unisolated store operations (by removing the IOM
 *       interface or simply moving the CRUD operations into ITx).
 */
abstract public class JournalStore implements IStore {

//    /**
//     * The backing store.
//     */
//    private final Journal _journal;
//    
//    /**
//     * The unisolated object manager.
//     */
//    private final IOM _om;
//
//    /**
//     * The next persistent identifier to be assigned.
//     *  
//     * @todo This is not restart safe.
//     */
//    private long _nextId = 1;
//
//    // @todo probably synchronous _definately_ atomic, could block up ids by tx,
//    // etc.
//    protected long nextId() {
//
//        return _nextId++;
//
//    }
//
//    public JournalStore(Properties properties) throws IOException {
//
//        this._journal = new Journal(properties);
//        
//        this._om = new OM(this,_journal);
//
//    }
//
//    public void close() {
//
//        _journal.close();
//
//    }
//
//    /**
//     * Convert int64 bigdata persistent identifier to an int32 within
//     * segment persistent identifier.
//     * 
//     * @param id
//     *            The int64 bigdata persistent identifier.
//     *            
//     * @return The int32 within segment persistent identifier.
//     * 
//     * @todo The conversions between int64 bigdata identifiers and int32
//     *       within segment _journal identifiers are hacked. The code
//     *       presumes that the _journal is segment0 and just uses casting. It
//     *       does not verify that the int64 identifier actually addresses
//     *       the segment covered by the _journal. Further, there are no test
//     *       cases. We should probably be using IOI and {@link OId}.
//     */
//    private int getId(long id) {
//
//        return (int) id;
//
//    }
//
//    //        /**
//    //         * Convert int32 within segment persistent identifier to a bigdata
//    //         * persistent identifier.
//    //         * 
//    //         * @param id
//    //         *            The int32 within segment persistent identifier.
//    //         * 
//    //         * @return The int64 bigdata persistent identifier.
//    //         */
//    //        private long getOId(int id) {
//    //            
//    //            return id;
//    //            
//    //        }
//
//    public IOM getObjectManager() {
//        
//        return _om;
//        
//    }
//    
//    public boolean isOpen() {
//        
//        return _journal.isOpen();
//        
//    }
//    
//    static class OM implements IOM {
//        
//        private final IIndexStore store;
//        private final Journal journal;
//
//        /**
//         * The name of the B+-Tree used to store objects. Its keys are long
//         * integers. Its values are the serialized objects.
//         */
//        private final static transient String OBJNDX = "_objndx";
//        
//        // FIXME This is NOT restart safe :-)
//        final private OMExtensibleSerializer _extSer;
//        
//        OM(IIndexStore store,Journal journal) {
//        
//            assert store != null;
//
//            assert journal != null;
//            
//            this.store = store;
//            
//            this.journal = journal;
//            
//            _extSer = new OMExtensibleSerializer(this);
//
//        }
//        
//        // @todo place _nextId on an abstract class: AbstractStore?
//        private long nextId() {
//
//            return ((JournalStore)store).nextId();
//
//        }
//        
//        // @todo place getId on an abstract class: AbstractStore?
//        private int getId(long id) {
//            
//            return ((JournalStore)store).getId(id);
//            
//        }
//
//        /**
//         * FIXME This is NOT restart safe. We need to store the state on the
//         * _journal and cache the location of the state in the root block.
//         * 
//         * FIXME In order to support transactions, we will need to be able to
//         * wrap the extser instance with one that resolves the IOM of the tx
//         * rather than the unisolated IOM.
//         * 
//         * FIXME Make this more flexible in terms of a service vs a static
//         * instance (for _journal only to support the object index) vs true
//         * extensibility (for an embedded database).
//         * 
//         * @return
//         */
//        public OMExtensibleSerializer getExtensibleSerializer() {
//
//            return _extSer;
//
//        }
//        
//        public long insert(Object obj) {
//
//            long id = nextId();
//
//            try {
//
//                journal.write(getId(id), ByteBuffer.wrap(getExtensibleSerializer()
//                        .serialize(0, obj)));
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//            return id;
//
//        }
//
//        public Object read(long id) {
//
//            try {
//
//                return getExtensibleSerializer().deserialize(id,
//                        journal.read(getId(id), null).array());
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            } catch( DataDeletedException ex ) {
//                
//                return null;
//                
//            }
//
//        }
//
//        public void update(long id, Object obj) {
//
//            try {
//
//                journal.write(getId(id), ByteBuffer.wrap(getExtensibleSerializer()
//                        .serialize(id, obj)));
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//        }
//
//        public void delete(long id) {
//
//            journal.delete(getId(id));
//
//        }
//
//    }
//    
//    /**
//     * FIXME Abstract and integrate with a transaction service. When this is a
//     * distributed database, this should start a distributed transaction.
//     */
//    public ITx startTx() {
//
//        return new OMTx(this, new Tx(_journal, TimestampFactory
//                .nextNanoTime()));
//
//    }
//
//    //      public InputStream getInputStream(long id) {
//    //      
//    //      ByteBuffer tmp = _journal.read(getId(id), null);
//    //      
//    //      return new ByteArrayInputStream(tmp.array());
//    //      
//    //  }
//    //
//    //  public StoreOutputStream getOutputStream() {
//    //
//    //      throw new UnsupportedOperationException();
//    //      
//    //  }
//    //
//    //  public StoreOutputStream getUpdateStream(long id) {
//    //
//    //      throw new UnsupportedOperationException();
//    //      
//    //  }
//
//    static class OMTx implements ITx {
//
//        private final IIndexStore store;
//
//        private final Tx tx;
//        
//        private final TxExtensibleSerializer extser;
//
//        OMTx(IIndexStore store, Tx tx) {
//
//            assert store != null;
//
//            assert tx != null;
//
//            this.store = store;
//
//            this.tx = tx;
//
//            this.extser = new TxExtensibleSerializer(this,
//                    (OMExtensibleSerializer) store.getObjectManager()
//                            .getExtensibleSerializer());
//            
//        }
//
//        // @todo place _nextId on an abstract class: AbstractStore?
//        private long nextId() {
//
//            return ((JournalStore)store).nextId();
//
//        }
//        
//        // @todo place getId on an abstract class: AbstractStore?
//        private int getId(long id) {
//            
//            return ((JournalStore)store).getId(id);
//            
//        }
//        
//        public IOM getRootObjectManager() {
//            
//            return store.getObjectManager();
//            
//        }
//        
//        /**
//         * @todo Non-transactional, cached, immutable writes _and_ wraps the ITx
//         *       so that deserialization has access to both the oid and the
//         *       transactional object manager context.
//         * 
//         * @return
//         */
//        public IOMExtensibleSerializer getExtensibleSerializer() {
//            
//            return extser;
//            
//        }
//
//        public void abort() {
//
//            tx.abort();
//
//        }
//
//        public void commit() {
//
//            tx.prepare();
//            
//            tx.commit();
//
//        }
//
//        public long insert(Object obj) {
//
//            long id = nextId();
//
//            try {
//
//                tx.write(getId(id), ByteBuffer.wrap(getExtensibleSerializer()
//                        .serialize(0, obj)));
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//            return id;
//
//        }
//
//        public Object read(long id) {
//
//            try {
//
//                return getExtensibleSerializer().deserialize(id,
//                        tx.read(getId(id), null).array());
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//        }
//
//        public void update(long id, Object obj) {
//
//            try {
//
//                tx.write(getId(id), ByteBuffer.wrap(getExtensibleSerializer()
//                        .serialize(id, obj)));
//
//            } catch (IOException ex) {
//
//                throw new RuntimeException(ex);
//
//            }
//
//        }
//
//        public void delete(long id) {
//
//            tx.delete(getId(id));
//
//        }
//
//    }

}
