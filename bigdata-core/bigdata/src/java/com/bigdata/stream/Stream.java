/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
 * Created on Jun 29, 2012
 */
package com.bigdata.stream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.BigdataStatics;
import com.bigdata.bop.solutions.SolutionSetStream;
import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.AbstractNode;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeCounters;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.ICheckpointProtocol;
import com.bigdata.btree.IDirtyListener;
import com.bigdata.btree.IReadWriteLockManager;
import com.bigdata.btree.IndexInconsistentError;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.IndexTypeEnum;
import com.bigdata.btree.Node;
import com.bigdata.btree.ReadWriteLockManager;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.OneShotInstrument;
import com.bigdata.htree.HTree;
import com.bigdata.io.LongPacker;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rawstore.IPSOutputStream;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.service.IBigdataFederation;

import cutthecrap.utils.striterators.ICloseableIterator;

/**
 * A persistence capable stream of "index" entries. The stream maintains the
 * order in which the entries were written.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         FIXME GIST : The base Stream should handle byte[]s, much like the
 *         basic BTree or HTree. That way it can be a concrete class and used
 *         for a variety of things. It might also allow an append() method
 *         (similar to insert) for non-iterator based incremental write
 *         protocols. This would be really just a thin wrapper over the
 *         {@link IPSOutputStream}, but integrated with the
 *         {@link ICheckpointProtocol} and (ideally) the MVCC architecture.
 * 
 * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/585"> GIST
 *      </a>
 */
abstract public class Stream implements ICheckpointProtocol {

    private static final Logger log = Logger.getLogger(Stream.class);

    /**
     * The index is already closed.
     */
    protected static final String ERROR_CLOSED = "Closed";

    // /**
    // * A parameter was less than zero.
    // */
    // protected static final String ERROR_LESS_THAN_ZERO = "Less than zero";
    //
    // /**
    // * A parameter was too large.
    // */
    // protected static final String ERROR_TOO_LARGE = "Too large";
    //
    /**
     * The index is read-only but a mutation operation was requested.
     */
    protected static final String ERROR_READ_ONLY = "Read-only";

    /**
     * The index object is no longer valid.
     */
    final protected static String ERROR_ERROR_STATE = "Index is in error state";

    /**
     * The backing store.
     */
    private final IRawStore store;

    /**
     * <code>true</code> iff the view is read-only.
     */
    private final boolean readOnly;
    
    /**
     * Hard reference iff the index is mutable (aka unisolated) allows us to
     * avoid patterns that create short life time versions of the object to
     * protect {@link #writeCheckpoint2()} and similar operations.
     */
    private final IReadWriteLockManager lockManager;

    protected volatile Throwable error;

    /**
     * <code>true</code> iff the {@link Stream} is open.
     */
    protected final AtomicBoolean open = new AtomicBoolean(false);

    /**
     * The #of entries in the stream.
     */
    protected long entryCount;

    /**
     * The address from which the stream may be read. This is mutable. On
     * update, it is the address at which the stream was last written. The
     * current value gets propagated into the {@link Checkpoint} record as
     * {@link Checkpoint#getRootAddr()}.
     */
    protected long rootAddr;

    @Override
    public IRawStore getStore() {

        return store;
        
    }
    
    /**
     * Required constructor. This constructor is used both to create a new named
     * solution set, and to load an existing named solution set from the store
     * using a {@link Checkpoint} record.
     * 
     * @param store
     *            The store.
     * @param checkpoint
     *            The {@link Checkpoint} record.
     * @param metadata
     *            The metadata record.
     * @param readOnly
     *            When <code>true</code> the view will be immutable.
     * 
     * @see #create(IRawStore, StreamIndexMetadata)
     * @see #load(IRawStore, long, boolean)
     */
    public Stream(final IRawStore store,
            final Checkpoint checkpoint, final IndexMetadata metadata,
            final boolean readOnly) {
    
        // show the copyright banner during startup.
        Banner.banner();

        if (store == null)
            throw new IllegalArgumentException();

        if (metadata == null)
            throw new IllegalArgumentException();

        if (checkpoint == null)
            throw new IllegalArgumentException();

        if (store != null) {

            if (checkpoint.getMetadataAddr() != metadata.getMetadataAddr()) {

                // must agree.
                throw new IllegalArgumentException();

            }

        }
        
        // save a reference to the immutable metadata record.
        this.metadata = (StreamIndexMetadata) metadata;

        this.store = (IRawStore) ((store instanceof AbstractJournal) ? ((AbstractJournal) store)
                .getBufferStrategy() : store);

        this.readOnly = readOnly;

        setCheckpoint(checkpoint);
        
        this.lockManager = ReadWriteLockManager.getLockManager(this);

    }

    /**
     * Sets the {@link #checkpoint} and initializes the mutable fields from the
     * checkpoint record. In order for this operation to be atomic, the caller
     * must be synchronized on the {@link BTree} or otherwise guaranteed to have
     * exclusive access, e.g., during the ctor or when the {@link BTree} is
     * mutable and access is therefore required to be single-threaded.
     */
    protected void setCheckpoint(final Checkpoint checkpoint) {

        this.checkpoint = checkpoint; // save reference.

        // this.height = checkpoint.getHeight();
        //
        // this.nnodes = checkpoint.getNodeCount();
        //
        // this.nleaves = checkpoint.getLeafCount();

        this.entryCount = checkpoint.getEntryCount();

        // this.counter = new AtomicLong( checkpoint.getCounter() );

        this.recordVersion = checkpoint.getRecordVersion();

        this.rootAddr = checkpoint.getRootAddr();
        
    }
    
    /**
     * The type of compression used on the stream.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static enum CompressionEnum {
        None,
        Zip;
    }
    
    /**
     * Metadata for a named solution set.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    public static class StreamIndexMetadata extends IndexMetadata {
        
        private static final long serialVersionUID = -1L;

        /**
         */
        public interface Options extends IndexMetadata.Options {
            
            /**
             * The name of a class derived from {@link Stream} that will be used
             * to re-load the index.
             */
            String STREAM_CLASS_NAME = Stream.class.getName()
                    + ".className";

            /**
             * The {@link CompressionEnum} for the stream.
             */
            String STREAM_COMPRESSION_TYPE = Stream.class.getName()
                    + ".compressionType";

        }

        /**
         * @see Options#STREAM_CLASS_NAME
         */
        private String streamClassName;
        
        /**
         * @see Options#STREAM_COMPRESSION_TYPE;
         */
        private CompressionEnum streamCompressionType;

        /**
         * The name of a class derived from {@link SolutionSetStream} that
         * will be used to re-load the index.
         * 
         * @see Options#STREAM_CLASS_NAME
         */
        public final String getStreamClassName() {

            return streamClassName;

        }

        public void setStreamClassName(final String className) {

            if (className == null)
                throw new IllegalArgumentException();

            this.streamClassName = className;

        }

        public CompressionEnum getStreamCompressionType() {
            
            return streamCompressionType;
            
        }

        public void setStreamCompressionType(final CompressionEnum e) {
            
            this.streamCompressionType = e;
            
        }
        
        
        /**
         * <strong>De-serialization constructor only</strong> - DO NOT use this ctor
         * for creating a new instance! It will result in a thrown exception,
         * typically from {@link #firstCheckpoint()}.
         */
        public StreamIndexMetadata() {
            
            super();
            
        }

        /**
         * Constructor used to configure a new <em>unnamed</em> {@link HTree}. The
         * index UUID is set to the given value and all other fields are defaulted
         * as explained at {@link #HTreeIndexMetadata(Properties, String, UUID)}.
         * Those defaults may be overridden using the various setter methods, but
         * some values can not be safely overridden after the index is in use.
         * 
         * @param indexUUID
         *            The indexUUID.
         * 
         * @throws IllegalArgumentException
         *             if the indexUUID is <code>null</code>.
         */
        public StreamIndexMetadata(final UUID indexUUID) {
            
            this(null/* name */, indexUUID);
            
        }

        /**
         * Constructor used to configure a new <em>named</em> {@link BTree}. The
         * index UUID is set to the given value and all other fields are defaulted
         * as explained at {@link #IndexMetadata(Properties, String, UUID)}. Those
         * defaults may be overridden using the various setter methods, but some
         * values can not be safely overridden after the index is in use.
         * 
         * @param name
         *            The index name. When this is a scale-out index, the same
         *            <i>name</i> is specified for each index resource. However they
         *            will be registered on the journal under different names
         *            depending on the index partition to which they belong.
         * 
         * @param indexUUID
         *            The indexUUID. The same index UUID MUST be used for all
         *            component indices in a scale-out index.
         * 
         * @throws IllegalArgumentException
         *             if the indexUUID is <code>null</code>.
         */
        public StreamIndexMetadata(final String name, final UUID indexUUID) {

            this(null/* name */, System.getProperties(), name, indexUUID);

        }

        /**
         * Constructor used to configure a new <em>named</em> B+Tree. The index UUID
         * is set to the given value and all other fields are defaulted as explained
         * at {@link #getProperty(Properties, String, String, String)}. Those
         * defaults may be overridden using the various setter methods.
         * 
         * @param indexManager
         *            Optional. When given and when the {@link IIndexManager} is a
         *            scale-out {@link IBigdataFederation}, this object will be used
         *            to interpret the {@link Options#INITIAL_DATA_SERVICE}
         *            property.
         * @param properties
         *            Properties object used to overridden the default values for
         *            this {@link IndexMetadata} instance.
         * @param namespace
         *            The index name. When this is a scale-out index, the same
         *            <i>name</i> is specified for each index resource. However they
         *            will be registered on the journal under different names
         *            depending on the index partition to which they belong.
         * @param indexUUID
         *            component indices in a scale-out index.
         *            The indexUUID. The same index UUID MUST be used for all
         * @param indexType
         *            Type-safe enumeration specifying the type of the persistence
         *            class data structure (historically, this was always a B+Tree).
         * 
         * @throws IllegalArgumentException
         *             if <i>properties</i> is <code>null</code>.
         * @throws IllegalArgumentException
         *             if <i>indexUUID</i> is <code>null</code>.
         */
        public StreamIndexMetadata(final IIndexManager indexManager,
                final Properties properties, final String namespace,
                final UUID indexUUID) {

            super(indexManager, properties, namespace, indexUUID,
                    IndexTypeEnum.Stream);
            
            /*
             * Intern'd to reduce duplication on the heap.
             */
            this.streamClassName = getProperty(indexManager, properties,
                    namespace, Options.STREAM_CLASS_NAME,
                    Stream.class.getName()).intern();

            this.streamCompressionType = CompressionEnum
                    .valueOf(getProperty(indexManager, properties, namespace,
                            Options.STREAM_COMPRESSION_TYPE,
                            CompressionEnum.Zip.name()));

        }

        @Override
        protected void toString(final StringBuilder sb) {

            super.toString(sb);

            // stream
            sb.append(", streamClassName=" + streamClassName);
            sb.append(", streamCompressionType=" + streamCompressionType);

        }

        /**
         * The initial version.
         */
        private static transient final int VERSION0 = 0x0;

        /**
         * The version that will be serialized by this class.
         */
        private static transient final int CURRENT_VERSION = VERSION0;

        @Override
        public void readExternal(final ObjectInput in) throws IOException,
                ClassNotFoundException {

            super.readExternal(in);

//            final int version = 
            LongPacker.unpackInt(in);// version
            
            streamClassName = in.readUTF();

            streamCompressionType = CompressionEnum.values()[LongPacker
                    .unpackInt(in)];

        }

        @Override
        public void writeExternal(final ObjectOutput out) throws IOException {

            super.writeExternal(out);
            
            final int version = CURRENT_VERSION;

            LongPacker.packLong(out, version);

            out.writeUTF(streamClassName);

            LongPacker.packLong(out, streamCompressionType.ordinal());

        }

        @Override
        public StreamIndexMetadata clone() {

            return (StreamIndexMetadata) super.clone();
            
        }

    }
    
    /**
     * Create a new {@link SolutionSetStream} or derived class. This method
     * works by writing the {@link StreamIndexMetadata} record on the store
     * and then loading the {@link SolutionSetStream} from the
     * {@link StreamIndexMetadata} record.
     * 
     * @param store
     *            The store.
     * 
     * @param metadata
     *            The metadata record.
     * 
     * @return The newly created {@link SolutionSetStream}.
     * 
     * @see #load(IRawStore, long, boolean)
     * 
     * @throws IllegalStateException
     *             If you attempt to create two {@link HTree} objects from the
     *             same metadata record since the metadata address will have
     *             already been noted on the {@link IndexMetadata} object. You
     *             can use {@link IndexMetadata#clone()} to obtain a new copy of
     *             the metadata object with the metadata address set to
     *             <code>0L</code>.
     * @exception IllegalStateException
     *                if the {@link IndexTypeEnum} in the supplied
     *                {@link IndexMetadata} object is not
     *                {@link IndexTypeEnum#BTree}.
     */
    public static SolutionSetStream create(final IRawStore store,
            final StreamIndexMetadata metadata) {

        if (metadata.getIndexType() != IndexTypeEnum.Stream) {

            throw new IllegalStateException("Wrong index type: "
                    + metadata.getIndexType());

        }

        if (store == null) {

            throw new IllegalArgumentException();

        }

        if (metadata.getMetadataAddr() != 0L) {

            throw new IllegalStateException("Metadata record already in use");
            
        }
        
        /*
         * Write metadata record on store. The address of that record is set as
         * a side-effect on the metadata object.
         */
        metadata.write(store);

        /*
         * Create checkpoint for the new solution set.
         */
        final Checkpoint firstCheckpoint = metadata.firstCheckpoint();
        
        /*
         * Write the checkpoint record on the store. The address of the
         * checkpoint record is set on the object as a side effect.
         */
        firstCheckpoint.write(store);
        
        /*
         * Load the HTree from the store using that checkpoint record. There is
         * no root so a new root leaf will be created when the HTree is opened.
         */
        return load(store, firstCheckpoint.getCheckpointAddr(), false/* readOnly */);
        
    }

    /**
     * Load an instance of a {@link HTree} or derived class from the store. The
     * {@link HTree} or derived class MUST declare a constructor with the
     * following signature: <code>
     * 
     * <i>className</i>(IRawStore store, Checkpoint checkpoint, IndexMetadata metadata, boolean readOnly)
     * 
     * </code>
     * 
     * @param store
     *            The store.
     * @param addrCheckpoint
     *            The address of a {@link Checkpoint} record for the index.
     * @param readOnly
     *            When <code>true</code> the {@link BTree} will be marked as
     *            read-only. Marking has some advantages relating to the locking
     *            scheme used by {@link Node#getChild(int)} since the root node
     *            is known to be read-only at the time that it is allocated as
     *            per-child locking is therefore in place for all nodes in the
     *            read-only {@link BTree}. It also results in much higher
     *            concurrency for {@link AbstractBTree#touch(AbstractNode)}.
     * 
     * @return The {@link HTree} or derived class loaded from that
     *         {@link Checkpoint} record.
     * 
     * @throws IllegalArgumentException
     *             if store is <code>null</code>.
     */
    @SuppressWarnings("unchecked")
    public static SolutionSetStream load(final IRawStore store,
            final long addrCheckpoint, final boolean readOnly) {

        if (store == null)
            throw new IllegalArgumentException();
        
        /*
         * Read checkpoint record from store.
         */
        final Checkpoint checkpoint;
        try {
            checkpoint = Checkpoint.load(store, addrCheckpoint);
        } catch (Throwable t) {
            throw new RuntimeException("Could not load Checkpoint: store="
                    + store + ", addrCheckpoint="
                    + store.toString(addrCheckpoint), t);
        }

        if (checkpoint.getIndexType() != IndexTypeEnum.Stream)
            throw new RuntimeException("Wrong checkpoint type: " + checkpoint);

        /*
         * Read metadata record from store.
         */
        final StreamIndexMetadata metadata;
        try {
            metadata = (StreamIndexMetadata) IndexMetadata.read(store,
                    checkpoint.getMetadataAddr());
        } catch (Throwable t) {
            throw new RuntimeException("Could not read IndexMetadata: store="
                    + store + ", checkpoint=" + checkpoint, t);
        }

        if (log.isInfoEnabled()) {

            // Note: this is the scale-out index name for a partitioned index.
            final String name = metadata.getName();

            log.info((name == null ? "" : "name=" + name + ", ")
                    + "readCheckpoint=" + checkpoint);

        }

        /*
         * Create HTree object instance.
         */
        try {

            @SuppressWarnings("rawtypes")
            final Class cl = Class.forName(metadata.getStreamClassName());

            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor for a class derived from
             * BTree.
             */
            @SuppressWarnings("rawtypes")
            final Constructor ctor = cl.getConstructor(new Class[] {
                    IRawStore.class,//
                    Checkpoint.class,//
                    IndexMetadata.class, //
                    Boolean.TYPE
                    });

            final SolutionSetStream solutions = (SolutionSetStream) ctor
                    .newInstance(new Object[] { //
                            store,//
                            checkpoint, //
                            metadata, //
                            readOnly
                    });

            solutions.reopen();

            return solutions;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }
    
    public void clear() {

        assertNotReadOnly();

        if (rootAddr != IRawStore.NULL) {
        
            recycle(rootAddr);
//            store.clear();

            rootAddr = IRawStore.NULL;
            
            fireDirtyEvent();
            
        }

    }

    /**
     * Apply the configured {@link CompressionEnum} for the {@link Stream} to
     * the caller's {@link OutputStream}.
     * 
     * @param out
     *            The {@link OutputStream}.
     * 
     * @return The wrapped {@link OutputStream}.
     * 
     * @throws IOException
     */
    protected OutputStream wrapOutputStream(final OutputStream out) {

        final CompressionEnum compressionType = metadata
                .getStreamCompressionType();

        switch (compressionType) {
        case None:
            return out;
        case Zip:
            return new DeflaterOutputStream(out);
        default:
            throw new UnsupportedOperationException("CompressionEnum="
                    + compressionType);
        }

    }

    /**
     * Wrap for decompression.
     */
    protected InputStream wrapInputStream(final InputStream in) {

        final CompressionEnum compressionType = metadata
                .getStreamCompressionType();

        switch (compressionType) {
        case None:
            return in;
        case Zip:
            return new InflaterInputStream(in);
        default:
            throw new UnsupportedOperationException("CompressionEnum="
                    + compressionType);
        }

    }

    /*
     * ICheckpointProtocol
     */
    
    /**
     * Return <code>true</code> iff this B+Tree is read-only.
     */
    final public boolean isReadOnly() {

        return readOnly;

    }

    /**
     * 
     * @throws UnsupportedOperationException
     *             if the B+Tree is read-only.
     * 
     * @see #isReadOnly()
     */
    final protected void assertNotReadOnly() {

        if (isReadOnly()) {

            throw new UnsupportedOperationException(ERROR_READ_ONLY);

        }

        if( error != null )
            throw new IndexInconsistentError(ERROR_ERROR_STATE, error);
        
    }

    /**
     * NOP. Implemented for compatibility with the interior APIs of the
     * {@link HTree} and {@link BTree}, but {@link Stream} does not support
     * "transient" data (in the sense of data which is not evicted to a backing
     * store).
     */
    final protected void assertNotTransient() {

        // NOP

    }
    
    @Override
    public CounterSet getCounters() {
        
        final CounterSet counterSet = new CounterSet();
        {
            
            counterSet.addCounter("index UUID", new OneShotInstrument<String>(
                    getIndexMetadata().getIndexUUID().toString()));

            counterSet.addCounter("class", new OneShotInstrument<String>(
                    getClass().getName()));

        }

        return counterSet;

    }

    @Override
    public long handleCommit(final long commitTime) {

        return writeCheckpoint2().getCheckpointAddr();
        
    }


    @Override
    public void invalidate(final Throwable t) {

        if (t == null)
            throw new IllegalArgumentException();

        if (error == null)
            error = t;

    }
    
    @Override
    public long getRecordVersion() {

        return recordVersion;
        
    }

    /**
     * The value of the record version number that will be assigned to the next
     * node or leaf written onto the backing store. This number is incremented
     * each time a node or leaf is written onto the backing store. The initial
     * value is ZERO (0). The first value assigned to a node or leaf will be
     * ZERO (0).
     */
    private long recordVersion;

    /**
     * The constructor sets this field initially based on a {@link Checkpoint}
     * record containing the only address of the {@link IndexMetadata} for the
     * index. Thereafter this reference is maintained as the {@link Checkpoint}
     * record last written by {@link #writeCheckpoint()} or read by
     * {@link #load(IRawStore, long, boolean)}.
     */
    private Checkpoint checkpoint = null;    

    @Override
    final public Checkpoint getCheckpoint() {

        if (checkpoint == null)
            throw new AssertionError();
        
        return checkpoint;
        
    }
    
    @Override
    final public long writeCheckpoint() {
        
        // write checkpoint and return address of that checkpoint record.
        return writeCheckpoint2().getCheckpointAddr();
        
    }

    @Override
    final public Checkpoint writeCheckpoint2() {
        
        assertNotTransient();
        assertNotReadOnly();

        /*
         * Note: Acquiring this lock provides for atomicity of the checkpoint of
         * the BTree during the commit protocol. Without this lock, users of the
         * UnisolatedReadWriteIndex could be concurrently modifying the BTree
         * while we are attempting to snapshot it for the commit.
         * 
         * Note: An alternative design would declare a global read/write lock
         * for mutation of the indices in addition to the per-BTree read/write
         * lock provided by UnisolatedReadWriteIndex. Rather than taking the
         * per-BTree write lock here, we would take the global write lock in the
         * AbstractJournal's commit protocol, e.g., commitNow(). The global read
         * lock would be taken by UnisolatedReadWriteIndex before taking the
         * per-BTree write lock. This is effectively a hierarchical locking
         * scheme and could provide a workaround if deadlocks are found to occur
         * due to lock ordering problems with the acquisition of the
         * UnisolatedReadWriteIndex lock (the absence of lock ordering problems
         * really hinges around UnisolatedReadWriteLocks not being taken for
         * more than one index at a time.)
         * 
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/278
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/284
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/288
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/343
         * @see https://sourceforge.net/apps/trac/bigdata/ticket/440
         */
        final Lock lock = writeLock();
        lock.lock();
        try {

            if (/* autoCommit && */needsCheckpoint()) {

                /*
                 * Flush the btree, write a checkpoint record, and return the
                 * address of that checkpoint record. The [checkpoint] reference
                 * is also updated.
                 */

                return _writeCheckpoint2();

            }

            /*
             * There have not been any writes on this btree or auto-commit is
             * disabled.
             * 
             * Note: if the application has explicitly invoked writeCheckpoint()
             * then the returned address will be the address of that checkpoint
             * record and the BTree will have a new checkpoint address made
             * restart safe on the backing store.
             */

            return checkpoint;

        } finally {

            lock.unlock();

        }

    }
    
    /**
     * Return true iff changes would be lost unless the {@link Stream} is
     * flushed to the backing store using {@link #writeCheckpoint()}.
     * 
     * @return <code>true</code> true iff changes would be lost unless the
     *         {@link Stream} was flushed to the backing store using
     *         {@link #writeCheckpoint()}.
     */
    protected boolean needsCheckpoint() {

        if (checkpoint.getCheckpointAddr() == 0L) {

            /*
             * The checkpoint record needs to be written.
             */

            return true;

        }

        if (metadata.getMetadataAddr() == 0L) {

            /*
             * The index metadata record was replaced and has not yet been
             * written onto the store.
             */

            return true;

        }

        if (metadata.getMetadataAddr() != checkpoint.getMetadataAddr()) {

            /*
             * The index metadata record was replaced and has been written on
             * the store but the checkpoint record does not reflect the new
             * address yet.
             */

            return true;

        }
     
//     if(checkpoint.getCounter() != counter.get()) {
//         
//         // The counter has been modified.
//         
//         return true;
//         
//     }
     
//     if(root != null ) {
//         
//         if (root.isDirty()) {
//
//             // The root node is dirty.
//
//             return true;
//
//         }
         
         if(checkpoint.getRootAddr() != rootAddr) {
     
             // The root node has a different persistent identity.
             
             return true;
             
         }
         
//     }

     /*
      * No apparent change in persistent state so we do NOT need to do a
      * checkpoint.
      */
     
     return false;
     
//     if (metadata.getMetadataAddr() != 0L && //
//             (root == null || //
//                     ( !root.dirty //
//                     && checkpoint.getRootAddr() == root.identity //
//                     && checkpoint.getCounter() == counter.get())
//             )
//     ) {
//         
//         return false;
//         
//     }
//
//     return true;
  
 }

    /**
     * Hook to flush anything which is dirty to the backing store. This is
     * invoked by {@link #_writeCheckpoint2()}.
     */
    protected void flush() {
        // NOP
    }
 
    /**
     * Core implementation invoked by {@link #writeCheckpoint2()} while holding
     * the lock - <strong>DO NOT INVOKE THIS METHOD DIRECTLY</strong>.
     * 
     * @return the checkpoint.
     */
    final private Checkpoint _writeCheckpoint2() {

        assertNotTransient();
        assertNotReadOnly();

        // assert root != null : "root is null"; // i.e., isOpen().

        // flush any dirty nodes.
        flush();

        if (metadata.getMetadataAddr() == 0L) {

            /*
             * Is there an old metadata addr in need of recyling?
             */
            if (checkpoint != null) {
                final long addr = checkpoint.getMetadataAddr();
                if (addr != IRawStore.NULL)
                    store.delete(addr);
            }

            /*
             * The index metadata has been modified so we write out a new
             * metadata record on the store.
             */

            metadata.write(store);

        }

        /*
         * Note: The old solutions are recycled when the new ones are written.
         */
        // if (checkpoint != null && getRoot() != null
        // && checkpoint.getRootAddr() != getRoot().getIdentity()) {
        // recycle(checkpoint.getRootAddr());
        // }

        if (checkpoint != null) {

            recycle(checkpoint.getCheckpointAddr());
            
        }

        // create new checkpoint record.
        checkpoint = newCheckpoint();

        // write it on the store.
        checkpoint.write(store);

        if (BigdataStatics.debug || log.isInfoEnabled()) {
            final String msg = "name=" + metadata.getName() + "} : "
                    + checkpoint;
            if (BigdataStatics.debug)
                System.err.println(msg);
            if (log.isInfoEnabled())
                log.info(msg);
        }

        // return the checkpoint record.
        return checkpoint;

    }

    /**
     * Create a {@link Checkpoint} for a {@link Stream}.
     * <p>
     * The caller is responsible for writing the {@link Checkpoint} record onto
     * the store.
     * <p>
     * The class identified by {@link IndexMetadata#getCheckpointClassName()}
     * MUST declare a public constructor with the following method signature
     * 
     * <pre>
     *   ...( Stream sset)
     * </pre>
     * 
     * @return The {@link Checkpoint}.
     */
    @SuppressWarnings("unchecked")
    final private Checkpoint newCheckpoint() {

        try {

            @SuppressWarnings("rawtypes")
            final Class cl = Class.forName(metadata.getCheckpointClassName());

            /*
             * Note: A NoSuchMethodException thrown here means that you did not
             * declare the required public constructor on the Checkpoint class
             * [cl].
             */

            @SuppressWarnings("rawtypes")
            final Constructor ctor = cl
                    .getConstructor(new Class[] { Stream.class //
                    });

            final Checkpoint checkpoint = (Checkpoint) ctor
                    .newInstance(new Object[] { //
                    this //
                    });

            return checkpoint;

        } catch (Exception ex) {

            throw new RuntimeException(ex);

        }

    }

    /**
     * Recycle (aka delete) the allocation. This method also adjusts the #of
     * bytes released in the {@link BTreeCounters}.
     * 
     * @param addr
     *            The address to be recycled.
     * 
     * @return The #of bytes which were recycled and ZERO (0) if the address is
     *         {@link IRawStore#NULL}.
     */
    protected int recycle(final long addr) {

        if (addr == IRawStore.NULL)
            return 0;

        final int nbytes = store.getByteCount(addr);

        // getBtreeCounters().bytesReleased += nbytes;

        store.delete(addr);

        return nbytes;

    }

    /**
     * {@inheritDoc}
     * 
     * @return The address from which the solutions may be read.
     */
    @Override
    final public long getRootAddr() {

        return rootAddr;

    }

    @Override
    final public long getMetadataAddr() {

        return metadata.getMetadataAddr();

    }
        
    @Override
    public StreamIndexMetadata getIndexMetadata() {

        if (isReadOnly()) {

            if (metadata2 == null) {

                synchronized (this) {

                    if (metadata2 == null) {

                        metadata2 = metadata.clone();

                    }

                }

            }

            return metadata2;

        }

        return metadata;

    }

    private volatile StreamIndexMetadata metadata2;

    /**
     * The metadata record for the index. This data rarely changes during the
     * life of the solution set, but it CAN be changed.
     */
    protected StreamIndexMetadata metadata;

    final public IDirtyListener getDirtyListener() {
        
        return listener;
        
    }

    final public long getLastCommitTime() {
        
        return lastCommitTime;
        
    }

    final public void setLastCommitTime(final long lastCommitTime) {
        
        if (lastCommitTime == 0L)
            throw new IllegalArgumentException();
        
        if (this.lastCommitTime == lastCommitTime) {

            // No change.
            
            return;
            
        }
        
        if (log.isInfoEnabled())
            log.info("old=" + this.lastCommitTime + ", new=" + lastCommitTime);
        // Note: Commented out to allow replay of historical transactions.
        /*if (this.lastCommitTime != 0L && this.lastCommitTime > lastCommitTime) {

            throw new IllegalStateException("Updated lastCommitTime: old="
                    + this.lastCommitTime + ", new=" + lastCommitTime);
            
        }*/

        this.lastCommitTime = lastCommitTime;
        
    }

    /**
     * The lastCommitTime of the {@link Checkpoint} record from which the
     * view was loaded.
     * <p>
     * Note: Made volatile on 8/2/2010 since it is not otherwise obvious what
     * would guarantee visibility of this field, through I do seem to remember
     * that visibility might be guaranteed by how the BTree class is discovered
     * and returned to the class. Still, it does no harm to make this a volatile
     * read.
     */
    volatile private long lastCommitTime = 0L;// Until the first commit.

    @Override
    final public void setDirtyListener(final IDirtyListener listener) {

        assertNotReadOnly();
        
        this.listener = listener;
        
    }
    
    private IDirtyListener listener;

    /**
     * Fire an event to the listener (iff set).
     */
    final protected void fireDirtyEvent() {

        assertNotReadOnly();

        final IDirtyListener l = this.listener;

        if (l == null)
            return;

        if (Thread.interrupted()) {

            throw new RuntimeException(new InterruptedException());

        }

        l.dirtyEvent(this);
        
    }

    @Override
    public long rangeCount() {

        return entryCount;
        
    }

    @Override
    public void removeAll() {
        
        clear();
        
    }

    @Override
    public void close() {

        open.set(false);
    
    }

    @Override
    public void reopen() {
    
        open.set(true);
        
    }

    @Override
    public boolean isOpen() {

        return open.get();
        
    }

    @Override
    abstract public ICloseableIterator<?> scan();

    /**
     * Write entries onto the stream.
     * 
     * @param src
     *            An iterator visiting the entries.
     */
    abstract public void write(ICloseableIterator<?> src);

    @Override
    final public Lock readLock() {

        return lockManager.readLock();
        
    }

    @Override
    final public Lock writeLock() {

        return lockManager.writeLock();
        
    }

    @Override
    final public int getReadLockCount() {
        
        return lockManager.getReadLockCount();
    }

}
