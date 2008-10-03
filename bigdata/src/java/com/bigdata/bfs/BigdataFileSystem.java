/*
 * Created on Jan 17, 2008
 */
package com.bigdata.bfs;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.IIndex;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.IndexMetadata;
import com.bigdata.btree.filter.FilterConstructor;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IResourceLock;
import com.bigdata.journal.ITx;
import com.bigdata.mdi.MetadataIndex;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.relation.AbstractResource;
import com.bigdata.relation.IDatabase;
import com.bigdata.relation.locator.DefaultResourceLocator;
import com.bigdata.relation.locator.RelationSchema;
import com.bigdata.search.FullTextIndex;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.sparse.ITPS;
import com.bigdata.sparse.ITPV;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;
import com.bigdata.sparse.TPS.TPV;
import com.bigdata.sparse.ValueType.AutoIncIntegerCounter;

import cutthecrap.utils.striterators.Resolver;
import cutthecrap.utils.striterators.Striterator;

/**
 * A distributed file system with extensible metadata and atomic append
 * implemented using the bigdata scale-out architecture. Files have a client
 * assigned identifier, which is a Unicode string. The file identifier MAY be
 * structured so as to look like a hierarchical file system using any desired
 * convention. Files are versioned and historical versions MAY be accessed until
 * the next compacting merge discards their data. File data is stored in large
 * {@link #blockSize} blocks. Partial and even empty blocks are allowed and only
 * the data written will be stored. <code>2^63-1</code> distinct blocks may be
 * written per file version, making the maximum possible file size
 * <code>536,870,912</code> exabytes. Files may be used as queues, in which
 * case blocks containing new records are atomically appended while a map/reduce
 * style master consumes the head block of the file.
 * <p>
 * Efficient method are offered for streaming and block oriented IO. All block
 * read and write operations are atomic, including block append. Files may be
 * easily written such that records never cross a block boundary by the
 * expediency of flushing the output stream if a record would overflow the
 * current block. A flush forces the atomic write of a partial block. Partial
 * blocks are stored efficiently - only the bytes actually written are stored.
 * Blocks are large enough that most applications can safely store a large
 * number of logical records in each block. Files comprised of application
 * defined logical records organized into a sequence of blocks are well-suited
 * to map/reduce processing. They may be efficiently split at block boundaries
 * and references to the blocks distributed to clients. Likewise, reduce clients
 * can aggregate data into large files suitable for further map/reduce
 * processing.
 * <p>
 * The distributed file system uses two scale-out indices to support ACID
 * operations on file metadata and atomic file append. These ACID guarentees
 * arise from the use of unisolated operations on the respective indices and
 * therefore apply only to the individual file metadata or file block
 * operations. In particular, file metadata read and write are atomic and all
 * individual file block IO (read, write, and append) operations are atomic.
 * Atomicity is NOT guarenteed when performing more than a single file block IO
 * operation, e.g., multiple appends MIGHT NOT write sequential blocks since
 * other block operations could have intervened.
 * <p>
 * The content length of the file is not stored as file metadata. Instead it MAY
 * be estimated by a range count of the index entries spanned by the file's
 * data. The exact file size may be readily determined when reading small files
 * by the expediency of sucking the entire file into a buffer - all reads are at
 * least one block. Streaming processing is advised in all cases when handling
 * large files, including when the file is to be delivered via HTTP.
 * <p>
 * The {@link #getMetadataIndex() metadata index} uses a {@link SparseRowStore}
 * design, similar to Google's bigtable or Hadoop's HBase. All updates to file
 * version metadata are atomic. The primary key in the metadata index for every
 * file is its {@link FileMetadataSchema#ID}. In addition, each version of a file
 * has a distinct {@link FileMetadataSchema#VERSION} property. File creation time,
 * version creation time, and file version metadata update timestamps may be
 * recovered from the timestamps associated with the properties in the metadata
 * index. The use of the {@link FileMetadataSchema#CONTENT_TYPE} and
 * {@link FileMetadataSchema#CONTENT_ENCODING} properties is enforced by the
 * high-level {@link Document} interface. Applications are free to define
 * additional properties.
 * <p>
 * Each time a file is created a new version number is assigned. The data index
 * uses the {@link FileMetadataSchema#ID} as the first field in a compound key. The
 * second field is the {@link FileMetadataSchema#VERSION} - a 32-bit integer. The
 * remainder of the key is a 64-bit signed block identifier (2^63-1 distinct
 * block identifiers). The block identifiers are strictly monotonic (e.g., up
 * one) and their sequence orders the blocks into the logical byte order of the
 * file.
 * <p>
 * Operations that create a new file actually create a new file version. The old
 * file version will eventually be garbage collected depending on the policy in
 * effect for compacting merges. Likewise, operations that delete a file simply
 * mark the metadata for the file version as deleted and the file version will
 * be eventually reclaimed. The high-level {@link #update(Document)} operation
 * in fact simply creates a new file version.
 * <p>
 * <h2>Use cases</h2>
 * <p>
 * Use case: A REST-ful repository. Documents may be stored, updated, read,
 * deleted, and searched using a full text index.
 * <p>
 * Use case: A map/reduce master reads document metadata using an index scan. It
 * examines the data index's {@link MetadataIndex} (that is, the index that
 * knows where each partition of the scale-out data index is stored) and
 * determines which map clients are going to be "close" to each document and
 * then hands off the document to one of those map clients.
 * <p>
 * Use case: The same as the use case above, but large files are being processed
 * and there is a requirement to "break" the files into splits and hand off the
 * splits. This can be achieved by estimating the file system using a range
 * count and multiplying through by the block size. Blocks may be handed off to
 * the clients in parallel (of course, clients need to deal with the hassle of
 * processing files where records will cross split boundaries unless they always
 * pad out with unused bytes to the next {@link #blockSize} boundary).
 * <p>
 * Use case: A reduce client wants to write a very large files so it creates a
 * metadata record for the file and then does a series of atomic appears to the
 * file. The file may grow arbitrarily large. Clients may begin to read from the
 * file as soon as the first block has been flushed.
 * <p>
 * Use case: Queues MAY be built from the operations to atomically read or
 * delete the first block for the file version. The "design pattern" is to have
 * clients append blocks to the file version, taking care that logical rows
 * never cross a block boundary (e.g., by flushing partial blocks). A master
 * then reads the head block from the file version, distributing the logical
 * records therein to consumers and providing fail safe processing in case
 * consumers die or take too long. Once all records for the head block have been
 * processed the master simply deletes the head block. This "pattern" is quite
 * similar to map/reduce and, like map/reduce, requires that the consumer
 * operations may be safely re-run.
 * <p>
 * Use case: File replication, retention of deleted versions, and media indexing
 * are administered by creating "zones" comprising one or more index partitions
 * with a shared file identifier prefix, e.g., /tmp or /highly-available, or
 * /deployment-text-index. All files in a given zone share the same policy for
 * file replication, compacting merges (determining when a deleted or even a
 * non-deleted file version will be discarded), and media indexing.
 * <p>
 * Use case: File rename is NOT a cheap operation. It essentially creates a new
 * file version with the desired name and copies the data from the old file
 * version to the new file version. Finally the old file version is "deleted".
 * This approach is necessary since files may moved from one "zone" to another
 * and since the file data must reside on the index partition(s) identified by
 * its file version.
 * 
 * FIXME write a JSON API that interoperates to the extent possible with GAE and
 * HBASE.
 * 
 * @todo implement "zones" and their various policies (replication, retention,
 *       and media indexing). access control could also be part of the zones.
 * 
 * @todo should compression be applied? applications are obviously free to apply
 *       their own compression, but it could be convienent to stored compressed
 *       blocks. the caller could specify the compression method on a per block
 *       basis (we don't want to lookup the file metadata for this). the
 *       compression method would be written into a block header. blocks can
 *       always be decompressed by examining the header.
 * 
 * @todo there should be some constraints on the file identifier but it general
 *       it represents a client determined absolute file path name. It is
 *       certainly possible to use a flat file namespace, but you can just as
 *       readily use a hierarchical one. Unicode characters are supported in the
 *       file identifiers.
 * 
 * @todo do we need a global lock mechanism to prevent concurrent high-level
 *       create/update/delete of the same file? a distributed lease-based lock
 *       system derived from jini or built ourselves? Can this be supported with
 *       the historical and not yet purged timestamped metadata for the file?
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataFileSystem extends
        AbstractResource<IDatabase<BigdataFileSystem>> implements
        IContentRepository {

    final protected static Logger log = Logger.getLogger(BigdataFileSystem.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isDebugEnabled();

    /**
     * Configuration options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options, KeyBuilder.Options {
        
    }
    
    /**
     * The #of offset bits.
     */
    private final int offsetBits;

    /** The size of a file block. */
    private final int blockSize;

    /**
     * The #of bits in a 64-bit long integer identifier that are used to encode
     * the byte offset of a record in the store as an unsigned integer.
     * 
     * @see com.bigdata.journal.Options#OFFSET_BITS
     * @see #getBlockSize()
     */
    public final int getOffsetBits() {
        
        return offsetBits;
        
    }
    
    /**
     * The size of a file block. Block identifiers are 64-bit signed integers.
     * The maximum file length is <code>2^63 - 1 </code> blocks ( 536,870,912
     * Exabytes).
     * <p>
     * Note: The {@link BigdataFileSystem} makes the <strong>assumption</strong>
     * that the {@link com.bigdata.journal.Options#OFFSET_BITS} is the #of
     * offset bits configured for the {@link IDataService}s in the connected
     * {@link IBigdataFederation} and computes the
     * {@link BigdataFileSystem#getBlockSize()} based on that assumption. It is
     * NOT possible to write blocks on the {@link BigdataFileSystem} whose size
     * is greater than the maximum block size actually configured for the
     * {@link IDataService}s in the connected {@link IBigdataFederation}.
     * 
     * @see com.bigdata.journal.Options#OFFSET_BITS
     * @see #getOffsetBits()
     */
    public final int getBlockSize() {
        
        return blockSize;
        
    }
    
    /**
     * The maximum block identifier that can be assigned to a file version.
     * <p>
     * Note: This is limited to {@value Long#MAX_VALUE}-1 so that we can always
     * form the key greater than any valid key for a file version. This is
     * required by the atomic append logic when it seeks the next block
     * identifier. See {@link AtomicBlockAppendProc}.
     */
    protected static final long MAX_BLOCK = Long.MAX_VALUE - 1;
    
    /**
     * The basename of the index in which the file metadata are stored. The
     * fully qualified name of the index uses {@link #getNamespace()} as a
     * prefix.
     * <p>
     * Note: This is a {@link SparseRowStore} governed by the
     * {@link FileMetadataSchema}.
     */
    public static final String FILE_METADATA_INDEX_BASENAME = "#fileMetadata";
    
    /**
     * The basename of the index in which the file data blocks are stored. The
     * fully qualified name of the index uses {@link #getNamespace()} as a
     * prefix.
     * <p>
     * Note: The entries in this index are a series of blocks for a file. Blocks
     * are {@link #blockSize} bytes each and are assigned monotonically
     * increasing block numbers by the atomic append operation. The final block
     * may be smaller (there is no need to pad out the data with nulls). The
     * keys are formed from two fields - a field containing the content
     * identifier followed by an integer field containing the sequential block
     * number. A range scan with a fromKey of the file identifier and a toKey
     * computed using the successor of the file identifier will naturally visit
     * all blocks in a file in sequence.
     */
    public static final String FILE_DATA_INDEX_BASENAME = "#fileData";
    
    public static final FileMetadataSchema metadataSchema = new FileMetadataSchema();
    
    private SparseRowStore metadataIndex;
    
    private IIndex dataIndex;
    
    // @todo unique or not?
    final protected long AUTO_TIMESTAMP = SparseRowStore.AUTO_TIMESTAMP_UNIQUE;
        
    protected static void assertString(Map<String, Object> properties, String name) {

        Object val = properties.get(name);

        if (val == null)
            throw new IllegalArgumentException(name + " is null");

        if (!(val instanceof String))
            throw new IllegalArgumentException(name + " must be String");

    }
    
    protected static void assertLong(Map<String, Object> properties, String name) {

        Object val = properties.get(name);

        if (val == null)
            throw new IllegalArgumentException(name + " is null");

        if (!(val instanceof Long))
            throw new IllegalArgumentException(name + " must be Long");

    }

    /**
     * Ctor specified by {@link DefaultResourceLocator}.
     * 
     * @see Options
     */
    public BigdataFileSystem(IIndexManager indexManager, String namespace,
            Long timestamp, Properties properties) {

        super(indexManager,namespace,timestamp,properties);
        
        offsetBits = Integer.parseInt(properties.getProperty(
                Options.OFFSET_BITS, Options.DEFAULT_OFFSET_BITS));
        
        blockSize = WormAddressManager.getMaxByteCount(offsetBits) - 1;
        
        if (INFO)
            log.info("offsetBits=" + offsetBits + ", blockSize=" + blockSize);
        
    }

    /**
     * The index in which the file metadata is stored (the index must exist).
     */
    public SparseRowStore getMetadataIndex() {

        if (metadataIndex == null) {

            throw new IllegalStateException();
            
        }

        return metadataIndex;
        
    }

    /**
     * The index in which the file blocks are stored (the index must exist).
     */
    public IIndex getDataIndex() {

        if (dataIndex == null) {

            throw new IllegalStateException();

        }

        return dataIndex;

    }
    
    /**
     * <code>true</code> unless {{@link #getTimestamp()} is {@link ITx#UNISOLATED}.
     */
    public boolean isReadOnly() {

        return getTimestamp() != ITx.UNISOLATED;
        
    }
    
    final protected void assertWritable() {
        
        if(isReadOnly()) {
            
            throw new IllegalStateException("READ_ONLY");
            
        }
        
    }

    /**
     * Note: A commit is required in order for a read-committed view to have
     * access to the registered indices. When running against an
     * {@link IBigdataFederation}, {@link ITx#UNISOLATED} operations will take
     * care of this for you. Otherwise you must do this yourself.
     */
    public void create() {

        assertWritable();

        final IResourceLock resourceLock = acquireExclusiveLock();
        
        final Properties tmp = getProperties();
        
//        final int branchingFactor = Integer.parseInt(tmp.getProperty(
//                Options.BRANCHING_FACTOR, Options.DEFAULT_BRANCHING_FACTOR));

        // set property that will let the contained relations locate their container.
        tmp.setProperty(RelationSchema.CONTAINER, getNamespace());
        
        try {

            super.create();
            
            final IIndexManager indexManager = getIndexManager();
            
            // setup metadata index.
            {

                /*
                 * FIXME specify an appropriate split handler (keeps the row
                 * together). This is a hard requirement. The atomic read/update
                 * guarentee depends on this.
                 */

                final String name = getNamespace()+FILE_METADATA_INDEX_BASENAME;
                
                final IndexMetadata md = new IndexMetadata(name, UUID
                        .randomUUID());
                
                indexManager.registerIndex(md);

                final IIndex ndx = indexManager.getIndex(name, getTimestamp());

                metadataIndex = new SparseRowStore(ndx);

            }

            // setup data index.
            {

                /*
                 * @todo specify split handler that tends to keep the blocks for a
                 * file together (soft requirement).
                 */

                final String name = getNamespace()+FILE_DATA_INDEX_BASENAME;
                
                final IndexMetadata md = new IndexMetadata(name, UUID.randomUUID());

                /*
                 * @todo unit tests for correct copying of blobs during overflow.
                 * See {@link IOverflowHandler}.
                 */
                md.setOverflowHandler(new BlobOverflowHandler());
                
                // register the index.
                indexManager.registerIndex(md);

                dataIndex = indexManager.getIndex(name,getTimestamp());

            }

        } finally {

            unlock(resourceLock);

        }
        
    }

    public void destroy() {

        assertWritable();

        final IResourceLock resourceLock = acquireExclusiveLock();
        
        try {

            getIndexManager().dropIndex(getNamespace()+FILE_METADATA_INDEX_BASENAME);

            getIndexManager().dropIndex(getNamespace()+FILE_DATA_INDEX_BASENAME);
            
            super.destroy();
            
        } finally {

            unlock(resourceLock);
            
        }

    }

    /**
     * Creates a new file version from the specified metadata. The new file
     * version will not have any blocks. You can use either stream-oriented or
     * block oriented IO to write data on the newly created file version.
     * 
     * @param metadata
     *            The file metadata.
     * 
     * @return The new version identifier.
     */
    public int create(Map<String, Object> metadata) {

        if (metadata == null)
            throw new IllegalArgumentException();

        // check required properties.
        assertString(metadata, FileMetadataSchema.ID);

        // clone the map since it may be unmodifiable.
        metadata = new HashMap<String, Object>(metadata);
        
        // auto-increment the last defined version counter.
        metadata.put(FileMetadataSchema.VERSION, AutoIncIntegerCounter.INSTANCE);
        
        // write the metadata (atomic operation).
        final ITPS tps = getMetadataIndex().write(metadataSchema, metadata,
                AUTO_TIMESTAMP, null/* filter */, null/*precondition*/);

        final int version = (Integer) tps.get(FileMetadataSchema.VERSION).getValue();

        if(INFO)
        log.info("Created new version: id=" + metadata.get(FileMetadataSchema.ID)
                + ", version=" + version);
        
        return version;
        
    }
    
    public int create(Document doc) {
        
        if (doc == null)
            throw new IllegalArgumentException();
        
        final String id = doc.getId(); 
        
        if (id == null)
            throw new RuntimeException("The " + FileMetadataSchema.ID
                    + " property must be defined.");

        final Map<String,Object> metadata = doc.asMap();

//        /*
//         * Verify content type was specified since we will write on the file
//         * version.
//         */
//        assertString(metadata, MetadataSchema.CONTENT_TYPE);

        /*
         * Vreate new file version.
         */
        final int version = create( metadata );

        /*
         * Copy data from the document.
         */
        copyStream(id, version, doc.getInputStream());
        
        return version;
        
    }
    
    /**
     * Reads the document metadata for the current version of the specified
     * file.
     * 
     * @param id
     *            The file identifier.
     * 
     * @return A read-only view of the file version that is capable of reading
     *         the content from the repository -or- <code>null</code> iff
     *         there is no current version for that file identifier.
     */
    public Document read(String id) {

        RepositoryDocumentImpl doc = new RepositoryDocumentImpl(this, id);

        if (!doc.exists()) {

            // no current version for that document.
            
            if(INFO)
            log.info("No current version: id="+id);
            
            return null;
            
        }
        
        return doc;
        
    }

    /**
     * Return the file metadata for the version of the file associated with the
     * specified timestamp.
     * 
     * @param id
     *            The file identifier.
     * @param timestamp
     *            The timestamp.
     * 
     * @return A read-only view of the logical row of metadata for that file as
     *         of that timestamp.
     * 
     * @see ITPS
     * @see SparseRowStore#read(Schema, Object, long, com.bigdata.sparse.INameFilter)
     */
    public ITPS readMetadata(String id, long timestamp) {

        return getMetadataIndex()
                .read(metadataSchema, id, timestamp, null/* filter */);

    }
    
    /**
     * Update the metadata for the current file version.
     * 
     * @param id
     *            The file identifier.
     * 
     * @param metadata
     *            The properties to be written. A <code>null</code> value for
     *            a property will cause the corresponding property to be
     *            deleted. Properties not present in this map will NOT be
     *            modified.
     * 
     * @return The complete metadata for the current file version.
     */
    public Map<String,Object> updateMetadata(String id, Map<String,Object> metadata) {

        // copy since the map might be unmodifyable.
        metadata = new HashMap<String,Object>(metadata);
        
        // set the id - this is required for the primary key.
        metadata.put(FileMetadataSchema.ID, id);

        // remove the version identifier if any - we do not want this modified!
        metadata.remove(FileMetadataSchema.VERSION);
        
        return getMetadataIndex().write(metadataSchema, metadata,
                AUTO_TIMESTAMP, null/* filter */,null/*precondition*/).asMap();
        
    }
    
    /**
     * Create a new file version using the supplied file metadata.
     * <p>
     * Note: This is essentially a delete + create operation. Since the combined
     * operation is NOT atomic it is possible that conflicts can arise when more
     * than one client attempts to update a file concurrently.
     * 
     * @param doc
     *            The file metadata.
     */
    public int update(Document doc) {
        
        Map<String,Object> metadata = doc.asMap();
        
        final String id = (String) metadata.get(FileMetadataSchema.ID); 
        
        // delete the existing file version (if any).
        delete( id );
        
        // create a new file version using that metadata.
        return create( doc );
        
    }

    /**
     * Note: A new file version is marked as deleted and then the file blocks
     * for the old version are deleted from the data index. This sequence means
     * (a) that clients attempting to read on the file using the high level API
     * will not see the file as soon as its metadata is updated; (b) that the
     * timestamp on the deleted version will be strictly LESS THAN the commit
     * time(s) when the file blocks are deleted, so reading from the timestamp
     * of the deleted version will let you see the deleted file blocks. This is
     * a deliberate convenience - if we were to delete the file blocks first
     * then we would not have ready access to a timestamp that would be before
     * the first file block delete and hence sufficient to perform a historical
     * read on the last state of the file before it was deleted.
     */
    public long delete(String id) {

        final RepositoryDocumentImpl doc = (RepositoryDocumentImpl) read(id);
        
        if (!doc.exists()) {
            
            // no current version.

            log.warn("No current version: id=" + id);

            return 0L;

        }

        final int version = doc.getVersion();
        
        /*
         * Mark the file version as deleted.
         * 
         * Note: This only deletes the "version" property - the other properties
         * are not changed.  Howevery, the file version will be understood as
         * "deleted" by this class.
         */
        {
            
            final Map<String, Object> metadata = new HashMap<String, Object>();

            // primary key.
            metadata.put(FileMetadataSchema.ID, id);

            // delete marker.
            metadata.put(FileMetadataSchema.VERSION, null);

            getMetadataIndex().write(metadataSchema, metadata, AUTO_TIMESTAMP,
                    null/* filter */, null/*precondition*/);
            
        }

        /*
         * Delete blocks from the file version.
         * 
         * Note: This is efficient in that it handles the delete on the data
         * service for each index partition. However, if the data spans more
         * than one index partition then the requests to delete the data on each
         * index partition are issued in sequence. A range-delete procedure
         * could be even more efficient since it can be parallelized when the
         * operation spans more than one index partition.
         */

        long blockCount = 0;

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        // the key for {file,version}
        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        // the key for {file,successor(version)}
        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        final ITupleIterator itr = getDataIndex().rangeIterator(fromKey, toKey,
                0/* capacity */, IRangeQuery.REMOVEALL, null/* filter */);

        while (itr.hasNext()) {

            itr.next();

            blockCount++;

        }

        if(INFO)
        log.info("Deleted " + blockCount + " blocks : id=" + id + ", version="
                + version);

        /*
         * There was a current version for the file. We have written a delete
         * marker and also deleted any blocks for that file version.
         */
        
        return blockCount;
        
    }

    /**
     * Return an array describing all non-eradicated versions of a file.
     * <p>
     * This method returns all known version identifiers together with their
     * timestamps, thereby making it possible to read either the metadata or the
     * data for historical file versions - as long as the metadata and/or data
     * has not yet been eradicated.
     * <p>
     * The file metadata and data blocks for historical version(s) of a file
     * remain available until they are eradicated from their respective indices
     * by a compacting merge in which the history policies no longer perserve
     * those data.
     * <p>
     * In order to read the historical file metadata you need to know the
     * timestamp associated with the version identifer which you wish to read.
     * This should be timestamp when that version was <em>deleted</em> MINUS
     * ONE in order to read the last valid metadata for the file version that
     * file version was deleted.
     * <p>
     * Likewise, in order to read the historical version data you need to know
     * the version identifer which you wish to read as well as the timestamp.
     * In this case, use the timestamp when that version was <em>deleted</em>
     * in order to read the last committed state for the file version.
     * <p>
     * Historical file version metadata is eradicated atomically since the
     * entire logical row will be hosted on the same index partition. Either the
     * file version metadata is available or it is now.
     * <p>
     * Historical file version data is eradicated one index partition at a time.
     * If the file version spans more than one index partition then it may be
     * possible to read some blocks from the file but not others.
     * <p>
     * Historical file version metadata and data will remain available until
     * their governing history policy is no longer satisified. Therefore, when
     * in doubt, you can consult the history policy in force for the file to
     * determine whether or not its data may have been eradicated.
     * 
     * @param id
     *            The file identifier.
     * 
     * @return An array containing (timestamp,version) tuples. Tuples where the
     *         {@link ITPV#getValue()} returns <code>null</code> give the
     *         timestamp at which a file version was <em>deleted</em>. Tuples
     *         where the {@link ITPV#getValue()} returns non-<code>null</code>
     *         give the timestamp at which a file version was <em>created</em>.
     * 
     * @see #readMetadata(String, long), to read the file version metadata based
     *      on a timestamp.
     * 
     * @see #inputStream(String, int, long), to read the file data as of a
     *      specific timestamp.
     * 
     * @todo expose history policy for a file (from its zone metadata, which is
     *       replicated onto the index partition metadata). Make sure that the
     *       zone metadata is consistent for the file version metadata and file
     *       version data. This means looking up the {@link IndexMetadata} for
     *       the index partition in which the file data is stored.
     */
    public ITPV[] getAllVersionInfo(String id) {
        
        /*
         * Query for all metadata for the file.
         */
        ITPS tps = readMetadata(id,Long.MAX_VALUE);

        Vector<ITPV> vec = new Vector<ITPV>();

        /*
         * Filter for only the version properties, skipping "delete" entries.
         */
        Iterator<? extends ITPV> itr = tps.iterator();
        
        while(itr.hasNext()) {
            
            ITPV tpv = itr.next();
            
            if(!tpv.getName().equals(FileMetadataSchema.VERSION)) {
                
                // Not a version field.
                
                continue;
                
            }

            vec.add(tpv);

        }

        return vec.toArray(new ITPV[vec.size()]);

    }
    
    /**
     * @todo write tests.
     */
    @SuppressWarnings("unchecked")
    public Iterator<? extends DocumentHeader> getDocumentHeaders(String fromId,
            String toId) {

        return new Striterator(getMetadataIndex().rangeQuery(
                metadataSchema, fromId, toId, 0/* capacity */,
                Long.MAX_VALUE/* timestamp */, null/* filter */))
                .addFilter(new Resolver() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    protected Object resolve(Object arg0) {
                        
                        ITPS tps = (ITPS) arg0;
                        
                        String id = (String) tps.get(FileMetadataSchema.ID).getValue();
                        
                        return new RepositoryDocumentImpl(
                                BigdataFileSystem.this, id, tps);
                        
                    }

                });
        
    }

    /**
     * Efficient delete of file metadata and file data for all files and file
     * versions spanned by the specified file identifiers.  File versions are
     * marked "deleted" before the file blocks are deleted so that you can
     * read on historical file version with exactly the same semantics as
     * {@link #delete(String)}.
     */
    public long deleteAll(String fromId, String toId) {
        
        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        // the key for {fromId}
        final byte[] fromKey = keyBuilder.reset().appendText(fromId,
                true/* unicode */, false/* successor */).getKey();

        // the key for {successor(toId)}
        final byte[] toKey = keyBuilder.reset().appendText(toId,
                true/* unicode */, true/* successor */).getKey();


        // delete file metadata
        long ndeleted = 0;
        {

            /*
             * Delete the file version metadata for each document in the key
             * range by replacing its VERSION column value with a null value
             * (and updating the timestamp in the key).
             */
            getMetadataIndex().getIndex().rangeIterator(
                    fromKey,
                    toKey,
                    0/* capacity */,
                    IRangeQuery.CURSOR,
                    new FilterConstructor<TPV>()
                            .addFilter(new FileVersionDeleter(
                                    SparseRowStore.AUTO_TIMESTAMP_UNIQUE)));
            
        }
        
        // delete file blocks.
        {

            final ITupleIterator itr = getDataIndex()
                    .rangeIterator(fromKey, toKey, 0/* capacity */,
                            IRangeQuery.REMOVEALL, null/* filter */);

            long blockCount = 0;

            while (itr.hasNext()) {

                itr.next();

                blockCount++;

            }
            
        }

        return ndeleted;
        
    }

    /**
     * FIXME Integrate with {@link FullTextIndex} to providing indexing and
     * search of file versions. Deleted file versions should be removed from the
     * text index. There should be explicit metadata on the file version in
     * order for it to be indexed. The text indexer will require content type
     * and encoding information in order to handle indexing. Low-level output
     * stream, writer, block write and block append operations will not trigger
     * the indexer since it depends on the metadata index to know whether or not
     * a file version should be indexed. However you could explicitly submit a
     * file version for indexing.
     * <p>
     * Perhaps the best way to handle this is to queue document metadata up for
     * a distributed full text indexing service. The service accepts metadata
     * for documents from the queue and decides whether or not the document
     * should be indexed based on its metadata and how the document should be
     * processed if it is to be indexed. Those business rules would be
     * registered with the full text indexing service. (Alternatively, they can
     * be configured with the {@link BigdataFileSystem} and applied locally as
     * the blocks of the file are written into the repository. That's certainly
     * easier right off the bat.)
     * 
     * @todo crawl or query job obtains a set of URLs, writing them onto a file.
     *       <p>
     *       m/r job downloads documents based on set of URLs, writing all
     *       documents into a single file version. text-based downloads can be
     *       record compressed and decompressed after the record is read. binary
     *       downloads will be truncated at 64M and might be skipped all
     *       together if the exceed the block size (get images, but not wildely
     *       large files).
     *       <p>
     *       m/r job extracts a simplified html format from the source image,
     *       writing the result onto another file. this job will optionally
     *       split documents into "pages" by breaking where necessary at
     *       paragraph boundaries.
     *       <p>
     *       m/r job builds text index from simplified html format.
     *       <p>
     *       m/r job runs extractors on simplified html format, producing
     *       rdf/xml which is written onto another file. The rdf/xml for each
     *       harvested document is written as its own logical record, perhaps
     *       one record per block.
     *       <p>
     *       concurrent batch load of rdf/xml into scale-out knowledge base. the
     *       input is a single file comprised of blocks, each of which is an
     *       rdf/xml file.
     */
    public Iterator<String> search(String query) {

        throw new UnsupportedOperationException();
        
    }

    /*
     * file data operations (read, atomic append).
     */
    
    /**
     * Returns an iterator that visits all block identifiers for the file
     * version in sequence.
     * <p>
     * Note: This may be used to efficiently distribute blocks among a
     * population of clients, e.g., in a map/reduce paradigm.
     */
    public Iterator<Long> blocks(String id, int version) {

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata()
                .getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        // just the keys.
        final int flags = IRangeQuery.KEYS;
        
        // visits the keys for the file version in block order.
        final ITupleIterator itr = getDataIndex().rangeIterator(fromKey, toKey,
                0/* capacity */, flags, null/* filter */);

        // resolve keys to block identifiers.
        return new BlockIdentifierIterator( id, version, itr );
        
    }
    
    /**
     * Copies blocks from one file version to another. The data in each block of
     * the source file version is copied into a new block that is appended to
     * the target file version. Empty blocks are copied. Partial blocks are NOT
     * combined. The block identifiers are NOT preserved since atomic append is
     * used to add blocks to the target file version.
     * 
     * @param fromId
     * @param fromVersion
     * @param toId
     * @param toVersion
     * 
     * @return The #of blocks copied.
     * 
     * FIXME This could be made more efficient by sending the copy operation to
     * each index partition in turn. that would avoid having to copy the data
     * first to the client and thence to the target index partition.
     */
    public long copyBlocks(String fromId, int fromVersion, String toId,
            int toVersion) {

        final Iterator<Long> src = blocks(fromId,fromVersion);
        
        long nblocks = 0L;
        
        while(src.hasNext()) {
        
            final long blockId = src.next();

            // read block
            final byte[] block = readBlock(fromId, fromVersion, blockId);
            
            // write block.
            appendBlock(toId, toVersion, block, 0, block.length);
            
            nblocks++;
            
        }
        
        return nblocks;
        
    }
    
    /**
     * Atomic write of a block for a file version.
     * <p>
     * Note: You can write any valid block identifier at any time. If the block
     * exists then its data will be replaced.
     * <p>
     * Note: Writing blocks out of sequence can create "holes". Those holes may
     * be filled by later writing the "missing" blocks.
     * {@link #copyBlocks(String, int, String, int)} will renumber the blocks
     * and produce a dense sequence of blocks.
     * <p>
     * Note: Atomic append will always write the successor of the largest block
     * identifier already written on the file version. If you write block
     * {@link #MAX_BLOCK} then it will no longer be possible to append blocks to
     * that file version, but you can still write blocks using
     * {@link #writeBlock(String, int, long, byte[], int, int)}.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param block
     *            The block identifier in [0:{@link #MAX_BLOCK}].
     * @param b
     *            The buffer containing the bytes to be written. When the buffer
     *            contains more than {@link #blockSize} bytes it will be broken
     *            up into multiple blocks.
     * @param off
     *            The offset of the 1st byte to be written.
     * @param len
     *            The #of bytes to be written.
     * 
     * @return <code>true</code> iff the block was overwritten (ie., if the
     *         block already exists, which case its contents were replaced).
     * 
     * @throws IllegalArgumentException
     *             if <i>id</id> is <code>null</code> or an empty string.
     * @throws IllegalArgumentException
     *             if <i>version</id> is negative.
     * @throws IllegalArgumentException
     *             if <i>block</id> is negative.
     * @throws IllegalArgumentException
     *             if <i>b</id> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>off</id> is negative or greater than the length of the
     *             byte[].
     * @throws IllegalArgumentException
     *             if <i>len</id> is negative or <i>off+len</i> is greater
     *             than the length of the byte[].
     * @throws IllegalArgumentException
     *             if <i>len</i> is greater than {@link #blockSize}.
     * 
     * @todo return the data for the old block instead in the case of an
     *       overwrite?
     */
    public boolean writeBlock(String id, int version, long block, byte[] b, int off, int len) {

        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (block < 0L) {
            /*
             * Note: restriction implies 63-bit block identifier (no
             * negative#s).
             */
            throw new IllegalArgumentException();
        }
        if (block > MAX_BLOCK) {
            throw new IllegalArgumentException();
        }
        if (b == null)
            throw new IllegalArgumentException();
        if (off < 0 || off > b.length)
            throw new IllegalArgumentException("off="+off+", b.length="+b.length);
        if (len < 0 || off + len > b.length)
            throw new IllegalArgumentException("off="+off+", len="+len+", b.length="+b.length);
        if(len>blockSize) {
            throw new IllegalArgumentException();
        }

        // construct the atomic write operation.
        final ISimpleIndexProcedure proc = new AtomicBlockWriteProc(this, id, version,
                block, b, off, len);

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        // the key for the {file,version,block}
        final byte[] key = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block).getKey();

        return (Boolean) getDataIndex().submit(key, proc);

    }

    /**
     * Atomic delete of the first block of the file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return The block identifier of the deleted block -or- <code>-1L</code>
     *         if nothing was deleted.
     */
    public long deleteHead(String id, int version) {

        if (INFO)
            log.info("id=" + id + ", version=" + version);

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata()
                .getKeyBuilder();

        // the key for {file,version}
        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        // the key for {file,successor(version)}
        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(
                version + 1).getKey();

        /*
         * The REMOVALL flag together with a limit of ONE (1) is used to obtain
         * an atomic delete of the first block for this file version.
         */

        final ITupleIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey,
                1, // Note: limit is ONE block!
                IRangeQuery.KEYS|IRangeQuery.REMOVEALL, null/* filter */);
        
        if (!itr.hasNext()) {

            log.warn("Nothing to delete: id=" + id + ", version=" + version);

            return -1L;

        }
        
        /*
         * Consume the iterator but note that the block was already deleted if
         * this was a remote request.
         */
        
        final long block = new BlockIdentifierIterator(id, version, itr).next();
            
        if(INFO)
        log.info("id="+id+", version="+version+" : deleted block="+block);

        return block;
        
    }
    
    /**
     * Atomic delete of a block for a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param block
     *            The block identifier -or- <code>-1L</code> to read the first
     *            block in the file version regardless of its block identifier.
     * 
     * @return <code>true</code> iff the block was deleted.
     */
    public boolean deleteBlock(String id, int version, long block) {
        
        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (block < 0L) {
            /*
             * Note: restriction implies 63-bit block identifier (no
             * negative#s).
             */
            throw new IllegalArgumentException();
        }
        if (block > MAX_BLOCK) {
            throw new IllegalArgumentException();
        }

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        final byte[] key = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block).getKey();
        
        /*
         * Note: The return value is just the serialized address of that block
         * on the journal (8 bytes).
         */
        
        final boolean deleted = getDataIndex().remove(key) != null;
        
        return deleted;
        
    }
    
    /**
     * Atomic read of the first block of the file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return The contents of the block -or- <code>null</code> iff there are
     *         no blocks for that file version. Note that an empty block will
     *         return an empty byte[] rather than <code>null</code>.
     */
    public byte[] readHead(String id, int version) {
        
        /*
         * Setup range scan than will span all blocks for the file version. We
         * are only interested in the first block, but this is how we get at its
         * data using an atomic read.
         */

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(0L).getKey();

        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(Long.MAX_VALUE).getKey();

        /*
         * Resolve the requested block : keys and data.
         */
        final ITupleIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

        if (!itr.hasNext()) {

            if (INFO)
                log.info("id=" + id + ", version=" + version + " : no blocks");

            return null;

        }

        return readBlock(id, version, itr.next());
        
    }
    
    /**
     * Atomic read of a block for a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param block
     *            The block identifier.
     * 
     * @return The contents of the block -or- <code>null</code> iff the block
     *         does not exist. Note that an empty block will return an empty
     *         byte[] rather than <code>null</code>.
     * 
     * @todo offer a variant that returns an {@link InputStream}?
     */
    public byte[] readBlock(String id, int version, long block) {
        
        if (id == null)
            throw new IllegalArgumentException();
        
        /*
         * Setup range scan than will span exactly the specified block.
         * 
         * Note: This uses a range scan because a lookup will return the address
         * of the block rather than its data!
         */

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block).getKey();

        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .append(block + 1).getKey();

        /*
         * Resolve the requested block : keys and data.
         */
        final ITupleIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

        if (!itr.hasNext()) {

            if (INFO)
                log.info("id=" + id + ", version=" + version + ", block="
                        + block + " : does not exist");

            return null;

        }

        return readBlock(id, version, itr.next());
        
    }

    /**
     * Helper to read a block from an {@link ITuple}.
     * 
     * @param id
     * @param version
     * @param tuple
     * @return
     */
    private byte[] readBlock(String id, int version, ITuple tuple) {
        
        final byte[] key = tuple.getKey();
        
        // decode the block identifier from the key.
//        block = KeyBuilder.decodeLong(tuple.getKeyBuffer().array(),
//                tuple.getKeyBuffer().pos() - Bytes.SIZEOF_LONG);
        long block = KeyBuilder.decodeLong(key, key.length - Bytes.SIZEOF_LONG);

        final long addr;
        try {

            DataInput in = tuple.getValueStream();
        
            addr = in.readLong();
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
        if (addr == 0L) {

            /*
             * Note: empty blocks are allowed and are recorded with 0L as
             * their address.
             */

            if(INFO)
            log.info("id=" + id + ", version=" + version + ", block=" + block
                    + " : empty block.");

            return new byte[]{};

        }
        
        /*
         * Read the block from the backing store.
         */
        final IBlock tmp = tuple.readBlock(addr);

        final int len = tmp.length();
        
        if(INFO)
        log.info("id=" + id + ", version=" + version + ", block=" + block
                + " : " + len + " bytes");

        // @todo reuse buffers, but must return {byte[],off,len} tuple.
        final byte[] data = new byte[len];
        
        try {

            final int nread = tmp.inputStream().read(data, 0, len);

            if (nread != len) {

                throw new RuntimeException("Expecting to read " + len
                        + " bytes but read " + nread + " bytes");

            }
            
        } catch (IOException e) {
            
            throw new RuntimeException(e);
            
        }
        
        return data;

    }

    /**
     * Atomic append of a block to a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param b
     *            The buffer containing the data to be written..
     * @param off
     *            The offset of the 1st byte to be written.
     * @param len
     *            The #of bytes to be written in [0:{@link #blockSize}].
     * 
     * @return The block identifer for the written block.
     * 
     * @throws IllegalArgumentException
     *             if <i>id</id> is <code>null</code> or an empty string.
     * @throws IllegalArgumentException
     *             if <i>version</id> is negative.
     * @throws IllegalArgumentException
     *             if <i>b</id> is <code>null</code>.
     * @throws IllegalArgumentException
     *             if <i>off</id> is negative or greater than the length of the
     *             byte[].
     * @throws IllegalArgumentException
     *             if <i>len</id> is negative or <i>off+len</i> is greater
     *             than the length of the byte[].
     * @throws IllegalArgumentException
     *             if <i>len</i> is greater than {@link #blockSize}.
     */
    public long appendBlock(String id, int version, byte[] b, int off, int len) {
        
        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (off < 0 || off > b.length)
            throw new IllegalArgumentException("off="+off+", b.length="+b.length);
        if (len < 0 || off + len > b.length)
            throw new IllegalArgumentException("off="+off+", len="+len+", b.length="+b.length);
        if (len > blockSize) {
            throw new IllegalArgumentException();
        }

        // construct the atomic append operation.
        final ISimpleIndexProcedure proc = new AtomicBlockAppendProc(this, id,
                version, b, off, len);

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();
        
        // the last possible key for this file
        final byte[] key = keyBuilder.reset().appendText(id,
                true/* unicode */, true/* successor */).append(version)
                .append(-1L).getKey();

        /*
         * Figure out which index partition will absorb writes on the end of the
         * file. We do this by finding the index partition that would contain
         * the successor of the id and then considering its leftSeparator. If
         * the leftSeparator is greater than the id then the id does not enter
         * this index partition and we use the prior index partition. Otherwise
         * the id enters this partition and we use it.
         * 
         * Note: File versions allow us to avoid painful edge cases when a file
         * has been deleted that spans more than one index partition. Since we
         * never attempt to write on the deleted file version we are not faced
         * with the problem of locating the largest index partition that
         * actually has data for that file. When a large file has been deleted
         * there can be EMPTY index partitions (containing only deleted entries)
         * until the next compacting merge.
         */
        return (Long) getDataIndex().submit(key, proc);
        
    }

    /**
     * Return the maximum #of blocks in the file version. The return value
     * includes any deleted but not yet eradicated blocks for the specified file
     * version, so it represents an upper bound on the #of blocks that could be
     * read for that file version.
     * <p>
     * Note: the block count only decreases when a compacting merge eradicates
     * deleted blocks from an index partition. It will increase any time there
     * is a write on a block for the file version for which neither a delete nor
     * an undeleted entry exists. The only way to count the #of non-deleted
     * blocks for a file version is to traverse the {@link #blocks(String, int)}
     * iterator.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version identifier.
     * 
     * @return The #of blocks in that file.
     */
    public long getBlockCount(String id, int version) {
     
        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata()
                .getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        final long nblocks = getDataIndex().rangeCount(fromKey, toKey);

        if (INFO)
            log.info("id=" + id + ", version=" + version + ", nblocks=" + nblocks);

        return nblocks;
        
    }

    /**
     * Return a {@link Writer} that will <em>append</em> character data on the
     * file version. Characters written on the {@link Writer} will be converted
     * to bytes using the specified encoding. Bytes will be buffered until the
     * block is full and then written on the file version using an atomic
     * append. An {@link Writer#flush()} will force a non-empty partial block to
     * be written immediately.
     * <p>
     * Note: Map/Reduce processing of a file version MAY be facilitated greatly
     * by ensuring that "records" never cross a block boundary - this means that
     * file versions can be split into blocks and blocks distributed to clients
     * without any regard for the record structure within those blocks. The
     * caller can prevent records from crossing block boundaries by the simple
     * expediency of invoking {@link Writer#flush()} to force the atomic append
     * of a (partial but non-empty) block to the file.
     * <p>
     * Since the characters are being converted to bytes, the caller MUST make
     * {@link Writer#flush()} decisions with an awareness of the expansion rate
     * of the specified encoding. For simplicity, it is easy to specify
     * <code>UTF-16</code> in which case you can simply count two bytes
     * written for each character written.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param encoding
     *            The character set encoding.
     * 
     * @return The writer on which to write the character data.
     * 
     * @throws UnsupportedEncodingException
     */
    public Writer writer(String id, int version, String encoding)
            throws UnsupportedEncodingException {
        
        if(INFO)
        log.info("id="+id+", version="+version+", encoding="+encoding);

        return new OutputStreamWriter(outputStream(id, version), encoding);
        
    }
    
    /**
     * Read character data from a file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param encoding
     *            The character set encoding.
     *            
     * @return The reader from which you can read the character data.
     * 
     * @throws UnsupportedEncodingException
     */
    public Reader reader(String id, int version, String encoding) throws UnsupportedEncodingException {

        if(INFO)
        log.info("id="+id+", version="+version+", encoding="+encoding);
        
        if (encoding == null) {

            throw new IllegalStateException();
            
        }
        
        return new InputStreamReader(inputStream(id, version), encoding);

    }

    /**
     * Read data from a file version.
     * <p>
     * Note: The input stream will remain coherent for the file version as of
     * the time that the view on the file version is formed. Additional atomic
     * appends MAY be read, but that is NOT guarenteed. If the file is deleted
     * and its data is expunged by a compacting merge during the read then the
     * read MAY be truncated.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return An input stream from which the caller may read the data in the
     *         file -or- <code>null</code> if there is no data for that file
     *         version, including no deleted blocks pending garbage collection.
     *         An empty input stream MAY be returned since empty blocks are
     *         allowed. An empty stream will also be returned after a file
     *         version is deleted until the deleted blocks are eradicated from
     *         the file data index.
     */
    public FileVersionInputStream inputStream(String id,int version) {

        return inputStream(id, version, ITx.UNISOLATED);
        
    }

    /**
     * Read data from a file version.
     * <p>
     * Some points about consistency and transaction identifiers.
     * <ol>
     * 
     * <li> When using an {@link ITx#UNISOLATED} read addition atomic writes and
     * atomic appends issued after the input stream view was formed MAY be read,
     * but that is NOT guarenteed - it depends on the buffering of the range
     * iterator used to read blocks for the file version. Likewise, if the file
     * is deleted and its data is expunged by a compacting merge during the read
     * then the read MAY be truncated. </li>
     * 
     * <li> It is possible to re-create historical states of a file version
     * corresponding to a <em>commit point</em> for the
     * {@link #getDataIndex() data index} provided that the relevant data has
     * not been eradicated by a compacting merge. It is not possible to recover
     * all states - merely committed states - since unisolated writes may be
     * grouped together by group commit and therefore have the same commit
     * point. </li>
     * 
     * <li> It is possible to issue transactional read requests, but you must
     * first open a transaction with an {@link ITransactionManagerService}. In general
     * the use of full transactions is discouraged as the
     * {@link BigdataFileSystem} is designed for high throughput and high
     * concurrency with weaker isolation levels suitable for scale-out
     * processing techniques including map/reduce.</li>
     * 
     * </ol>
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param tx
     *            The transaction identifier. This is generally either
     *            {@link ITx#UNISOLATED} to use an unisolated read -or-
     *            <code>- timestamp</code> to use a historical read for the
     *            most recent consistent state of the file data not later than
     *            <i>timestamp</i>.
     * 
     * @return An input stream from which the caller may read the data in the
     *         file -or- <code>null</code> if there is no data for that file
     *         version, including no deleted blocks pending garbage collection.
     *         An empty input stream MAY be returned since empty blocks are
     *         allowed. An empty stream will also be returned after a file
     *         version is deleted until the deleted blocks are eradicated from
     *         the file data index.
     */
    public FileVersionInputStream inputStream(String id, int version, long tx) {

        if (INFO)
            log.info("id=" + id + ", version=" + version + ", tx=" + tx);

        /*
         * Range count the file and version on the federation - this is the
         * number of blocks of data for that file and version as of the start of
         * this read operation. If the result is zero then there are no index
         * partitions which span that file and version and we return null.
         * 
         * Note: This step is skipped for historical and transactional reads
         * since getBlockCount() does not accept the transaction identifier.
         */

        if (tx == ITx.UNISOLATED && getBlockCount(id, version) == 0L) {

            if (INFO)
                log.info("No data: id=" + id + ", version=" + version);

            return null;

        }
        
        /*
         * Return an input stream that will progress through a range scan of the
         * blocks for that file and version.
         */

        final IKeyBuilder keyBuilder = getDataIndex().getIndexMetadata().getKeyBuilder();

        final byte[] fromKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version)
                .getKey();

        final byte[] toKey = keyBuilder.reset().appendText(id,
                true/* unicode */, false/* successor */).append(version + 1)
                .getKey();

        /*
         * The capacity is essentially the #of block addresses to transfer at a
         * time, not the #of blocks. I've set a moderately low limit here since
         * the blocks themselves need to be transferred as well, so there is
         * little point in buffering too many block addresses.
         * 
         * The addresses associated with a block identifier are updated when the
         * block is re-written, so if you buffer a lot of block addresses here
         * then updates to the blocks for the buffered identifiers will not be
         * visible to the client.
         * 
         * Finally, for very large files you may find that the block addresses
         * grow stale (the resource on which they were written may be moved or
         * deleted following a compacting merge), forcing a re-start of the read
         * from the last visited block identifier.
         * 
         * @todo handle automatic restart of the read from the next block
         * identifier if we learn that the resource on which a block was written
         * has been deleted.
         */
        final int capacity = 1000;
        
        // both keys and values.
        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        final ITupleIterator itr;
        
        final IIndex dataIndex;
        
        if (tx == ITx.UNISOLATED) {

            dataIndex = getDataIndex();

        } else {

            /*
             * Obtain the index view for that historical timestamp or isolated
             * by the specified transaction.
             */

            dataIndex = getIndexManager().getIndex(getNamespace()+FILE_DATA_INDEX_BASENAME,tx);
            
        }

        itr = dataIndex
                .rangeIterator(fromKey, toKey, capacity, flags, null/* filter */);

        return new FileVersionInputStream(id, version, itr);
        
    }

    /**
     * Return an output stream that will <em>append</em> on the file version.
     * Bytes written on the output stream will be buffered until they are full
     * blocks and then written on the file version using an atomic append. An
     * {@link OutputStream#flush()} will force a non-empty partial block to be
     * written immediately.
     * <p>
     * Note: Map/Reduce processing of a file version MAY be facilitated greatly
     * by ensuring that "records" never cross a block boundary - this means that
     * files can be split into blocks and blocks distributed to clients without
     * any regard for the record structure within those blocks. The caller can
     * prevent records from crossing block boundaries by the simple expediency
     * of invoking {@link OutputStream#flush()} to force the atomic append of a
     * (partial but non-empty) block to the file.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return The output stream.
     */
    public OutputStream outputStream(String id, int version) {

        if(INFO)
        log.info("id="+id+", version="+version);

        return new FileVersionOutputStream(this, id, version);

    }
    
    /**
     * Copies data from the input stream to the file version. The data is
     * buffered into blocks. Each block is written on the file version using an
     * atomic append. Writing an empty stream will cause an empty block to be
     * appended (this ensures that read back will read an empty stream).
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * @param is
     *            The input stream (closed iff it is fully consumed).
     * 
     * @return The #of bytes copied.
     */
    public long copyStream(String id, int version, InputStream is) {
        
        final FileVersionOutputStream os = (FileVersionOutputStream) outputStream(
                id, version);

        final long ncopied;
        
        try {

            ncopied = os.copyStream( is );
       
            if (ncopied == 0) {
                
                // force an empty block to be written.
                appendBlock(id, version, new byte[]{}, 0, 0);
                
            }
            
            os.close();
            
        } catch(IOException ex) {
            
            throw new RuntimeException(ex);
            
        }
        
        return ncopied;

    }

}
