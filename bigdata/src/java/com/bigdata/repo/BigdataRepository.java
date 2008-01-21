/*
 * Created on Jan 17, 2008
 */
package com.bigdata.repo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.BTree;
import com.bigdata.btree.BTreeMetadata;
import com.bigdata.btree.BytesUtil;
import com.bigdata.btree.EntryFilter;
import com.bigdata.btree.IEntryFilter;
import com.bigdata.btree.IEntryIterator;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IKeyBuilder;
import com.bigdata.btree.ILinearList;
import com.bigdata.btree.IRangeQuery;
import com.bigdata.btree.IndexProcedure;
import com.bigdata.btree.IndexSegment;
import com.bigdata.btree.IndexSegmentFileStore;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.io.DataInputBuffer;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.IByteArrayBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IRawStore;
import com.bigdata.scaleup.IPartitionMetadata;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.scaleup.PartitionMetadata;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.DataService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IDataService;
import com.bigdata.service.PartitionMetadataWithSeparatorKeys;
import com.bigdata.service.UnisolatedBTreePartition;
import com.bigdata.service.UnisolatedBTreePartitionConstructor;
import com.bigdata.sparse.KeyType;
import com.bigdata.sparse.Schema;
import com.bigdata.sparse.SparseRowStore;

/**
 * A distributed file system with extensible metadata and atomic append
 * implemented using the bigdata scale-out architecture. Files have a client
 * assigned identifier, which is a Unicode string. The file identifier MAY be
 * structured so as to look like a hierarchical file system using any desired
 * convention. Files are versioned and historical versions MAY be accessed until
 * the next compacting merge discards their data. File data is stored in large
 * {@link #BLOCK_SIZE} blocks, but partial and even empty blocks are allowed.
 * Storage is space - only the data written will be stored.
 * <p>
 * Efficient method are offered for streaming and block oriented IO. All block
 * read and write operations are atomic, including block append. Files may be
 * easily written such that records never cross a block boundary by the
 * expediency of flushing the output stream if a record would overflow the
 * current block. (A flush forces the atomic write of a partial block. Partial
 * blocks are stored efficiently - only the bytes actually written are stored.)
 * Such files are well-suited to map/reduce processing as they may be
 * efficiently split at block boundaries and references to the blocks
 * distributed to clients. Likewise, reduce clients can aggregate data into
 * large files suitable for further map/reduce processing.
 * <p>
 * The distributed file system uses two scale-out indices to support ACID
 * operations on file metadata and atomic file append. These ACID guarentees
 * arise from the use of unisolated operations on the respective indices and
 * therefore apply only to the individual file metadata or file block
 * operations. In particular, file metadata read and write are atomic and all
 * individual file block IO (read,write,and append) operations are atomic.
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
 * The metadata index uses a {@link SparseRowStore} design, similar to Google's
 * bigtable or Hadoop's HBase. Certain properties MUST be defined for each entry -
 * they are documented on the {@link MetadataSchema}. Applications are free to
 * define additional properties. Reads and writes of file metadata are always
 * atomic.
 * <p>
 * Each time a file is created a new version number is assigned. The data index
 * uses the {@link MetadataSchema#ID} as the first field in a compound key. The
 * second field is the {@link MetadataSchema#VERSION} - a 32-bit integer. The
 * remainder of the key is a 64-bit block identifier. The block identifiers are
 * strictly monotonic (e.g., up one) and their sequence orders the blocks into
 * the logical byte order of the file.
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
 * pad out with unused bytes to the next 64k boundary).
 * <p>
 * Use case: A reduce client wants to write a very large files so it creates a
 * metadata record for the file and then does a series of atomic appears to the
 * file. The file may grow arbitrarily large. Clients may begin to read from the
 * file as soon as the first block has been flushed.
 * <p>
 * Use case: Queue-like mechanisms MAY be created. A file version is written by
 * clients that ensure that records never cross block boundaries (e.g., by
 * flushing a partial block if the next record would cause the block to
 * overflow). A master reads from the head of the file version, obtaining a set
 * of records. Those records are distributed to clients for processing. Once all
 * records in the head of the block have been consumed the master performs an
 * atomic delete of the head block. Clients MAY continue to append to the file
 * version while the master is running. The result is a block oriented queue
 * sharing many features of the map/reduce architecture.
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
 * @todo provide scans for file versions, e.g., given a file, what versions
 *       exist.
 * 
 * @todo implement "zones" and their various policies (replication, retention,
 *       and media indexing).
 * 
 * @todo compacting merge policies. consider how data is eradicated from the
 *       metadata and data indices and that it might not be "atomically"
 *       eradicated so it is quite possible that only some data will remain
 *       available after a period of time. the policy can set the minimum
 *       criteria, e.g., in terms of time and #of versions to be retained. After
 *       those criteria have been met you may still find your data or not. You
 *       can read blocks directly for a file version and blocks that have not
 *       been eradicated will still have valid data.
 * 
 * @todo should compression be applied? applications are obviously free to apply
 *       their own compression, but it could be convienent to stored compressed
 *       blocks. the caller could specify the compression method on a per block
 *       basis (we don't want to lookup the file metadata for this). the
 *       compression method would be written into a block header. blocks can
 *       always be decompressed by examining the header.
 * 
 * @todo Full text indexing support. Perhaps the best way to handle this is to
 *       queue document metadata up for a distributed full text indexing
 *       service. The service accepts metadata for documents from the queue and
 *       decides whether or not the document should be indexed based on its
 *       metadata and how the document should be processed if it is to be
 *       indexed. Those business rules would be registered with the full text
 *       indexing service. (Alternatively, they can be configured with the
 *       {@link BigdataRepository} and applied locally as the blocks of the file
 *       are written into the repository. That's certainly easier right off the
 *       bat.)
 * 
 * @todo full transactions could be used to perform atomic updates on the file
 *       metadata. in a way this is easier than resorting to a distinct locking
 *       mechanism. file data writes MUST be outside of the transaction.
 * 
 * @todo ensure that index partitions do not break across the metadata record
 *       for a file.
 * 
 * @todo there should be some constraints on the file identifier but it general
 *       it represents a client determined absolute file path name. It is
 *       certainly possible to use a flat file namespace, but you can just as
 *       readily use a hierarchical one. Unicode characters are supported in the
 *       file identifiers.
 * 
 * @todo create an abstract base class and derive two versions: one runs against
 *       and embedded data service (supports restart but does not use jini) and
 *       the other runs against a federation (scale-out deployment).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRepository implements ContentRepository {

    protected static Logger log = Logger.getLogger(BigdataRepository.class);
    
    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final public static boolean INFO = log.getEffectiveLevel().toInt() <= Level.INFO
            .toInt();

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final public static boolean DEBUG = log.getEffectiveLevel().toInt() <= Level.DEBUG
            .toInt();

    /**
     * Configuration options.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static interface Options extends com.bigdata.journal.Options, KeyBuilder.Options {

    }

    /**
     * The size of a file block (65,536 bytes aka 64k). Block identifiers are
     * 64-bit integers. The maximum file length is <code>2^(16+64) - 1 </code>
     * bytes (16 Exabytes).
     */
    public static final int BLOCK_SIZE = 64 * Bytes.kilobyte32;

    /**
     * The maximum block identifier that can be assigned to a file version.
     * <p>
     * Note: This is limited to {@value Long#MAX_VALUE}-1 so that we can always
     * form the key greater than any valid key for a file version. This is
     * required by the atomic append logic when it seeks the next block
     * identifier. See {@link AtomicAppendProc}.
     */
    protected static final long MAX_BLOCK = Long.MAX_VALUE - 1;
    
    /**
     * The connection to the bigdata federation.
     */
    private IBigdataFederation fed;
    
    /**
     * The name of the scale-out index in which the metadata are stored. This is
     * a {@link SparseRowStore} governed by the {@link #metadataSchema}.
     */
    private static final String METADATA_NAME = BigdataRepository.class.getSimpleName()+"#metadata";
    
    /**
     * The name of the scale-out index in which the data are stored. The entries
     * in this index are a series of blocks for a file. Blocks are
     * {@link #BLOCK_SIZE} bytes each and are assigned monotonically increasing
     * block numbers by the atomic append operation. The final block may be
     * smaller (there is no need to pad out the data with nulls). The keys are
     * formed from two fields - a field containing the content identifier
     * followed by an integer field containing the sequential block number. A
     * range scan with a fromKey of the file identifier and a toKey computed
     * using the successor of the file identifier will naturally visit all
     * blocks in a file in sequence.
     */
    private static final String DATA_NAME = BigdataRepository.class.getSimpleName()+"#data";
    
    /**
     * A shared instance configured using the properties specified to the ctor.
     * <p>
     * Note: This object is NOT thread-safe. Code MUST be synchronized on this
     * object.
     */
    private final IKeyBuilder keyBuilder;
    
    /**
     * The schema for metadata about blobs stored in the repository. Some well
     * known properties are always defined, but any property may be stored -
     * ideally within their own namespace!
     * 
     * @todo An Last-Modified property COULD be defined by the application, but
     *       the application will have to explicitly update that property - it
     *       will NOT be updated when a block is written on the file version
     *       since block IO is decoupled by design from reading or writing the
     *       file metadata.  (This is the same reason why Content-Length is NOT
     *       defined.)
     * 
     * @todo other obvious metadata would include the user identifier.
     * 
     * @todo if permissions are developed for the file system (a nightmare) then
     *       that metadata would go here as well.
     */
    public static class MetadataSchema extends Schema {
        
        /**
         * The content identifer is an arbitrary Unicode {@link String} whose
         * value may be defined by the client.
         * 
         * @todo support server-side generation of the id attribute.
         */
        public static final String ID = "Id";

//        /**
//         * The length of the encoded content #of bytes (the same semantics as
//         * the HTTP <code>Content-Length</code> header).
//         */
//        public static final String CONTENT_LENGTH = "ContentLength";
        
        /**
         * The MIME type associated with the content (the same semantics as the
         * HTTP <code>Content-Type</code> header).
         */
        public static final String CONTENT_TYPE = "ContentType";

        /**
         * The encoding, if any, used to convert the byte[] content to
         * characters.
         * <p>
         * Note: This is typically deduced from an analysis of the MIME Type in
         * <code>Content-Type</code> header and at times the leading bytes of
         * the response body itself.
         */
        public static final String CONTENT_ENCODING = "ContentEncoding";

        /**
         * The file version number. Together the file {@link #ID} and the file
         * {@link #VERSION} form the primary key for the data index.
         */
        public static final String VERSION = "Version";
        
        public MetadataSchema() {
            
            super("metadata", "id", KeyType.Unicode);
            
        }
        
    }

    public static final MetadataSchema metadataSchema = new MetadataSchema();
    
    private final Properties properties;
    
    private SparseRowStore metadataIndex;
    
    private IIndex dataIndex;
    
    // @todo SparseRowStore should defined this constant.
    final long AUTO_TIMESTAMP = -1L;
        
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
     * @param client
     *            The client.
     * @param properties
     *            See {@link Options}.
     */
    public BigdataRepository(IBigdataFederation fed,Properties properties) {
        
        this.fed = fed;
        
        // clone the properties to keep them immutable.
        this.properties = (Properties) properties.clone();

        // configured shared key builder instance for use by the client.
        this.keyBuilder = KeyBuilder.newUnicodeInstance(properties);
        
    }

    /**
     * An object wrapping the properties provided to the ctor.
     */
    protected Properties getProperties() {
        
        return new Properties(properties);
        
    }

    /**
     * The index in which the file metadata is stored (the index must exist).
     */
    protected SparseRowStore getMetadataIndex() {

        if (metadataIndex == null) {

            IIndex ndx = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, METADATA_NAME);

            metadataIndex = new SparseRowStore(ndx, KeyBuilder
                    .newUnicodeInstance(properties), metadataSchema);
        }

        return metadataIndex;

    }

    /**
     * The index in which the file data is stored (the index must exist).
     */
    protected IIndex getDataIndex() {

        if (dataIndex == null) {

            dataIndex = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, DATA_NAME);

        }

        return dataIndex;

    }
    
    /**
     * Registers the scale-out indices.
     * 
     * @todo make this an atomic operation.
     */
    public void registerIndices() {

        final int branchingFactor = Integer.parseInt(properties.getProperty(
                Options.BRANCHING_FACTOR, Options.DEFAULT_BRANCHING_FACTOR));
        
        // setup metadata index.
        {

            fed.registerIndex(METADATA_NAME,
                    new UnisolatedBTreePartitionConstructor(branchingFactor));

            IIndex ndx = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, METADATA_NAME);

            metadataIndex = new SparseRowStore(ndx, KeyBuilder
                    .newUnicodeInstance(properties), metadataSchema);
            
        }

        // setup data index.
        {

            // Note: resolves addrs -> blocks on the journal.
            fed.registerIndex(DATA_NAME,
                    new FileDataBTreePartitionConstructor(branchingFactor));

            dataIndex = (ClientIndexView) fed.getIndex(
                        IBigdataFederation.UNISOLATED, DATA_NAME);
            
        }

    }

    /**
     * NOP - the caller should disconnect their client from the federation when
     * they are no longer using that connection.
     */
    public void close() {
        
    }

    /**
     * @todo do we need a global lock mechanism to prevent concurrent
     *       create/update/delete of the same file? Perhaps a lease-based lock?
     *       Can this be supported with the historical and not yet purged
     *       timestamped metadata for the file?
     */
    public void create(Document document) {
        
        if (document == null)
            throw new IllegalArgumentException();
        
        final String id = document.getId(); 
        
        if (id == null)
            throw new RuntimeException("The " + MetadataSchema.ID
                    + " property must be defined.");
        
        if ( read( id ) != null ) {
            
            throw new ExistsException(id);
            
        }
        
        Map<String,Object> properties = new HashMap<String,Object>();
        
        Iterator<PropertyValue> itr = document.propertyValues();
        
        while(itr.hasNext()) {
            
            PropertyValue tmp = itr.next();
            
            properties.put(tmp.getName(), tmp.getValue());
            
        }

        // required properties.
        assertString(properties, MetadataSchema.ID);
        assertString(properties, MetadataSchema.CONTENT_TYPE);
        assertString(properties, MetadataSchema.CONTENT_ENCODING);
//        assertLong(properties, MetadataSchema.CONTENT_LENGTH);

        // write the metadata (atomic operation).
        metadataIndex.write(properties, AUTO_TIMESTAMP);

        /*
         * FIXME Lookup and increment. Until we have overflow of the journal
         * this does not matter since we will never encounter the problems that
         * file versions are designed to solve (but of course you will not be
         * able to access the historical versions of the file).
         */
        final int version = 0;

        copyStream(id, version, document.getInputStream());
        
    }
    
    public Document read(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Return the current metadata for the file.
     * <p>
     * Note: This method will return metadata for a file whose most recent
     * version is deleted.
     * 
     * @param id
     *            The file identifier.
     * 
     * @return The most recent metadata for that file.
     */
    public Map<String,Object> _read(String id) {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Note: This interface replaces a files content.  if you want to append to
     * an existing file use {@link #atomicAppend(String, int, byte[])} or
     * {@link #writeContent(String, InputStream)}.
     * 
     * FIXME Update that overwrites the content of the file MUST assign a new
     * file version identifer!
     */
    public void update(Document document) {
        // TODO Auto-generated method stub
        
    }

    /**
     * FIXME define a range-delete operation so we do not have to materialize
     * the keys to delete the data. This can be just an {@link IIndexProcedure}
     * but it should not extend {@link IndexProcedure} since that expects the
     * serialization of keys and values with the request. In fact, a common base
     * class could doubtless be identified for use with the range iterator as
     * well.
     * 
     * FIXME delete MUST indicate that the current file version identifier is no
     * longer valid such that a subsequent create will use a new file version
     * identifier.
     */
    public void delete(String id) {
        // TODO Auto-generated method stub
        
    }

    public void deleteAll(String fromId, String toId) {
        // TODO Auto-generated method stub
        
    }

    public Iterator<? extends DocumentHeader> getDocumentHeaders(String fromId, String toId) {
        // TODO Auto-generated method stub
        return null;
    }

    public Iterator<String> search(String query) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * file data operations (read, atomic append).
     */
    
    /**
     * Atomic append of a single block to a file version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AtomicAppendProc implements IIndexProcedure,
            Externalizable {

        private static final long serialVersionUID = 1441331704737671258L;

        protected static transient Logger log = Logger
                .getLogger(AtomicAppendProc.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final public static transient boolean INFO = log.getEffectiveLevel()
                .toInt() <= Level.INFO.toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final public static transient boolean DEBUG = log.getEffectiveLevel()
                .toInt() <= Level.DEBUG.toInt();

        private String id;
        private int version;
        private int off;
        private int len;
        private byte[] b;
        
        /**
         * 
         * @param id
         *            The file identifier.
         * @param version
         *            The file version.
         * @param b
         *            The buffer containing the data to be written.
         * @param off
         *            The offset in the buffer of the first byte to be written.
         * @param len
         *            The #of bytes to be written.
         */
        public AtomicAppendProc(String id, int version, byte[] b, int off, int len) {

            assert id != null && id.length() > 0;
            assert version >= 0;
            assert b != null;
            assert off >= 0 : "off="+off;
            assert len >= 0 && off + len <= b.length;
            assert len <= BLOCK_SIZE : "len="+len+" exceeds blockSize="+BLOCK_SIZE;

            this.id = id;
            this.version = version;
            this.off = off;
            this.len = len;
            this.b = b;

        }
        
        /**
         * This procedure runs on the unisolated index. The block identifier is
         * computed as a one up long integer for that file version using locally
         * available state. The raw data for the block is written directly onto
         * the {@link Journal} and an index entry is added for the file,
         * version, and block whose value is the address of the block's data on
         * the {@link Journal}.
         * <p>
         * Note: The caller MUST have correctly identified the data service on
         * which the tail of the file exists (or on which the head of the file
         * will be written).
         * <p>
         * The block identifier is computed by reading and decoding the key for
         * the last block written for this file version (if any). Special cases
         * exist when the file version spans more than one index partition, when
         * the block would be the first block (in key order) for the index
         * partition, and when the block would be the last block (in key order)
         * for the index partition.
         * 
         * @return <code>true</code> iff the block was overwritten.
         */
        public Object apply(IIndex ndx) {

            // tunnel through to the backing journal.
            final AbstractJournal journal = (AbstractJournal)((AbstractBTree)ndx).getStore();
            
            // obtain the thread-local key builder for that journal.
            final IKeyBuilder keyBuilder = journal.getKeyBuilder();
            
            /*
             * The next block identifier to be assigned.
             */
            final long block;
            {

                /*
                 * Find the key for the last block written for this file
                 * version. We do this by forming a probe key from the file,
                 * version, and the maximum allowed block identifier. This is
                 * guarenteed to be after any existing block for that file and
                 * version.
                 * 
                 * @todo This implies that the leftSeparator for the index
                 * partition MUST NOT split the blocks for a file unless there
                 * is at least one block in the index partition. In practice
                 * this guarentee is easy to maintain. By default we choose to
                 * split an index partition on a file boundary. If that would
                 * result in an uneven split (or an empty split in the case of
                 * very large files) then we choose a split point that lies
                 * within the file's data - leaving at least one block for the
                 * file (probably many) in both partitions created by the split.
                 */
                
                final byte[] toKey = keyBuilder.reset().appendText(id,
                        true/* unicode */, false/* successor */).append(
                        version).append(Long.MAX_VALUE).getKey();

                // @todo promote this interface onto IIndex?
                // @todo verify iface implemented for index partition view.
                final ILinearList tmp = (ILinearList)ndx;
                
                /*
                 * Index of the first key after this file version.
                 * 
                 * Note: This will always be an insertion point (a negative
                 * value) since the toKey only encodes the successor of the file
                 * identifier.
                 * 
                 * We convert the insertion point to an index.
                 * 
                 * If the index is zero (0) then there are no blocks for this
                 * file and the file will be the first file in the index order
                 * on this index partition (there may or may not be other files
                 * already on the index partition).
                 * 
                 * Else fetch the key at that index. If that key encodes the
                 * same id as this file then we are appending to a file with
                 * existing block(s) and we decode the block identifier from the
                 * key. Otherwise this will be the first block written for that
                 * file.
                 */
                int toIndex = tmp.indexOf(toKey);

                assert toIndex < 0 : "Expecting insertion point: id=" + id
                        + ", version=" + version + ", toIndex=" + toIndex;

                log.debug("insertionPoint="+toIndex);
                
                toIndex = -(toIndex+1); // convert to an index.

                // #of entries in the index.
                final int entryCount = ((AbstractBTree)ndx).getEntryCount();
                
                log.debug("toIndex="+toIndex+", entryCount="+entryCount);

                if (toIndex == 0) {

                    /*
                     * Insertion point is before all other entries in the index.
                     * 
                     * Note: In this case we need to examine the leftSeparator
                     * key for the index partition. If that key is for the same
                     * file version then we use the successor of the block
                     * identifier found in that key.
                     * 
                     * Note: when it is not for the same file version it MAY be
                     * that the leftSeparator does not include the block
                     * identifier - the block identifier is only required in the
                     * leftSeparator when the a file version spans both the
                     * prior index partition and this index partition.
                     */
                    
                    log.debug("Insertion point is before all entries in the index partition: id="
                                    + id + ", version=" + version);
                    
                    final byte[] leftSeparator = ((UnisolatedBTreePartition) ndx)
                            .getPartitionMetadata().getLeftSeparatorKey();

                    block = getNextBlockFromPriorKey(keyBuilder, leftSeparator);
                    
                } else {
                    
                    if (toIndex == entryCount) {

                        /*
                         * Insertion point is after all entries in the index.
                         * 
                         * Note: In this case we consider the prior key in the
                         * index partition. If that key is for the same file
                         * version then we use the successor of the block
                         * identifier found in that key.
                         */

                        log.debug("Insertion point is after all entries in the index partition: id="
                                        + id + ", version=" + version);

                    } else {

                        /*
                         * Insertion point is at the toKey.
                         * 
                         * Note: Since the probe key is beyond the last block
                         * for the file version we adjust the toIndex so that we
                         * consider the prior key.
                         */

                        log.debug("Insertion point is at the toKey: id=" + id
                                + ", version=" + version);

                    }

                    /*
                     * Adjust to consider the key before the insertion point.
                     */

                    toIndex--;
                    
                    /*
                     * Look at the key at the computed index. If it is a key for
                     * this file version then we use the successor of the given
                     * block identifier. Otherwise we are writing a new file
                     * version and the block identifier will be zero (0).
                     */
                    
                    log.debug("adjusted toIndex="+toIndex+", entryCount="+entryCount);
                    
                    // the key at that index.
                    final byte[] key = tmp.keyAt(toIndex);

                    assert key != null : "Expecting entry: id=" + id
                            + ", version=" + version + ", toIndex=" + toIndex;

                    block = getNextBlockFromPriorKey(keyBuilder, key);
                    
                }

                log.info("Will write " + len + " bytes on id=" + id
                        + ", version=" + version + ", block#=" + block);
                
            }

            {

                /*
                 * write the block on the journal obtaining the address at which
                 * it was written - use 0L for the address of an empty block.
                 */
                final long addr = len == 0 ? 0L : journal.write(ByteBuffer
                        .wrap(b, off, len));

                // form the key for the index entry for this block.
                final byte[] key = keyBuilder.reset().appendText(id,
                        true/* unicode */, false/* successor */).append(
                        version).append(block).getKey();

                // record the address of the block in the index.
                {

                    final DataOutputBuffer out = new DataOutputBuffer(
                            Bytes.SIZEOF_LONG);

                    // encode the value for the entry.
                    out.reset().putLong(addr);

                    final byte[] val = out.toByteArray();

                    // insert the entry into the index.
                    ndx.insert(key, val);

                }

                log.info("Wrote " + len + " bytes : id=" + id + ", version="
                        + version + ", block#=" + block + " @ addr"
                        + journal.toString(addr));

            }

            // the block identifier.
            return block;

        }

        /**
         * Decode the block identifier in the key and return the block
         * identifier plus one, which is the block identifier to be used for the
         * atomic append operation. If the key does NOT encode the same file +
         * version then no blocks exist for that file version and the method
         * returns zero (0L) as the block identifer to be used.
         * 
         * @param keyBuilder
         *            The key builder.
         * @param key
         *            The key - either from the index partition or in some cases
         *            from the leftSeparator of the index partition metadata.
         *            <p>
         *            Note that the leftSeparator MAY be an empty byte[] (e.g.,
         *            for the 1st index partition in the key order) and MIGHT
         *            NOT include the block identifier (the block identifier is
         *            only included when it is necessary to split a file across
         *            index partitions). When the block identifier is omitted
         *            from the key and the key encodes the same file and version
         *            we therefore use zero (0L) as the next block identifier
         *            since we will be appending the first block to the file
         *            version.
         * 
         * @return The block identifier that will be used by the atomic append
         *         operation.
         */
        protected long getNextBlockFromPriorKey(IKeyBuilder keyBuilder,
                byte[] key) {

            // encode just the file id and the version.
            final byte[] prefix = keyBuilder.reset().appendText(id,
                    true/* unicode */, false/* successor */).append(version)
                    .getKey();

            if (DEBUG)
                log.debug("Comparing\nkey   :" + Arrays.toString(key)
                        + "\nprefix:" + Arrays.toString(prefix));

            /*
             * Test the encoded file id and version against the encoded file id
             * and version in the recovered key. If they compare equals (for the
             * length of the key that we just built) then they encode the same
             * file id and version.
             * 
             * (I.e., if true, then the key is from a block entry for this
             * version of this file).
             */

            if (key.length >= prefix.length) {

                final int cmp = BytesUtil.compareBytesWithLenAndOffset(0,
                        prefix.length, prefix, 0, prefix.length, key);

                log.debug("Comparing " + prefix.length + " byte prefix with "
                        + key.length + " byte key: cmp=" + cmp);

                if (cmp == 0) {

                    /*
                     * The key at the computed toIndex is the same file version.
                     */
                    if (prefix.length + Bytes.SIZEOF_LONG == key.length) {
                        
                        /*
                         * The given key includes a block identifier so we
                         * extract it.
                         * 
                         * Note: When the given key is a leftSeparator for an
                         * index partition AND the file version is not split
                         * across the index partition then the block identifer
                         * MAY be omitted from the leftSeparator. In this case
                         * the block identifier will be zero since there are no
                         * blocks yet for that file version.
                         */

                        // last block identifier assigned for this file + 1.
                        final long block = KeyBuilder.decodeLong(key,
                                key.length - Bytes.SIZEOF_LONG) + 1;

                        if (block > MAX_BLOCK) {

                            throw new RuntimeException(
                                    "File version has maximum #of blocks: id="
                                            + id + ", version=" + version);

                        }

                        log.info("Appending to existing file version: id=" + id
                                + ", version=" + version + ", block=" + block);

                        return block;

                    } else {
                        
                        /*
                         * This case arises when the leftSeparator encodes the
                         * file version but does not include a block identifier.
                         */
                        
                        log.info("Key is for same file version but does not contain block identifier.");
                        
                    }
                    
                } else {
                    
                    /*
                     * Since the key does not compare as equal for the full
                     * length of the prefix it can not encode the same file
                     * version.
                     */
                    
                    log.debug("Key does not compare as equal for length of prefix.");
                    
                }

            } else {
                
                /*
                 * Since the key is shorter than the prefix it can not be for
                 * the same file version.
                 */
                
                log.debug("Key is shorter than prefix.");
                
            }

            /*
             * The key at computed toIndex is a different file version so we are
             * starting a new file version at block := 0.
             */

            log.info("Appending to new file version: id=" + id + ", version="
                    + version + ", block=" + 0L);

            return 0L;

        }

        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            id = in.readUTF();

            version = in.readInt();

            off = 0; // Note: offset always zero when de-serialized.

            len = in.readInt();

            b = new byte[len];

            in.readFully(b);

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeUTF(id);

            out.writeInt(version);

            /*
             * Note: offset not written when serialized and always zero when
             * de-serialized.
             */
            
            out.writeInt(len); /* length */
            
            out.write(b, off, len); /* data */
            
        }
        
    }

    /**
     * Atomic write of a single block for a file version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AtomicWriteProc implements IIndexProcedure,
            Externalizable {

        private static final long serialVersionUID = 4982851251684333327L;

        protected static transient Logger log = Logger
                .getLogger(AtomicWriteProc.class);

        /**
         * True iff the {@link #log} level is INFO or less.
         */
        final public static transient boolean INFO = log.getEffectiveLevel()
                .toInt() <= Level.INFO.toInt();

        /**
         * True iff the {@link #log} level is DEBUG or less.
         */
        final public static transient boolean DEBUG = log.getEffectiveLevel()
                .toInt() <= Level.DEBUG.toInt();

        private String id;
        private int version;
        private long block;
        private int off;
        private int len;
        private byte[] b;
        
        /**
         * 
         * @param id
         *            The file identifier.
         * @param version
         *            The file version.
         * @param block
         *            The block identifier.
         * @param b
         *            The buffer containing the data to be written.
         * @param off
         *            The offset in the buffer of the first byte to be written.
         * @param len
         *            The #of bytes to be written.
         */
        public AtomicWriteProc(String id, int version, long block, byte[] b, int off, int len) {

            assert id != null && id.length() > 0;
            assert version >= 0;
            assert block >= 0 && block <= MAX_BLOCK;
            assert b != null;
            assert off >= 0 : "off="+off;
            assert len >= 0 && off + len <= b.length;
            assert len <= BLOCK_SIZE : "len="+len+" exceeds blockSize="+BLOCK_SIZE;

            this.id = id;
            this.version = version;
            this.block = block;
            this.off = off;
            this.len = len;
            this.b = b;

        }
        
        /**
         * This procedure runs on the unisolated index. The raw data is written
         * directly onto the {@link Journal} and the index is added/updated
         * using the given file, version and block and the address of the
         * block's data on the {@link Journal}.
         * 
         * @return A {@link Boolean} whose value is <code>true</code> iff the
         *         block was overwritten.
         */
        public Object apply(IIndex ndx) {

            // tunnel through to the backing journal.
            final AbstractJournal journal = (AbstractJournal)((AbstractBTree)ndx).getStore();
            
            // obtain the thread-local key builder for that journal.
            final IKeyBuilder keyBuilder = journal.getKeyBuilder();

            /*
             * Write the block on the journal, obtaining the address at which it
             * was writte - use 0L as the addrress for an empty block.
             */
            final long addr = len == 0 ? 0L : journal.write(ByteBuffer.wrap(b,
                    off, len));

            // form the key for the index entry for this block.
            final byte[] key = keyBuilder.reset().appendText(id,
                    true/* unicode */, false/* successor */).append(version)
                    .append(block).getKey();

            // record the address of the block in the index.
            final boolean overwrite;
            {

                final DataOutputBuffer out = new DataOutputBuffer(
                        Bytes.SIZEOF_LONG);

                // encode the value for the entry.
                out.reset().putLong(addr);

                final byte[] val = out.toByteArray();

                // insert the entry into the index.
                overwrite = ndx.insert(key, val) != null;

            }

            log.info("Wrote " + len + " bytes : id=" + id + ", version="
                    + version + ", block#=" + block + " @ addr"
                    + journal.toString(addr) + ", overwrite=" + overwrite);

            return Boolean.valueOf(overwrite);

        }
        
        public void readExternal(ObjectInput in) throws IOException,
                ClassNotFoundException {

            id = in.readUTF();

            version = in.readInt();

            block = in.readLong();

            off = 0; // Note: offset always zero when de-serialized.

            len = in.readInt();

            b = new byte[len];

            in.readFully(b);

        }

        public void writeExternal(ObjectOutput out) throws IOException {

            out.writeUTF(id);

            out.writeInt(version);

            out.writeLong(block);

            /*
             * Note: offset not written when serialized and always zero when
             * de-serialized.
             */
            
            out.writeInt(len); /* length */
            
            out.write(b, off, len); /* data */
            
        }
        
    }

    /**
     * Returns an iterator that visits all block identifiers for the file
     * version in sequence.
     * <p>
     * Note: This may be used to efficiently distribute blocks among a
     * population of clients, e.g., in a map/reduce paradigm.
     * 
     * @todo consider writing blocks out of line for the {@link IndexSegment}s
     *       as well so that the block identifier key scan will move less data
     *       (not having to read the blocks in with the leaves).
     */
    public Iterator<Long> blocks(String id,int version) {
        
        final byte[] fromKey;
        final byte[] toKey;
        
        synchronized(keyBuilder) {
            
            fromKey = keyBuilder.reset().appendText(id, true/* unicode */,
                    false/* successor */).append(version).getKey();

            toKey = keyBuilder.reset()
                    .appendText(id, true/* unicode */, false/* successor */)
                    .append(version+1).getKey();
            
        }

        // just the keys.
        final int flags = IRangeQuery.KEYS;
        
        // visits the keys for the file version in block order.
        IEntryIterator itr = dataIndex.rangeIterator(fromKey, toKey,
                0/* capacity */, flags, null/* filter */);

        // resolve keys to block identifiers.
        return new BlockIdentifierIterator( id, version, itr );
        
    }
    
    /**
     * Extracts the block identifier from the key.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class BlockIdentifierIterator implements Iterator<Long> {
        
        final private String id;
        final private int version;
        final private IEntryIterator src;

        public String getId() {
            
            return id;
            
        }
        
        public int getVersion() {
            
            return version;
            
        }
        
        public BlockIdentifierIterator(String id, int version, IEntryIterator src) {
        
            if (id == null)
                throw new IllegalArgumentException();
            if (src == null)
                throw new IllegalArgumentException();
            
            this.id = id;
            this.version = version;
            this.src = src;
            
        }

        public boolean hasNext() {

            return src.hasNext();
            
        }

        public Long next() {

            src.next();
            
            byte[] key = src.getKey();
            
            long block = KeyBuilder.decodeLong(key, key.length
                    - Bytes.SIZEOF_LONG);

            return block;
            
        }

        /**
         * Removes the last visited block for the file version.
         */
        public void remove() {

            src.remove();
            
        }
        
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
     * @todo this could be made much more efficient by sending the copy
     *       operation to each index partition in turn. that would avoid having
     *       to copy the data first to the client and thence to the target index
     *       partition.
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
     *            contains more than {@link #BLOCK_SIZE} bytes it will be broken
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
     *             if <i>len</i> is greater than {@link #BLOCK_SIZE}.
     * 
     * @todo return the data for the old block instead in the case of an
     *       overwrite?
     * 
     * @todo unit tests of [off,len].
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
        if(len>BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        /*
         * Figure out which index partition will get the write.
         */
        
        final MetadataIndex mdi = fed.getMetadataIndex(DATA_NAME);

        final PartitionMetadata pmd;
        
        synchronized( keyBuilder ) {
            
            // the key for the {file,version,block}
            final byte[] nextKey = keyBuilder.reset().appendText(id,
                    true/* unicode */, false/* successor */).append(version)
                    .append(block).getKey(); 
            
            // the partition spanning that key.
            pmd = mdi.find(nextKey);
            
            log.info("Write for id=" + id + ", version=" + version + ", block="
                    + block + " will go into partition: " + pmd);
            
        }
        
        /*
         * Lookup the data service for that index partition.
         */
        
        final IDataService dataService;
        {
            
            final UUID serviceUUID = pmd.getDataServices()[0];

            dataService = fed.getClient().getDataService(serviceUUID);
            
        }
        
        /*
         * Submit the atomic write operation to that data service.
         */
        
        try {
            
            final String name = DataService.getIndexPartitionName(DATA_NAME, pmd
                    .getPartitionId());

            // construct the atomic write operation.
            final IIndexProcedure proc = new AtomicWriteProc(id, version,
                    block, b, off, len);

            log.info("Submitting write operation to data service: id=" + id
                    + ", version=" + version + ", len=" + len);

            boolean overwrite = (Boolean) dataService.submit(
                    IBigdataFederation.UNISOLATED, name, proc);

            return overwrite;
            
        } catch (Exception ex) {
        
            throw new RuntimeException("Atomic write failed: id=" + id
                    + ", version=" + version, ex);
            
        }
        
    }

    /**
     * Atomic delete of the first block of the file version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The version identifier.
     * 
     * @return <code>true</code> iff a block was deleted.
     * 
     * @todo this implementation is not truely atomic. It uses one operation to
     *       locate the head block identifer and another to delete it. The
     *       proposed design pattern for block-oriented queues presumes a single
     *       master reading on the head of the queue so this should be
     *       sufficient.
     *       <p>
     *       A truely atomic head delete could be created using logic similar to
     *       the atomic append.
     */
    public boolean deleteHead(String id, int version) {
        
        Iterator<Long> itr = blocks(id, version);

        if (!itr.hasNext())
            return false;

        long block = itr.next();

        return deleteBlock(id, version, block);
        
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
     * 
     * @todo write tests.
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

        final byte[] key;
        
        synchronized (keyBuilder) {

            key = keyBuilder.reset()
                    .appendText(id, true/* unicode */, false/* successor */)
                    .append(version).append(block).getKey();

        }
        
        /*
         * Note: When the block is on the journal then the return value is just
         * the address of that block on the journal (8 bytes).
         * 
         * @todo Depending on how overflow onto the IndexSegment is handled this
         * sends MIGHT back the entire deleted block. For example, if the block
         * is inline with the leaf in the index segment then the entire block's
         * data will be sent back. If this is the case, then modify this code to
         * submit an index procedure that performs the delete and sends back
         * only a boolean.
         */
        
        boolean deleted = getDataIndex().remove(key) != null;
        
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
        final byte[] fromKey;
        final byte[] toKey;

        synchronized (keyBuilder) {

            fromKey = keyBuilder.reset().appendText(id, true/* unicode */,
                    false/* successor */).append(version).append(0L)
                    .getKey();

            toKey = keyBuilder.reset()
                    .appendText(id, true/* unicode */, false/* successor */)
                    .append(version).append(Long.MAX_VALUE).getKey();
            
        }

        /*
         * Resolve the requested block : keys and data.
         */
        final IEntryIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

        if (!itr.hasNext()) {

            log.info("id=" + id + ", version=" + version + " : no blocks");

            return null;

        }

        // the block's data.
        final byte[] data = (byte[]) itr.next();

        final byte[] key = itr.getKey();
        
        // decode the block identifier.
        final long block = KeyBuilder.decodeLong(key, key.length
                - Bytes.SIZEOF_LONG);  
            
        log.info("id=" + id + ", version=" + version + " : " + data.length
                + " bytes read from block#" + block);

        return data;
        
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
        final byte[] fromKey;
        final byte[] toKey;

        synchronized (keyBuilder) {

            fromKey = keyBuilder.reset().appendText(id, true/* unicode */,
                    false/* successor */).append(version).append(block)
                    .getKey();

            toKey = keyBuilder.reset()
                    .appendText(id, true/* unicode */, false/* successor */)
                    .append(version).append(block+1).getKey();
            
        }

        /*
         * Resolve the requested block : keys and data.
         */
        final IEntryIterator itr = getDataIndex()
                .rangeIterator(fromKey, toKey, 1/* capacity */,
                        IRangeQuery.KEYS | IRangeQuery.VALS, null/* filter */);

        if (!itr.hasNext()) {

            log.info("id=" + id + ", version=" + version + ", block=" + block
                    + " : does not exist");

            return null;

        }

        final byte[] data = (byte[]) itr.next();

        log.info("id=" + id + ", version=" + version + ", block=" + block
                + " : " + data.length + " bytes read");

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
     *            The #of bytes to be written in [0:{@link #BLOCK_SIZE}].
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
     *             if <i>len</i> is greater than {@link #BLOCK_SIZE}.
     *             
     * @todo unit tests of [off,len].
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
        if (len > BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }
        
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
        
        final MetadataIndex mdi = fed.getMetadataIndex(DATA_NAME);

        final PartitionMetadata pmd;
        
        synchronized( keyBuilder ) {
            
            // the last possible key for this file
            final byte[] nextKey = keyBuilder.reset().appendText(id,
                    true/* unicode */, true/* successor */).append(version)
                    .append(-1L).getKey(); 
            
            // the partition spanning that key.
            pmd = mdi.find(nextKey);
            
            log.info("Appends for id=" + id + ", version=" + version
                    + " will go into partition: " + pmd);
            
        }
        
        /*
         * Lookup the data service for that index partition.
         */
        
        final IDataService dataService;
        {
            
            final UUID serviceUUID = pmd.getDataServices()[0];

            dataService = fed.getClient().getDataService(serviceUUID);
            
        }
        
        /*
         * Submit the atomic append operation to that data service.
         */
        
        try {
            
            final String name = DataService.getIndexPartitionName(DATA_NAME, pmd
                    .getPartitionId());

            // construct the atomic append operation.
            final IIndexProcedure proc = new AtomicAppendProc(id, version, b, off,
                    len);

            log.info("Submitting append operation to data service: id=" + id
                    + ", version=" + version + ", len=" + len);

            Long block = (Long) dataService.submit(
                    IBigdataFederation.UNISOLATED, name, proc);

            return block;
            
        } catch (Exception ex) {
        
            throw new RuntimeException("Atomic append failed: id=" + id
                    + ", version=" + version, ex);
            
        }
        
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
     
        final byte[] fromKey;
        final byte[] toKey;
        
        synchronized(keyBuilder) {
            
            fromKey = keyBuilder.reset().appendText(id, true/* unicode */,
                    false/* successor */).append(version).getKey();

            toKey = keyBuilder.reset()
                    .appendText(id, true/* unicode */, false/* successor */)
                    .append(version+1).getKey();
            
        }
        
        // @todo modify range count to return [long].
        long nblocks = dataIndex.rangeCount(fromKey, toKey);
        
        log.info("id="+id+", version="+version+", nblocks="+nblocks);
        
        return nblocks;
        
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
     *         version. An empty input stream MAY be returned since empty blocks
     *         are allowed.
     */
    public FileVersionInputStream inputStream(String id,int version) {

        /*
         * Range count the file and version on the federation - this is the
         * number of blocks of data for that file and version as of the start of
         * this read operation. If the result is zero then there are no index
         * partitions which span that file and version and we return null.
         */

        final long nblocks = getBlockCount(id, version);

        if (nblocks == 0) {

            log.info("No data: id=" + id + ", version=" + version);
            
            return null;
            
        }
        
        /*
         * Return an input stream that will progress through a range scan of the
         * blocks for that file and version.
         */
        
        final byte[] fromKey;
        final byte[] toKey;
        
        synchronized(keyBuilder) {
            
            fromKey = keyBuilder.reset().appendText(id, true/* unicode */,
                    false/* successor */).append(version).getKey();

            toKey = keyBuilder.reset()
                    .appendText(id, true/* unicode */, false/* successor */)
                    .append(version+1).getKey();
            
        }

        /*
         * Note: since the block size is so large this iterator limits its
         * capacity to avoid an undue burden on the heap. E.g., we do NOT want
         * it to buffer and transmit 100,000 64k blocks at a time! In practice,
         * the buffering is per data service, but that could still be hundreds
         * of megabytes!
         * 
         * Note: The capacity is essentially the #of blocks to transfer at a
         * time. Each block is 64k, so a capacity of 10 is 640k per transfer. Of
         * course, no more blocks will be transferred that exist on the data
         * service for the identified file version.
         */
        final int capacity = 20;
        
        // both keys and values.
        final int flags = IRangeQuery.KEYS | IRangeQuery.VALS;
        
        IEntryIterator itr = dataIndex.rangeIterator(fromKey, toKey, capacity,
                flags, null/*filter*/);
        
        return new FileVersionInputStream(id, version, itr);
        
    }

    /**
     * Return an output stream that will <em>append</em> on the file version.
     * Bytes written on the output stream will be buffered until they are full
     * blocks and then written on the file version using an atomic append.
     * <p>
     * Note: Map/Reduce processing of files MAY be facilitated greatly by
     * ensuring that "records" never cross a block boundary - this means that
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

    /**
     * Class buffers up to a block of data at a time and flushes blocks using an
     * atomic append operation on the identifier file version.
     * 
     * @todo this would benefit from asynchronous write-behind of the last block
     *       so that caller's do not wait for the RPC that writes the block onto
     *       the data index. use a blocking queue of buffers to be written so
     *       that the caller can not get far ahead of the database. a queue
     *       capacity of 1 or 2 should be sufficient.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class FileVersionOutputStream extends OutputStream {

        protected final BigdataRepository repo;
        protected final String id;
        protected final int version;
        
        /**
         * The file identifier.
         */
        public String getId() {
            
            return id;
            
        }

        /**
         * The file version identifer.
         */
        public int getVersion() {

            return version;
            
        }

        /**
         * The buffer in which the current block is being accumulated.
         */
        private byte[] buffer = new byte[BigdataRepository.BLOCK_SIZE];

        /**
         * The index of the next byte in {@link #buffer} on which a byte would be
         * written.
         */
        private int len = 0;

        /**
         * #of bytes written onto this output stream.
         */
        private long nwritten;
        
        /**
         * #of bytes written onto this output stream.
         * 
         * @todo handle overflow of long - leave counter at {@link Long#MAX_VALUE}.
         */
        public long getByteCount() {
            
            return nwritten;
            
        }

        /**
         * #of blocks written onto the file version.
         */
        private long nblocks;
        
        /**
         * #of blocks written onto the file version.
         */
        public long getBlockCount() {
           
            return nblocks;
            
        }
        
        /**
         * Create an output stream that will atomically append blocks of data to
         * the specified file version.
         * 
         * @param id
         *            The file identifier.
         * @param version
         *            The version identifier.
         */
        public FileVersionOutputStream(BigdataRepository repo, String id, int version) {
            
            if (repo == null)
                throw new IllegalArgumentException();
            if (id == null)
                throw new IllegalArgumentException();
            
            this.repo = repo;
            
            this.id = id;
            
            this.version = version;
            
        }

        /**
         * Buffers the byte. If the buffer would overflow then it is flushed.
         * 
         * @throws IOException
         */
        public void write(int b) throws IOException {

            if (len == buffer.length) {

                // buffer would overflow.
                
                flush();
                
            }
            
            buffer[len++] = (byte) (b & 0xff);
            
            nwritten++;
            
        }

        /**
         * If there is data data accumulated in the buffer then it is written on
         * the file version using an atomic append (empty buffers are NOT
         * flushed).
         * 
         * @throws IOException
         */
        public void flush() throws IOException {
            
            if (len > 0) {

                log.info("Flushing buffer: id="+id+", version="+version+", len="+len);
                
                repo.appendBlock(id, version, buffer, 0, len);

                len = 0;
                
                nblocks++;
                
            }
            
        }
        
        /**
         * Flushes the buffer.
         * 
         * @throws IOException
         */
        public void close() throws IOException {
           
            flush();
            
        }

        /**
         * Consumes the input stream, writing blocks onto the file version. The
         * output stream is NOT flushed.
         * 
         * @param is
         *            The input stream (closed iff it is fully consumed).
         * 
         * @return The #of bytes copied from the input stream.
         * 
         * @throws IOException
         */
        public long copyStream(InputStream is) throws IOException {

            long ncopied = 0L;

            while (true) {

                if (this.len == buffer.length) {

                    // flush if the buffer would overflow.
                    
                    flush();
                    
                }
                
                // next byte to write in the buffer.
                final int off = this.len;

                // #of bytes remaining in the buffer.
                final int remainder = this.buffer.length - off;

                // read into the buffer.
                final int nread = is.read(buffer, off, remainder);

                if (nread == -1) {

                    // the input stream is exhausted.
                    
                    log.info("Copied " + ncopied + " bytes: id=" + id
                            + ", version=" + version);

                    try {

                        is.close();
                        
                    } catch (IOException ex) {
                        
                        log.warn("Problem closing input stream: id=" + id
                                + ", version=" + version, ex);
                        
                    }

                    return ncopied;

                }

                // update the index of the next byte to write in the buffer.
                this.len = off + nread;

                // update #of bytes copied.
                ncopied += nread;

                // update #of bytes written on this output stream.
                nwritten += nread;

            }

        }
        
    }
    
    /**
     * Reads from blocks visited by a range scan for a file and version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected static class FileVersionInputStream extends InputStream {

        protected final String id;
        protected final int version;
        private final IEntryIterator src;

        /**
         * The current block# whose data are being read.
         */
        private long block;
        
        /**
         * A buffer holding the current block's data. This is initially filled
         * from the first block by the ctor. When no more data is available it
         * is set to <code>null</code> to indicate that the input stream has
         * been exhausted.
         */
        private byte[] b;
        
        /**
         * The next byte to be returned from the current block's data.
         */
        private int off;
        
        /**
         * The #of bytes remaining in the current block's data.
         */
        private int len;
        
        /**
         * The file identifier.
         */
        public String getId() {
            
            return id;
            
        }

        /**
         * The file version identifer.
         */
        public int getVersion() {

            return version;
            
        }
        
        /**
         * The current block identifier.
         */
        public long getBlock() {

            return block;
            
        }
        
        public FileVersionInputStream(String id, int version, IEntryIterator src) {
            
            this.id = id;
            
            this.version = version;
            
            this.src = src;
            
            // read the first block of data.
            nextBlock();
            
        }
        
        /**
         * Reads the next block of data from the iterator and sets it on the
         * internal buffer. If the iterator is exhausted then the internal
         * buffer is set to <code>null</code>.
         * 
         * @return true iff another block of data was read.
         */
        private boolean nextBlock() {
            
            assert b == null || off == len;
            
            if(!src.hasNext()) {

                log.info("No more blocks: id="+id+", version="+version);
                
                b = null;
                
                off = 0;
                
                len = 0;
                
                return false;
                
            }
            
            // the next block's data FIXME offer a no-copy variant of next()!!! It's 64k per copy!
            b = (byte[]) src.next();
            
            off = 0;
            
            len = b.length;
            
            assert b != null;
//            assert b.length > 0; // zero length blocks are allowed.
            
            // the key buffer.
            final IByteArrayBuffer kbuf = src.getTuple().getKeyBuffer();
            
            // the backing byte[].
            final byte[] key = kbuf.array();
            
            // decode the block identifier from the key.
            block = KeyBuilder.decodeLong(key, kbuf.position()
                    - Bytes.SIZEOF_LONG);
            
            log.info("Read "+b.length+" bytes: id="+id+", version="+version+", block="+block);
            
            return true;
            
        }
        
        public int read() throws IOException {

            if (b == null) {

                // nothing left to read.

                return -1;

            }
            
            if(off == len) {
                
                if (!nextBlock()) {
                    
                    // no more blocks so nothing left to read.
                    
                    return -1;
                    
                }
                
            }
            
            // the next byte.
            int v = (0xff & b[off++]);

            return v;
            
        }
        
        /**
         * Overriden for greater efficiency.
         */
        public int read(byte[] b,int off, int len) throws IOException {

            if (b == null) {

                // nothing left to read.

                return -1;

            }
            
            if(this.off == this.len) {
                
                if (!nextBlock()) {
                    
                    // no more blocks so nothing left to read.
                    
                    return -1;
                    
                }
                
            }
            
            /*
             * Copy everything in our internal buffer up to the #of bytes
             * remaining in the caller's buffer.
             */
            
            final int n = Math.min(this.len, len); 
            
            System.arraycopy(this.b, this.off, b, off, n);

            this.off += n;

            this.len -= n;
            
            return n;
            
        }
        
    }
 
    /**
     * This resolves the address of the block to the block on the journal.
     * <p>
     * Note: This filter can ONLY be applied on the data service side since you
     * need to already have the journal reference on hand.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class BlockResolver extends EntryFilter {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        private final IRawStore store;

        /**
         * Used to decode the address from the visited values.
         */
        private final DataInputBuffer in = new DataInputBuffer(new byte[]{});
       
        private static transient final byte[] EMPTY_BLOCK = new byte[]{};
        
        /**
         * 
         * @param store
         *            The store against which the block's address will be
         *            resolved.
         */
        public BlockResolver(IRawStore store) {
            
            this.store = store;
            
        }
        
        public Object resolve(Object val) {
            
            final byte[] b = (byte[])val;
            
            assert b != null;
            assert b.length == 8 :
                "Expecting 8 bytes not "+b.length;// +" : "+Arrays.toString(b);
            
            in.setBuffer(b);
            
            final long addr;
            
            try {
            
                addr = in.readLong();
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
            if (addr == 0L) {

                /*
                 * Note: empty blocks are allowed and are recorded with 0L as
                 * their address.
                 */
                
                return EMPTY_BLOCK;

            }
            
            /*
             * @todo This is a great example of when passing in the buffer makes
             * sense. The code here jumps through some hoops to avoid double
             * copying the data.  However, when the buffer is readOnly we need
             * to copy it anyway - and it generally is to prevent modifications
             * to the data on the journal!
             * 
             * Note: hasArray() will not return true if the buffer is marked
             * [readOnly].
             */
            
            log.info("Reading block from addr="+store.toString(addr));
            
            final ByteBuffer buffer = store.read(addr);

            if(buffer.hasArray() && buffer.arrayOffset()==0) {

                return buffer.array();
                
            }
            
            // log a notice since this is so annoying.
            log.warn("Cloning data in block: len="+buffer.limit());
            
            // allocate array into which we will copy the data.
            byte[] dst = new byte[buffer.limit()];
            
            // copy the data.
            buffer.get(dst);
            
            return dst;
            
        }
        
    }
    
    /**
     * Class is specialized to store blocks on the raw journal with only the
     * address of the block in the entry value. However, when the entry value is
     * requested the block is de-referenced (read off the journal) and returned
     * instead.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     * 
     * FIXME This optimizes the performance of the mutable {@link BTree} by
     * writing the blocks onto raw records on the journal and recording the
     * offset of the blocks in the {@link BTree}. Therefore, on overflow, the
     * export of the {@link BTree} MUST be overriden so as to serialize the
     * blocks themselves rather than the offsets in the {@link IndexSegment}
     * (alternatively, we could embed the blocks in the
     * {@link IndexSegmentFileStore}). This is designed to maximize the
     * performance of both the mutable {@link BTree} and the read-only
     * {@link IndexSegment}.
     * 
     * @see AtomicAppendProc
     * @see AtomicWriteProc
     */
    public static class FileDataBTreePartition extends UnisolatedBTreePartition {

        public FileDataBTreePartition(IRawStore store, int branchingFactor,
                UUID indexUUID, PartitionMetadataWithSeparatorKeys pmd) {

            super(store, branchingFactor, indexUUID, pmd);

        }

        /**
         * @param store
         * @param metadata
         */
        public FileDataBTreePartition(IRawStore store, BTreeMetadata metadata) {

            super(store, metadata);

        }

        /**
         * Overriden to automatically resolve addresses of blocks to the block's
         * data which is read from the backing store.
         */
        public IEntryIterator rangeIterator(byte[] fromKey, byte[] toKey,
                int capacity, int flags, IEntryFilter filter) {

            final IEntryFilter f = new BlockResolver(getStore());

            if (filter != null) {

                f.add(filter);

            }

            return super.rangeIterator(fromKey, toKey, capacity, flags, f);

        }
        
    }

    /**
     * Creates an {@link FileDataBTreePartition} instance.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class FileDataBTreePartitionConstructor extends UnisolatedBTreePartitionConstructor {

        /**
         * 
         */
        private static final long serialVersionUID = -8590231817525371488L;

        /**
         * De-serialization constructor.
         */
        public FileDataBTreePartitionConstructor() {

            super();
            
        }

        /**
         * 
         * @param branchingFactor
         *            The branching factor.
         */
        public FileDataBTreePartitionConstructor(int branchingFactor) {

            super( branchingFactor );
            
        }
        
        public BTree newInstance(IRawStore store, UUID indexUUID,IPartitionMetadata pmd) {

            log.info("Creating file data partition#"+pmd.getPartitionId());
            
            return new FileDataBTreePartition(store, branchingFactor, indexUUID,
                    (PartitionMetadataWithSeparatorKeys) pmd);
            
        }

    }
    
}
