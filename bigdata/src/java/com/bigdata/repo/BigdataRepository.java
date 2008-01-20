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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

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
 * therefore apply only to the individual file metadata or file data operations.
 * In particular, file metadata read and write are atomic and file append is
 * atomic. File read will never read inconsistent data. Files once created are
 * append only.
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
 * file.
 * 
 * @todo describe the relationship to the media redundency system. unless the
 *       replication factor is part of the key all files in a file system will
 *       have the same replication factor (the journals are mirrored, leading to
 *       mirrored index partitions). Is there any way to achieve per file
 *       replication quotas? Perhaps just choose the partition of the file
 *       system, e.g., <code>/highly-available</code> might be given on
 *       guarentee while other prefixes have a different guarentee - that seems
 *       simple enough. The availability can be managed by changing what data is
 *       on an index partition. If availability prefixes are created then we
 *       could juggle the separator keys to correspond to exact partition
 *       boundaries so that we never over-replicate a file because it's lumped
 *       with a highly available partition.
 * 
 * @todo multiple file systems can be easily created but that seems to just
 *       create problems since the file identifers are no longer sufficient to
 *       locate a file.
 * 
 * @todo provide scans for file versions, e.g., given a file, what versions
 *       exist.
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
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRepository implements ContentRepository {

    protected static Logger log = Logger.getLogger(BigdataRepository.class);
    
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
     * @todo add an explicit Last-Modified - this can be optional. We can not
     *       simply use the timestamp on the logical row in the metadata index
     *       since changes to metadata do not of necessity imply changes to the
     *       content itself and the application may need to decide when it will
     *       update the Last-Modified timestamp.
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
    
//    /**
//     * Break up the content into blocks and write them onto the data index.
//     * 
//     * @todo encapsulate this as a class using a pipe model so that someone can
//     *       write continuously on an {@link OutputStream} and have the data
//     *       transparently partitioned into blocks and those blocks written with
//     *       atomic appends. Provide a record aware and block boundary aware
//     *       extensible layer for the class so that applications can pad out to
//     *       fill a block when the goal is transparently split during m/r
//     *       processing.
//     */
//    public void writeContent(String id, int version, InputStream is) {
//
//        /*
//         * This constant determines the amount of data that will go into each
//         * atomic append invoked by this method.
//         */
//        final int BUFFER_SIZE = 64 * Bytes.kilobyte32; // @todo config.
//        
//        if (!(is instanceof ByteArrayInputStream)
////                && !(is instanceof BufferedInputStream)
//                ) {
//
//            /*
//             * @todo Try this with just the default buffer size. I think that
//             * the size of the input stream buffer will become less critical as
//             * the #of concurrent clients of this interface goes up. However we
//             * always want to use a large buffer to collect writes that will be
//             * sent across the data service interface in order to keep up the
//             * efficiency of the RPC.
//             */
//
//            is = new BufferedInputStream(is, BUFFER_SIZE);
//            
//        }
//
//        /*
//         * Fill the buffer repeatedly, doing an atomic append on the file
//         * each time the buffer becomes full.
//         */
//        
//        // @todo use a thread-local variable for this buffer?
//        final byte[] b = new byte[BUFFER_SIZE];
//
//        int off = 0; // offset of the 1st unwritten byte in the buffer.
//        int remaining = b.length; // #of unwritten bytes remaining in buf.
//        long nwritten = 0; // total #of bytes written.
//        int nblocks = 0; // total #of blocks (buffers) written.
//        
//        try {
//
//            while (true) {
//
//                int nread = is.read(b, off, remaining);
//
//                if (nread == -1)
//                    break;
//
//                off += nread;
//                remaining -= nread;
//                
//                if (remaining == 0) {
//
//                    /*
//                     * The buffer is full. Incremental write using atomic append
//                     * each time the buffer is filled to capacity.
//                     */
//                    
//                    atomicAppend( id, version, b, 0/*offset*/, off/*length*/ );
//
//                    // increment totals.
//                    nblocks++;
//                    nwritten += off;
//
//                    // reset within buffer counters.
//                    off = 0;
//                    remaining = b.length;
//                    
//                }
//                
//            }
//
//            if (off > 0) {
//
//                /*
//                 * Write anything remaining once the input stream is exhausted.
//                 */
//                
//                atomicAppend( id, version, b, 0/*offset*/, off/*length*/ );
//
//                // incremental totals.
//                nblocks++;
//                nwritten += off;
//
//            }
//
//            log.info("Wrote " + nwritten + " bytes on " + id + " in " + nblocks
//                    + " blocks");
//            
//        } catch (Throwable t) {
//
//            log.error("Problem writing on file: id=" + id, t);
//            
//            throw new RuntimeException( t );
//
//        } finally {
//
//            try {
//                
//                is.close();
//
//            } catch (IOException e) {
//                
//                log.warn("Problem closing input stream.", e);
//                
//            }
//
//        }
//        
//    }

    /**
     * Atomic append of one or more 64k blocks to a file.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class AtomicAppendProc implements IIndexProcedure,
            Externalizable {

        private static final long serialVersionUID = 1441331704737671258L;

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
         * This procedure runs on the unisolated index. The raw data is written
         * directly onto the {@link Journal} in a series of one or more
         * {@link BigdataRepository#BLOCK_SIZE} records (the final write may be
         * a partial record). Index entries are added for each block written
         * using a sequential block identifier based on locally available state.
         * <p>
         * Note: The caller MUST have correctly identified the data service on
         * which the tail of the file exists (or on which the head of the file
         * will be written).
         * <p>
         * The basic algorithm is:
         * <ol>
         * <li>Read and decode the key for the last block written for this
         * file. This may be either on the mutable BTree or on a read-only index
         * segment for the same index partition. In any case, the last written
         * block identifier is encoded at the tail of the key as a 64-bit
         * integer.</li>
         * <li>Repeat the next steps until done</li>
         * <ol>
         * <li> Compute the next block identifier (block := block + 1) </li>
         * <li> Write up to BLOCK_SIZE bytes on the journal from the buffer,
         * obtaining the long address of that record [addr]. </li>
         * <li> Insert an entry into the index (key := [id,block], val=addr).
         * </li>
         * <li> Repeat until the buffer has been consumed. </li>
         * </ol>
         * </ol>
         * 
         * @return The #of blocks written as an {@link Integer}.
         * 
         * FIXME This optimizes the performance of the mutable {@link BTree} by
         * writing the blocks onto raw records on the journal and recording the
         * offset of the blocks in the {@link BTree}. Therefore, on overflow,
         * the export of the {@link BTree} MUST be overriden so as to serialize
         * the blocks themselves rather than the offsets in the
         * {@link IndexSegment} (alternatively, we could embed the blocks in the
         * {@link IndexSegmentFileStore}). This is designed to maximize the
         * performance of both the mutable {@link BTree} and the read-only
         * {@link IndexSegment}.
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
                 * version. When no blocks for this file version have been
                 * written on this index partition then we start with block#0.
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
                        true/* unicode */, true/* successor */).append(
                        version).getKey();

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

                log.debug("insertionPoint="+toIndex); // @todo comment out.
                
                toIndex = -(toIndex+1); // convert to an index.

                // #of entries in the index.
                final int entryCount = ((AbstractBTree)ndx).getEntryCount();
                
                log.debug("toIndex="+toIndex+", entryCount="+entryCount);

                if (toIndex == 0) {

                    // insertion point is before all other entries in the index.
                    
                    log.info("Will write 1st block: id=" + id + ", version="
                            + version);
                    
                    block = 0;
                    
                } else if (toIndex == entryCount) {
                    
                    // insertion point is after all entries in the index.
                    
                    log.info("Will write 1st block: id=" + id + ", version="
                            + version);
                    
                    block = 0;
                    
                } else {
                    
                    // the key at that index.
                    final byte[] key = tmp.keyAt(toIndex);
                    
                    assert key != null : "Expecting entry: id=" + id
                            + ", version=" + version + ", toIndex=" + toIndex;

                    // encode just the file id and the version.
                    final byte[] prefix = keyBuilder.reset().appendText(id,
                            true/* unicode */, false/* successor */).append(
                            version).getKey();
                    
                    /*
                     * Test the encoded file id and version against the encoded
                     * file id and version in the recovered key. If they compare
                     * equals (for the length of the key that we just built)
                     * then they encode the same file id and version.
                     * 
                     * (I.e., if true, then the key is from a block entry for
                     * this version of this file).
                     */
                    if (BytesUtil.compareBytesWithLenAndOffset(0,
                            prefix.length, prefix, 0, prefix.length, key) != 0) {
                        
                        // last block identifier assigned for this file + 1.
                        block = KeyBuilder.decodeLong(key, key.length
                                - Bytes.SIZEOF_LONG) + 1;
                        
                        log.info("Appending to existing file: id=" + id
                                + ", version=" + version + ", block=" + block);
                        
                    } else {
                        
                        log.info("Will write 1st block for file: id=" + id
                                + ", version=" + version);
                        
                        block = 0;
                        
                    }
                    
                }

                log.info("Will write " + len + " bytes on id=" + id
                        + ", version=" + version + ", block#=" + block);
                
            }

            {
            
                // write the block on the journal - use 0L for an empty block.
                final long addr = len == 0 ? 0L : journal.write(ByteBuffer
                        .wrap(b, off, len));

                // form the key for the index entry for this block.
                final byte[] key = keyBuilder.reset()
                        .appendText(id, true/* unicode */, false/* successor */)
                        .append(version)
                        .append(block).getKey();
                
                // record the address of the block in the index.
                {
                
                    final DataOutputBuffer out = new DataOutputBuffer(Bytes.SIZEOF_LONG);
                    
                    // encode the value for the entry.
                    out.reset().putLong(addr);
                    
                    final byte[] val = out.toByteArray();

                    // insert the entry into the index.
                    ndx.insert(key, val);
                 
                    log.info("id=" + id + ", version=" + version
                            + ", wrote block=" + block + " @ addr"
                            + journal.toString(addr));
                    
                }
            
            }
            
            log.info("Wrote " + len + " bytes : id=" + id + ", version="
                    + version + ", block#=" + block);
            
            // #of bytes written.
            return Long.valueOf(len);
            
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

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
            
            out.writeInt(len); /*length*/
            
            out.write(b, off, len); /* data */
            
        }
        
    }

    /**
     * Atomic write of a block into an existing file and version.
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param block
     *            The block identifier.
     * @param b
     *            The buffer containing the bytes to be written. When the buffer
     *            contains more than {@link #BLOCK_SIZE} bytes it will be broken
     *            up into multiple blocks.
     * @param off
     *            The offset of the 1st byte to be written.
     * @param len
     *            The #of bytes to be written.
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
     * @return The #of bytes written.
     * 
     * @todo should writes beyond the end of the file be allowed? They will
     *       leave "holes", but that's ok, right? Can any block identifier be
     *       written in this manner whether or not it exists or is beyond the
     *       end of the file?
     */
    public int atomicWrite(String id, int version, long block, byte[] b, int off, int len) {

        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if(block<0L) {
            // Note: restriction implies 63-bit block identifier (no negative#s).
            throw new IllegalArgumentException();
        }
        if (b == null)
            throw new IllegalArgumentException();
        if (off < 0 || off > b.length)
            throw new IllegalArgumentException();
        if (len < 0 || off + len > b.length)
            throw new IllegalArgumentException();
        if(len>BLOCK_SIZE) {
            throw new IllegalArgumentException();
        }

        throw new UnsupportedOperationException();
        
    }
    
    /**
     * Atomic append of data onto the end of a file.
     * <p>
     * Note: Atomic append is intended for writing one or more
     * {@link #BLOCK_SIZE} blocks at a time onto a file. Do NOT use this method
     * to perform writes that are not even multiples of {@link #BLOCK_SIZE}
     * unless you know that you are writing either the entire content of the
     * file or its final block (partial writes of final blocks are allowed).
     * 
     * @param id
     *            The file identifier.
     * @param version
     *            The file version.
     * @param b
     *            The buffer containing the bytes to be written. When the buffer
     *            contains more than {@link #BLOCK_SIZE} bytes it will be broken
     *            up into multiple blocks.
     * @param off
     *            The offset of the 1st byte to be written.
     * @param len
     *            The #of bytes to be written.
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
     * @return #of bytes written.
     */
    public long atomicAppend(String id, int version, byte[] b, int off, int len) {
        
        if (id == null || id.length() == 0)
            throw new IllegalArgumentException();
        if (version < 0)
            throw new IllegalArgumentException();
        if (b == null)
            throw new IllegalArgumentException();
        if (off < 0 || off > b.length)
            throw new IllegalArgumentException();
        if (len < 0 || off + len > b.length)
            throw new IllegalArgumentException();
        if(len>BLOCK_SIZE) {
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
         * @todo test the edge case where there are no blocks for the file. the
         * same logic should produce the correct answer since the decision is
         * made based on the separator keys for the index partitions.
         * 
         * @todo test where there is only a single index partition and where
         * there are several.
         * 
         * @todo test where the file is first in index order, where it falls at
         * the start of a partition other than the first, and where it is the
         * last in the index order.
         * 
         * @todo write tests for this logic.
         * 
         * FIXME There is an edge case when a file has been deleted that spans
         * more than one index partition. In this case simply locating the index
         * partition within which we would find the last possible block for the
         * file is not going to produce the correct answer since we really need
         * the largest partition that actually has data for that file. When the
         * file is empty, this is the partition into which block#0 would go.
         * When a large file has been deleted there can be EMPTY index
         * partitions (containing only delete entries) until the next compacting
         * merge, so we need to locate the correct entry for the append
         * operation. This is, all in all, a mess. I think that the easier way
         * to deal with this is to have the ids in the data index be versioned.
         * When a file is deleted we add a metadata property indicating the next
         * file id version to be assigned. In this way we avoid this problem
         * completely.
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

            Long nwritten = (Long) dataService.submit(
                    IBigdataFederation.UNISOLATED, name, proc);

            return nwritten;
            
        } catch (Exception ex) {
        
            throw new RuntimeException("Atomic append failed: id=" + id
                    + ", version=" + version, ex);
            
        }
        
    }

    /**
     * Return the #of blocks in the file.
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
    
//    /**
//     * Estimate the #of bytes in the file. The estimate is computed as follows.
//     * The #of blocks in the file is discovered. The size of the last block in
//     * the file is discovered. The return value is
//     * <code>(nblocks-1) * BLOCK_SIZE + sizeof(lastBlock)</code> unless the
//     * #of bytes would overflow a {@link Long} in which case it is
//     * {@link Long#MAX_VALUE}.
//     * 
//     * @param id
//     *            The file identifier.
//     * @param version
//     *            The file version identifier.
//     * 
//     * @return The #of bytes in the file. If the value would overflow a
//     *         {@link Long} then the return value will be {@link Long#MAX_VALUE}.
//     * 
//     * @todo closely examine the logic here for fence posts with signed longs
//     *       and max bytes. problems will not show up until files are 128-256 TB
//     *       in size (2^27 or 2^48 blocks in length).
//     */
//    public long getLength(String id, int version) {
//        
//        long nblocks = getBlockCount(id, version);
//
//        if (nblocks == 0) {
//
//            log.info("No blocks: id=" + id + ", version=" + version);
//
//            return 0L;
//
//        }
//
//        if ((nblocks >>> 48) > 0) {
//
//            log.warn("Size in excess of 2^64 bytes: id=" + id + ", version="
//                    + version);
//
//            return Long.MAX_VALUE;
//
//        }
//
//        int lastBlockSize = getLastBlockSize(id, version);
//        
//        // @todo overflow is possible here as well.
//        long len = nblocks * BLOCK_SIZE + lastBlockSize;
//        
//        return len;
//        
//    }
    
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
     * Return an output stream that will write on the file version. Bytes
     * written on the output stream will be buffered until they are full blocks
     * and then written on the file version using an atomic append.
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
                atomicAppend(id, version, new byte[]{}, 0, 0);
                
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
         * the file version using an atomic append.
         * 
         * @throws IOException
         */
        public void flush() throws IOException {
            
            if (len > 0) {

                log.info("Flushing buffer: id="+id+", version="+version+", len="+len);
                
                repo.atomicAppend(id, version, buffer, 0, len);

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
         * 
         * @todo write tests at fence posts (copying zero, N, and BLOCK_SIZE
         *       bytes) and make sure that flush occurs only when the buffer is
         *       completely full.
         * 
         * @todo test multi block writes.
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
            
            final byte[] key = src.getKey(); // @todo use the key buffer (no copy)
            
            block = KeyBuilder.decodeLong(key,key.length-Bytes.SIZEOF_LONG);
            
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
            
//            // @todo remove - shows each invocation context.
//            log.info("Context",new RuntimeException("Context"));
            
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
             * FIXME This is a great example of when passing in the buffer makes
             * sense. The code here jumps through some hoops to avoid double
             * copying the data.
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
