/*
 * Created on Jan 17, 2008
 */
package com.bigdata.repo;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
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
 * implemented using the bigdata scale-out architecture.
 * <p>
 * The distributed file system uses two scale-out indices to support ACID
 * operations on file metadata and atomic file append. These ACID guarentees
 * arise from the use of unisolated operations on the respective indices and
 * therefore apply only to the individual file metadata or file data operations.
 * In particular, file metadata read and write are atomic and file append is
 * atomic. File read will never read inconsistent data. Files once created are
 * append only.
 * <p>
 * The content length of the file is not stored as file metadata. Instead it is
 * estimated by a range count of the index entries spanned by the file's data.
 * The exact file size may be readily determined when reading small files by the
 * expediency of sucking the entire file into a buffer. Streaming processing is
 * advised in all cases when handling large files, including when the file is to
 * be delivered via HTTP.
 * <p>
 * The metadata index uses a {@link SparseRowStore} design, similar to Google's
 * bigtable or Hadoop's HBase. Certain properties MUST be defined for each entry -
 * they are documented on the {@link MetadataSchema}. Applications are free to
 * define additional properties. Reads and writes of file metadata are always
 * atomic.
 * <p>
 * <p>
 * Each time a file is created a new version number is assigned. The data index
 * uses the {@link MetadataSchema#ID} as the first field in a compound key. The
 * second field is the {@link MetadataSchema#VERSION}. The remainder of the key
 * is a 64-bit block identifier. The block identifiers are strictly monotonic
 * (e.g., up one) and their sequence orders the blocks into the logical byte
 * order of the file.
 * <p>
 * All writes on a file are atomic appends. In general, applications atomic
 * appends should write 64k of data - less if the last block of the file is
 * being written. As an aid to local clients, method exists to consume an
 * {@link InputStream}, buffering data and performing atomic appends as the
 * buffer overflows. Distributed clients can also use atomic appends, but they
 * must provide themselves for an internal consistency within the blocks, e.g.,
 * by always padding out records to the next 64k boundary. In particular, this
 * makes it easy for a master to split a file across map clients and makes it
 * equally easy for distributed processes to aggregate results.
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
 * FIXME Modify to always logically pad out to {@link #BLOCK_SIZE}. However,
 * store the actual length of the block in the data record. Never read the
 * unwritten bytes so that short files and long files all appear to have exactly
 * those bytes that were written but you can read blocks using random access.
 * <P>
 * Also modify the {@link FileVersionInputStream} to maintain running byte count
 * so that it can report the #of blocks expected, visited so far, and the #of
 * bytes read so far. The {@link #inputStream(String, int)} method can be
 * defined to return an interface that reveals those little goodies.
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
 * @todo ensure that atomic appends always go to a single data service so that
 *       the update is in fact atomic. after the write the data service can
 *       decide the partition the index, in which case the next append might go
 *       to another data service. In all cases the append is atomic on a single
 *       data service.
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
        
        writeContent( id, version, document.getInputStream() );
        
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
     * Break up the content into blocks and write them onto the data index.
     * 
     * @todo encapsulate this as a class using a pipe model so that someone can
     *       write continuously on an {@link OutputStream} and have the data
     *       transparently partitioned into blocks and those blocks written with
     *       atomic appends. Provide a record aware and block boundary aware
     *       extensible layer for the class so that applications can pad out to
     *       fill a block when the goal is transparently split during m/r
     *       processing.
     */
    public void writeContent(String id, int version, InputStream is) {

        /*
         * This constant determines the amount of data that will go into each
         * atomic append invoked by this method.
         */
        final int BUFFER_SIZE = 64 * Bytes.kilobyte32; // @todo config.
        
        if (!(is instanceof ByteArrayInputStream)
//                && !(is instanceof BufferedInputStream)
                ) {

            /*
             * @todo Try this with just the default buffer size. I think that
             * the size of the input stream buffer will become less critical as
             * the #of concurrent clients of this interface goes up. However we
             * always want to use a large buffer to collect writes that will be
             * sent across the data service interface in order to keep up the
             * efficiency of the RPC.
             */

            is = new BufferedInputStream(is, BUFFER_SIZE);
            
        }

        /*
         * Fill the buffer repeatedly, doing an atomic append on the file
         * each time the buffer becomes full.
         */
        
        // @todo use a thread-local variable for this buffer?
        final byte[] b = new byte[BUFFER_SIZE];

        int off = 0; // offset of the 1st unwritten byte in the buffer.
        int remaining = b.length; // #of unwritten bytes remaining in buf.
        long nwritten = 0; // total #of bytes written.
        int nblocks = 0; // total #of blocks (buffers) written.
        
        try {

            while (true) {

                int nread = is.read(b, off, remaining);

                if (nread == -1)
                    break;

                off += nread;
                remaining -= nread;
                
                if (remaining == 0) {

                    /*
                     * The buffer is full. Incremental write using atomic append
                     * each time the buffer is filled to capacity.
                     */
                    
                    atomicAppend( id, version, b, 0/*offset*/, off/*length*/ );

                    // increment totals.
                    nblocks++;
                    nwritten += off;

                    // reset within buffer counters.
                    off = 0;
                    remaining = b.length;
                    
                }
                
            }

            if (off > 0) {

                /*
                 * Write anything remaining once the input stream is exhausted.
                 */
                
                atomicAppend( id, version, b, 0/*offset*/, off/*length*/ );

                // incremental totals.
                nblocks++;
                nwritten += off;

            }

            log.info("Wrote " + nwritten + " bytes on " + id + " in " + nblocks
                    + " blocks");
            
        } catch (Throwable t) {

            log.error("Problem writing on file: id=" + id, t);
            
            throw new RuntimeException( t );

        } finally {

            try {
                
                is.close();

            } catch (IOException e) {
                
                log.warn("Problem closing input stream.", e);
                
            }

        }
        
    }

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
            final long nextBlock;
            {

                /*
                 * Find the key for the last block written for this file. When
                 * no blocks have been written on this index partition then we
                 * start with block#0.
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
                 * Index of the first key after this file.
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

                log.debug("toIndex="+toIndex+", entryCount="+ndx.rangeCount(null, null)); // @todo comment out.

                if (toIndex == 0) {
                    
                    log.info("Will write 1st block for file: id="+id);
                    
                    nextBlock = 0;
                    
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
                        nextBlock = KeyBuilder.decodeLong(key, key.length
                                - Bytes.SIZEOF_LONG) + 1;
                        
                        log.info("Appending to existing file: id=" + id
                                + ", version=" + version + ", nextBlock="
                                + nextBlock);
                        
                    } else {
                        
                        log.info("Will write 1st block for file: id=" + id
                                + ", version=" + version);
                        
                        nextBlock = 0;
                        
                    }
                    
                }

                log.info("Will write " + len + " bytes on id=" + id
                                + ", version=" + version + ", next block#="
                                + nextBlock);
                
            }

            long block = nextBlock; // next block# to be written.
            int off = this.off; // offset of the next byte to write.
            int remaining = this.len; // #of bytes remaining to be written.
            long nwritten = 0; // #of bytes written.
            int nblocks = 0; // #of blocks written.

            // the block will be written on the store.
            final IRawStore store = ((BTree) ndx).getStore();
            
            final DataOutputBuffer out = new DataOutputBuffer(Bytes.SIZEOF_LONG);
            
            while(remaining>0) {
            
                // write block size bytes up to the #of bytes remaining in the buffer.
                final int n = Math.min(BLOCK_SIZE, remaining);
                
                // write the block on the journal.
                final long addr = store.write(
                        ByteBuffer.wrap(b, off, n));

                // form the key for the index entry for this block.
                final byte[] key = keyBuilder.reset()
                        .appendText(id, true/* unicode */, false/* successor */)
                        .append(version)
                        .append(block).getKey();
                
                // record the address of the block in the index.
                {
                
                    // encode the value for the entry.
                    out.reset().putLong(addr);
                    
                    // insert the entry into the index.
                    ndx.insert(key, out.toByteArray() );
                 
                    log.info("id=" + id + ", version=" + version
                            + ", wrote block=" + block + " @ addr"
                            + store.toString(addr));
                    
                }

                // increment counters.
                off += n;
                remaining -= n;
                nwritten += n;
                nblocks++;
                block++;
            
            }
            
            log.info("Wrote " + nwritten + " bytes in " + nblocks
                    + " blocks: id=" + id + ", version=" + version
                    + ", next block#=" + block);
            
            // #of blocks written.
            return Integer.valueOf(nblocks);
            
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
     * 
     * @return #of blocks written.
     */
    public int atomicAppend(String id, int version, byte[] b, int off, int len) {
        
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

        if(len % BLOCK_SIZE != 0) {
            
            log.warn("Not an even multiple of the block size: id=" + id
                    + ", version=" + version + ", len=" + len + ", blockSize="
                    + BLOCK_SIZE);
            
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

            log.info("Submitting append operation to data service: id="+id+", version="+version+", len="+len);
            
            Integer nblocks = (Integer) dataService.submit(IBigdataFederation.UNISOLATED, name, proc);

            return nblocks;
            
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
     *         version.
     */
    public FileVersionInputStream inputStream(String id,int version) {

        /*
         * Range count the file and version on the federation - this is the
         * number of blocks of data for that file and version as of the start of
         * this read operation. If the result is zero then there are no index
         * partitions which span that file and version and we return null.
         */
        
        final long nblocks = getBlockCount(id, version);
        
        if(nblocks==0) {
            
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
     * Reads from blocks visited by a range scan for a file and version.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    protected class FileVersionInputStream extends InputStream {

        private final String id;
        private final int version;
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
            assert b.length > 0;
            
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
            int v = b[off++];

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
            
            byte[] b = (byte[])val;
            
            in.setBuffer(b);
            
            final long addr;
            
            try {
            
                addr = in.readLong();
                
            } catch (IOException e) {
                
                throw new RuntimeException(e);
                
            }
            
            /*
             * FIXME This is a great example of when passing in the buffer makes
             * sense. The code here jumps through some hoops to avoid double
             * copying the data.
             * 
             * Note: hasArray() will not return true if the buffer is marked
             * [readOnly].
             */
            final ByteBuffer buffer = store.read(addr);

            if(buffer.hasArray() && buffer.arrayOffset()==0) {

                return buffer.array();
                
            }
            
            // log a notice since this is so annoying.
            log.warn("Cloning data in block");
            
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
