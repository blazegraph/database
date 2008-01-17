/*
 * Created on Jan 17, 2008
 */
package com.bigdata.repo;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bigdata.btree.BatchInsert;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.IIndexProcedure;
import com.bigdata.btree.IndexProcedure;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.scaleup.MetadataIndex;
import com.bigdata.service.ClientIndexView;
import com.bigdata.service.IBigdataClient;
import com.bigdata.service.IBigdataFederation;
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
 * append only. The content length of the file is not stored as file metadata
 * and is estimated by a range count of the index entries spanned by the file's
 * data. The exact file size may be readily determined when reading small files
 * by the expediency of sucking the entire file into a buffer. Streaming
 * processing is advised in all cases when handling large files, including when
 * the file is to be delivered via HTTP.
 * <p>
 * The metadata index uses a {@link SparseRowStore} design, similar to Google's
 * bigtable or Hadoop's HBase. Certain properties MUST be defined for each entry -
 * they are documented on the {@link MetadataSchema}. Applications are free to
 * define additional properties. Reads and writes of file metadata are always
 * atomic.
 * <p>
 * The data index uses the {@link MetadataSchema#ID} as the first field in a
 * compound key. The remainder of the key is a "chunk" identifier. The chunk
 * identifiers are strictly monotonic (e.g., up one) and their sequence orders
 * the chunks in the logical byte order of the file. All writes on a file are
 * atomic appends. In general, applications atomic appends should write 64k of
 * data - less if the last block of the file is being written. As an aid to
 * local clients, method exists to consume an {@link InputStream}, buffering
 * data and performing atomic appends as the buffer overflows. Distributed
 * clients can also use atomic appends, but they must provide themselves for an
 * internal consistency within the blocks, e.g., by always padding out records
 * to the next 64k boundary. In particular, this makes it easy for a master to
 * split a file across map clients and makes it equally easy for distributed
 * processes to aggregate results.
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
 * count and multiplying through by the chunk size. Splits of some desirable
 * block size can then be assembled out of those chunks and handed off to the
 * clients in parallel (of course, clients need to deal with the hassle of
 * processing files where records will cross split boundaries unless they always
 * pad out with unused bytes to the next 64k boundary).
 * <p>
 * Use case: A reduce client wants to write a very large files so it creates a
 * metadata record for the file and then does a series of atomic appears to the
 * file.
 * 
 * @todo Full text indexing support. Perhaps the best way to handle this is to
 *       queue document metadata up for a distributed full text indexing
 *       service. The service accepts metadata for documents from the queue and
 *       decides whether or not the document should be indexed based on its
 *       metadata and how the document should be processed if it is to be
 *       indexed. Those business rules would be registered with the full text
 *       indexing service. (Alternatively, they can be configured with the
 *       {@link BigdataRepository} and applied locally as the chunks of the file
 *       are written into the repository. That's certainly easier right off the
 *       bat.)
 * 
 * @todo add test suite.
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
     * The content will be broken up into chunks of this size. Those chunks will
     * become the values stored in the {@link #dataIndex}. Keeping the chunk
     * size down makes it easier for the index to manage its storage. Note that
     * a large number of chunks may be written or read in a single operation and
     * that chunks will be sequential in the {@link #dataIndex} so the chunk
     * size has very little to do with the efficiency of reads and writes.
     * 
     * @todo it would be good to reuse the buffers but we can only down that
     *       when we know that the index is remote (or once we modify the btree
     *       to copy its keys and values rather than storing references).
     */
    private final int CHUNK_SIZE = 8 * Bytes.kilobyte32;
    
    /**
     * The client used to connect to the bigdata federation.
     */
    private IBigdataClient client;
    
    /**
     * The name of the scale-out index in which the metadata are stored. This is
     * a {@link SparseRowStore} governed by the {@link #metadataSchema}.
     */
    private static final String METADATA_NAME = BigdataRepository.class.getSimpleName()+"#metadata";
    
    /**
     * The name of the scale-out index in which the data are stored. The entries
     * in this index are a series of chunks for a blob. Chunks are up to 16k
     * each and are assigned sequential chunk numbers by the client. The final
     * chunk may be smaller (there is no need to pad out the data with nulls).
     * The keys are formed from two fields - a field containing the content
     * identifier followed by an integer field containing the sequential chunk
     * number. A range scan with a fromKey of the chunk identifier and a toKey
     * computed using the successor of the chunk identifier will naturally visit
     * all chunks of a blob in sequence.
     */
    private static final String DATA_NAME = BigdataRepository.class.getSimpleName()+"#data";
    
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

        public MetadataSchema() {
            
            super("metadata", "id", KeyType.Unicode);
            
        }
        
    }

    public static final MetadataSchema metadataSchema = new MetadataSchema();
    
    final SparseRowStore metadataIndex;
    
    final IIndex dataIndex;
    
    // @todo SparseRowStore should defined this constant.
    final long AUTO_TIMESTAMP = -1L;
    
    /**
     * @param client
     *            The client.
     * @param properties
     *            See {@link Options}.
     */
    public BigdataRepository(IBigdataClient client,Properties properties) {
        
        this.client = client;
        
        IBigdataFederation fed = client.connect();

        // setup metadata index.
        {

            IIndex ndx = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, METADATA_NAME);

            if (ndx == null) {

                fed.registerIndex(METADATA_NAME);

                // @todo review provisioning.
                ndx = (ClientIndexView) fed.getIndex(
                        IBigdataFederation.UNISOLATED, METADATA_NAME);

            }

            metadataIndex = new SparseRowStore(ndx, KeyBuilder
                    .newUnicodeInstance(properties), metadataSchema);
            
        }

        // setup data index.
        {

            IIndex ndx = (ClientIndexView) fed.getIndex(
                    IBigdataFederation.UNISOLATED, DATA_NAME);

            if (ndx == null) {

                // @todo review provisioning.
                fed.registerIndex(DATA_NAME);

                ndx = (ClientIndexView) fed.getIndex(
                        IBigdataFederation.UNISOLATED, DATA_NAME);

            }
            
            dataIndex = ndx;

        }

    }

    public void close() {

        // disconnects and terminates the client.
        client.terminate();
        
    }

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

        writeContent( id, document.getInputStream() );
        
    }
    
    /**
     * Break up the content into chunks and write them onto the data index.
     */
    protected void writeContent(String id, InputStream is) {

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
        final byte[] buffer = new byte[BUFFER_SIZE];

        int off = 0;
        int len = buffer.length;
        long nwritten = 0;
        int nblocks = 0;
        
        try {

            while (true) {

                int nread = is.read(buffer, off, len);

                if (nread == -1)
                    break;

                off += nread;
                len -= nread;
                
                if(len==0) {

                    /*
                     * Incremental write using atomic append each time the
                     * buffer is filled to capacity.
                     */
                    
                    atomicAppend( id, off, buffer );

                    nblocks++;
                    nwritten += off;
                    off = 0;
                    len = buffer.length;
                    
                }
                
            }

            if (off > 0) {

                /*
                 * Write anything remaining once the input stream is exhausted.
                 */
                
                nblocks++;
                nwritten += off;
                atomicAppend(id, off, buffer);

            }

            log.info("Wrote " + nwritten + " bytes on " + id + " in " + nblocks
                    + " blocks");
            
        } catch (Throwable t) {

            /*
             * @todo remove the entry that we just made to the metadata index?
             */

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
     * Atomic append of data onto the end of a file.
     * <p>
     * Note: Atomic append is well-suited to writing blocks of 64k or more at a
     * time onto a file.
     * 
     * @param id
     *            The file identifier.
     * @param nbytes
     *            The #of bytes to be written.
     * @param buffer
     *            The buffer containing the bytes to be written.
     */
    public void atomicAppend(String id, int nbytes, byte[] buffer) {
            
        if (nbytes == 0) {

            log.warn("Nothing to write: id=" + id);

            return;

        }

        /*
         * Transmit up to 64k at a time.
         * 
         * FIXME This should just be a custom procedure so that we don't have to
         * break down the byte[] into a byte[][] just to send it across. Also,
         * the index will have local knowledge about the last assigned chunk in
         * the sequence for this id, so we really have to make those decisions
         * on the server.
         */
        final int MAX_CHUNKS = 64 * Bytes.kilobyte32 / CHUNK_SIZE;

        int nchunks = 0;

        byte[][] keys = new byte[MAX_CHUNKS][];
        byte[][] vals = new byte[MAX_CHUNKS][];

        while (true) {

            if (nchunks == MAX_CHUNKS)
                break;

        }

        BatchInsert op = new BatchInsert(nchunks, keys, vals);

        dataIndex.insert(op);

    }
    
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

    public Document read(String id) {
        // TODO Auto-generated method stub
        return null;
    }

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

}
