package com.bigdata.rdf.internal;

import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.io.IByteArrayBuffer;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;

/**
 * Copies blocks onto the target store during overflow handling. Blocks that are
 * no longer referenced by the file data index will be left behind on the
 * journal and eventually discarded with the journal. The blob reference is a
 * single <code>long</code> integer stored in the value of the tuple and gives
 * the address of the raw record from which the blob may be read on
 * {@link IRawStore} from which the tuple was read.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id: BlobOverflowHandler.java 2265 2009-10-26 12:51:06Z thompsonbry
 *          $
 * 
 *          FIXME {@link IBlock} reads are not finished so this will not work
 *          yet. It seems that {@link IBlock} is might stand a refactor so that
 *          you get an {@link IByteArrayBuffer} instead of an {@link IBlock}.
 *          The remote iterator methods and remote tuple lookup methods (which
 *          have not yet been defined) would then use a smart proxy to handle
 *          the materialization of the raw record in the caller's JVM. There is
 *          no reason to add a streaming interface here since raw records and
 *          {@link IOverflowHandler}s are not useful once you get more than a
 *          few MB worth of data in the record. At that point you really need to
 *          use a REST-ful repository service which stores the data as chunks in
 *          the file system (ala BFS, but not trying to store the chunks in the
 *          journal or index segment since they are capped at ~200MB each and
 *          the file system chunk size might be closer to 64MB).
 * 
 *          FIXME Move this class to the btree package and integrate into its
 *          test suite.
 * 
 *          FIXME The class of the same name in the BFS package will need to be
 *          modified. Per above, BFS will need to store its chunks in the local
 *          file system. Therefore the blob reference will have to include the
 *          local file name (UUID_chunkNumber_version?) and the chunk will have
 *          to be replicated on each node having a given shard of chunks. For
 *          BFS, we clearly DO NOT want to move shards around if we can help it
 *          since we will have to move a huge amount of data around as well.
 *          <p>
 *          An alternative is to use a parallel file system to store the chunks,
 *          in which case we do not have to worry about replicating the chunks
 *          and we can move the shards easily enough since the chunks are
 *          elsewhere.
 */
public class BlobOverflowHandler implements IOverflowHandler {

    /**
     * 
     */
    private static final long serialVersionUID = 6203072965860516919L;
    
    protected static final transient Logger log = Logger.getLogger(BlobOverflowHandler.class);
    
    /**
     * Sole constructor.
     */
    public BlobOverflowHandler() {

    }

    /**
     * Lazily initialized by {@link #handle(ITuple, IRawStore)}.
     */
    private transient DataOutputBuffer buf = null;

    public void close() {

        // release reference to the buffer.
        buf = null;

    }

    public byte[] handle(final ITuple tuple, final IRawStore target) {

        if (buf == null) {

            buf = new DataOutputBuffer();

        }

        /*
         * Decode the blob reference.
         * 
         * Note: The blob reference is just the [addr] of the record on the
         * journal or index segment from which that blob reference was read.
         */
        final long addr;
        try {
            addr = tuple.getValueStream().readLong();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (addr == 0L) {
            /*
             * Note: empty blocks are allowed and are recorded with 0L as
             * their address.
             */
            return KeyBuilder.asSortKey(0L);
        }

        // read block from underlying source store.
        final IBlock block = tuple.readBlock(addr);

        // #of bytes in the block.
        final int len = block.length();

        // make sure buffer has sufficient capacity.
        buf.ensureCapacity(len);

        // prepare buffer for write.
        buf.reset();

        // the address on which the block will be written.
        final long addr2;

        // read on the source record.
        final InputStream bin = block.inputStream();
        try {

            // read source into buffer (will be large enough).
            final int nread = bin.read(buf.array(), 0, len);

            if (nread != len) {

                throw new RuntimeException("Premature end of block: expected="
                        + len + ", actual=" + nread);

            }

            // write on the target store.
            addr2 = target.write(buf.asByteBuffer());

        } catch (IOException ex) {

            throw new RuntimeException("Problem copying block: addr=" + addr
                    + ", len=" + len, ex);

        } finally {

            try {
                bin.close();
            } catch (IOException ex) {
                log.warn(ex);
            }

        }

        /*
         * Format the address of the block on the target store (a long integer)
         * into a byte[] and return that byte[]. This will be the value paired
         * with the tuple in the target resource.
         */
        buf.reset();
        buf.putLong(addr2);
        return buf.toByteArray();

    }

}
