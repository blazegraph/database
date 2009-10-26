package com.bigdata.bfs;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

import com.bigdata.btree.IOverflowHandler;
import com.bigdata.btree.ITuple;
import com.bigdata.btree.keys.KeyBuilder;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.rawstore.IBlock;
import com.bigdata.rawstore.IRawStore;

/**
 * Copies blocks onto the target store during overflow handling. Blocks that
 * are no longer referenced by the file data index will be left behind on
 * the journal and eventually discarded with the journal.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BlobOverflowHandler implements IOverflowHandler {

    /**
     * 
     */
    private static final long serialVersionUID = -8180664203349900189L;

    /**
     * De-serialization constructor.
     */
    public BlobOverflowHandler() {

    }

    DataOutputBuffer buf;

    public void close() {

        buf = null;

    }

    public byte[] handle(final ITuple tuple, final IRawStore target) {

        if (buf == null) {

            buf = new DataOutputBuffer();

        }

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

        final InputStream bin = block.inputStream();

        //            // the address on which the block will be written.
        //            final long addr2 = block2.getAddress();
        final long addr2;
        try {

            //                // #of bytes read so far.
            //                long n = 0;
            //                
            //                while (len - n > 0) {

            // read source into buffer.
            final int nread = bin.read(buf.array(), 0, len);

            if (nread != len) {

                throw new RuntimeException("Premature end of block: expected="
                        + len + ", actual=" + nread);

            }

            // write on the target store.
            addr2 = target.write(buf.asByteBuffer());

            //                    // write buffer onto sink.
            //                    bout.write(buf, 0, nread);

            //                    n += nread;
            //
            //                }

            //                bout.flush();

        } catch (IOException ex) {

            BigdataFileSystem.log.warn("Problem copying block: addr=" + addr
                    + ", len=" + len, ex);

            throw new RuntimeException(ex);

        } finally {

            //                try {
            //                    bout.close();
            //                } catch (IOException ex) {
            //                    log.warn(ex);
            //                }

            try {
                bin.close();
            } catch (IOException ex) {
                BigdataFileSystem.log.warn(ex);
            }

        }

        // the address of the block on the target store.
        return KeyBuilder.asSortKey(addr2);

    }

}
