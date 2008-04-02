package com.bigdata.repo;

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

import com.bigdata.btree.ITuple;
import com.bigdata.btree.ITupleIterator;
import com.bigdata.btree.KeyBuilder;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.IBlock;

/**
 * Reads from blocks visited by a range scan for a file and version.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileVersionInputStream extends InputStream {

    protected final String id;

    protected final int version;

    private final ITupleIterator src;

    /**
     * The current block# whose data are being read.
     */
    private long block;

    /**
     * A buffer holding the current block's data. This is initially filled
     * from the first block by the ctor. When no more data is available it
     * is set to <code>null</code> to indicate that the input stream has
     * been exhausted.
     * 
     * @todo reuse buffers sized out to the block size.
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

    public FileVersionInputStream(String id, int version, ITupleIterator src) {

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

        if (!src.hasNext()) {

            BigdataRepository.log.info("No more blocks: id=" + id
                    + ", version=" + version);

            b = null;

            off = 0;

            len = 0;

            return false;

        }

        final ITuple tuple = src.next();

        /*
         * decode the block address.
         */
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

            b = new byte[] {};

            off = 0;

            len = 0;

            BigdataRepository.log.info("Read zero bytes: id=" + id
                    + ", version=" + version + ", block=" + block);

        } else {

            byte[] key = tuple.getKey();

            // decode the block identifier from the key.
            //                block = KeyBuilder.decodeLong(tuple.getKeyBuffer().array(),
            //                        tuple.getKeyBuffer().pos() - Bytes.SIZEOF_LONG);
            block = KeyBuilder.decodeLong(key, key.length - Bytes.SIZEOF_LONG);

            final IBlock tmp = tuple.readBlock(addr);

            final int nbytes = tmp.length();

            // @todo reuse buffer!
            b = new byte[nbytes];

            off = 0;

            len = nbytes;

            try {

                final int nread = tmp.inputStream().read(b, off, len);

                if (nread != len) {

                    throw new RuntimeException("Expecting " + len
                            + " bytes but read " + nread);

                }

            } catch (IOException e) {

                throw new RuntimeException(e);

            }

            BigdataRepository.log.info("Read " + b.length + " bytes: id=" + id
                    + ", version=" + version + ", block=" + block);

        }

        return true;

    }

    public int read() throws IOException {

        if (b == null) {

            // nothing left to read.

            return -1;

        }

        if (off == len) {

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
    public int read(byte[] b, int off, int len) throws IOException {

        if (b == null) {

            // nothing left to read.

            return -1;

        }

        if (this.off == this.len) {

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

        return n;

    }

}
