package com.bigdata.repo;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.bigdata.btree.AbstractBTree;
import com.bigdata.btree.IIndex;
import com.bigdata.btree.keys.IKeyBuilder;
import com.bigdata.btree.proc.ISimpleIndexProcedure;
import com.bigdata.io.DataOutputBuffer;
import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.Journal;
import com.bigdata.rawstore.Bytes;

/**
 * Atomic write of a single block for a file version.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class AtomicBlockWriteProc implements ISimpleIndexProcedure,
        Externalizable {

    private static final long serialVersionUID = 4982851251684333327L;

    protected static transient Logger log = Logger
            .getLogger(AtomicBlockWriteProc.class);

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
    
    public final boolean isReadOnly() {
        
        return false;
        
    }
    
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
    public AtomicBlockWriteProc(BigdataRepository repo,String id, int version, long block, byte[] b, int off, int len) {

        assert id != null && id.length() > 0;
        assert version >= 0;
        assert block >= 0 && block <= BigdataRepository.MAX_BLOCK;
        assert b != null;
        assert off >= 0 : "off="+off;
        assert len >= 0 && off + len <= b.length;
        assert len <= repo.getBlockSize(): "len="+len+" exceeds blockSize="+repo.getBlockSize();

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
        final IKeyBuilder keyBuilder = ndx.getIndexMetadata().getKeyBuilder();

        /*
         * Write the block on the journal, obtaining the address at which it
         * was written - use 0L as the address for an empty block.
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