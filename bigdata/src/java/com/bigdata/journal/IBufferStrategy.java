package com.bigdata.journal;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.bigdata.rawstore.Addr;
import com.bigdata.rawstore.IRawStore;

/**
 * <p>
 * Interface for implementations of a buffer strategy as identified by a
 * {@link BufferMode}. This interface is designed to encapsulate the
 * specifics of reading and writing slots and performing operations to make
 * an atomic commit.
 * </p>
 * <p>
 * Note: Implementations of this interface SHOULD NOT provide thread-safety.
 * This interface is always used within a single-threaded context.
 * </p>
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IBufferStrategy extends IRawStore {
    
    /**
     * The next offset at which a data item would be written on the store.
     */
    public int getNextOffset();
    
    /**
     * The buffer mode supported by the implementation
     * 
     * @return The implemented buffer mode.
     */
    public BufferMode getBufferMode();
    
    public long getInitialExtent();
    
    public long getMaximumExtent();
    
    /**
     * The current size of the journal in bytes.  When the journal is backed by
     * a disk file this is the actual size on disk of that file.  The initial
     * value for this property is set by {@link Options#INITIAL_EXTENT}.
     */
    public long getExtent();

    /**
     * The size of the user data extent in bytes.
     * <p>
     * Note: The size of the user extent is always generally smaller than the
     * value reported by {@link #getExtent()} since the latter also reports the
     * space allocated to the journal header and root blocks.
     */
    public long getUserExtent();
    
    /**
     * Either truncates or extends the journal.
     * 
     * @param extent The new extent.
     * 
     * @exception IllegalArgumentException
     *                The user extent MAY NOT be increased beyond
     *                {@link Integer#MAX_VALUE} since that is the largest offset
     *                that may be addressed by an {@link Addr address}.
     */
    public void truncate(long extent);

    /**
     * Delete the backing file for the journal if any.
     * 
     * @exception IllegalStateException
     *                if the journal is not closed.
     */
    public void deleteFile();

    /**
     * Write the root block onto stable storage (ie, flush it through to disk).
     * 
     * @param rootBlock
     *            The root block. Which root block is indicated by
     *            {@link IRootBlockView#isRootBlock0()}.
     * 
     * @param forceOnCommit
     *            Governs whether or not the journal is forced to stable storage
     *            and whether or not the file metadata for the journal is forced
     *            to stable storage. See {@link Options#FORCE_ON_COMMIT}.
     * 
     * @todo this is basically an atomic commit. Reconcile it with
     *       {@link IAtomicStore#commit()}. Note that the latter works with
     *       registered committers, while this does not.
     */
    public void writeRootBlock(IRootBlockView rootBlock,
            ForceEnum forceOnCommitEnum);
    
    /**
     * A block operation that transfers the serialized records (aka the written
     * on portion of the user extent) en mass from the buffer onto an output
     * file. The buffered records are written "in order" starting at the current
     * position on the output file. The file is grown if necessary.
     * 
     * @param out
     *            The file to which the buffer contents will be transferred.
     * 
     * @return The #of bytes written.
     * 
     * @throws IOException
     */
    public long transferTo(RandomAccessFile out) throws IOException;
    
}
