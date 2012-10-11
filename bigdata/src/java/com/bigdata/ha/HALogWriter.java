package com.bigdata.ha;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Formatter;

import org.apache.log4j.Logger;

import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.SerializerUtil;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.rawstore.Bytes;

/**
 * Wrapper class to handle process log creation and output for HA.
 * 
 * The process log stores the HAWriteMessages and buffers to support reading and
 * reprocessing as part of the HA synchronization protocol.
 * 
 * The writer encapsulates not only the writing of individual messages but also
 * the closing and creation of new files.
 * 
 * @author Martyn Cutcher
 */
public class HALogWriter {

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /*
     * Note: All of this stuff is to be more or less compatible with the magic
     * and version at the start of a Journal file. We use a different MAGIC
     * value for the HA Log, but the same offset to the first and second root
     * blocks. The data starts after the 2nd root block.
     */
    
    static final int SIZE_MAGIC = Bytes.SIZEOF_INT;
    static final int SIZE_VERSION = Bytes.SIZEOF_INT;
    static final int SIZEOF_ROOT_BLOCK = RootBlockView.SIZEOF_ROOT_BLOCK;

    /**
     * Offset of the first root block in the file.
     */
    static final int OFFSET_ROOT_BLOCK0 = SIZE_MAGIC + SIZE_VERSION;

    /**
     * Offset of the second root block in the file.
     */
    static final int OFFSET_ROOT_BLOCK1 = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 1);

    /**
     * The size of the file header, including MAGIC, version, and both root
     * blocks. The data starts at this offset.
     */
    static final int headerSize0 = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 2);

    /**
     * Magic value for HA Log (the root blocks have their own magic value).
     */
    static final int MAGIC = 0x83d9b735;

    /**
     * HA log version number (version 1).
     */
    static final int VERSION1 = 0x1;

    /** HA log directory. */
    private final File m_dir;

    /**
     * The root block of the leader at the start of the current write set.
     */
    private IRootBlockView m_rootBlock;

    /** Current write cache block sequence counter. */
    private long m_nextSequence = 0;

    /** current log file. */
    private File m_log = null;

    public static final String HA_LOG_EXT = ".ha-log";
    
    /** current output file channel. */
    private RandomAccessFile m_raf = null;
    private FileChannel m_channel = null;

    /** current write point on the channel. */
    private long m_position = headerSize0;

    /**
     * Return the commit counter that is expected for the writes that will be
     * logged (the same commit counter that is on the opening root block).
     */
    public long getCommitCounter() {

        assertOpen();
        
        return m_rootBlock.getCommitCounter();
        
    }
    
    /**
     * Return the sequence number that is expected for the next write.
     */
    public long getSequence() {

        assertOpen();

        return m_nextSequence;
        
    }
    
    private void assertOpen() {

        if (m_raf == null)
            throw new IllegalStateException();
        
    }

    /**
     * Return the log file (if any).
     */
    public File getFile() {
        
        return m_log;
        
    }
    
    /**
     * Return the local name of the HA Log file associated with the 
     * @param commitCounter
     * @return
     */
    public static String getHALogFileName(final long commitCounter) {

        /*
         * Format the name of the log file.
         * 
         * Note: The commit counter in the file name should be zero filled to 20
         * digits so we have the files in lexical order in the file system (for
         * convenience).
         */
        final String logFile;
        {

            final StringBuilder sb = new StringBuilder();

            final Formatter f = new Formatter(sb);

            f.format("%020d" + HA_LOG_EXT, commitCounter);
            f.flush();
            f.close();

            logFile = sb.toString();

        }

        return logFile;

    }

    public String toString() {
        
        final IRootBlockView tmp = m_rootBlock;

        final long seq = m_nextSequence;

        return getClass().getName() + "{" + m_raf == null ? "closed"
                : "commitCounter=" + tmp.getCommitCounter() + ",nextSequence="
                        + seq + "}";

    }

    public HALogWriter(final File logDir) {

        m_dir = logDir;

    }

    /**
     * Open an HA log file for the write set starting with the given root block.
     * 
     * @param rootBlock
     *            The root block.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void createLog(final IRootBlockView rootBlock)
            throws FileNotFoundException, IOException {

        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (m_rootBlock != null) // may not be open.
            throw new IllegalStateException();

        if (haLog.isInfoEnabled())
            haLog.info("rootBlock=" + rootBlock);

        m_rootBlock = rootBlock;

        m_nextSequence = 0L;

        /*
         * Format the name of the log file.
         * 
         * Note: The commit counter in the file name should be zero filled to 20
         * digits so we have the files in lexical order in the file system (for
         * convenience).
         * 
         * Note: We use commitCounter+1 so the file will be labeled by the
         * commit point that will be achieved when that log file is applied to a
         * journal whose current commit point is [commitCounter].
         */

        final long commitCounter = rootBlock.getCommitCounter();

        final String logFile = getHALogFileName(commitCounter + 1);

        m_log = new File(m_dir, logFile);

        // Must delete file if it exists.
        if (m_log.exists() && !m_log.delete()) {

            /*
             * It is a problem if a file exists and we can not delete it. We
             * need to be able to remove the file and replace it with a new file
             * when we log the write set for this commit point.
             */

            throw new IOException("Could not delete: " + m_log);

        }
        
        m_raf = new RandomAccessFile(m_log, "rw");

        m_channel = m_raf.getChannel();

        /*
         * Write the MAGIC and version on the file.
         */
        m_raf.seek(0);
        m_raf.writeInt(MAGIC);
        m_raf.writeInt(VERSION1);

        /*
         * Write root block to slots 0 and 1.
         * 
         * Initially, the same root block is in both slots. This is a valid, but
         * logically empty, HA Log file.
         * 
         * When the HA Log file is properly sealed with a root block, that root
         * block is written onto slot 1.
         */

        writeRootBlock(true/* isRootBlock0 */, rootBlock);
        
        writeRootBlock(false/* isRootBlock0 */, rootBlock);

    }

    /**
     * Hook for {@link FileChannelUtility#writeAll(IReopenChannel, ByteBuffer, long)}
     */
    private final IReopenChannel<FileChannel> reopener = new IReopenChannel<FileChannel>() {

        @Override
        public FileChannel reopenChannel() throws IOException {

            if (m_channel == null)
                throw new IOException("Closed");

            return m_channel;

        }
    };

    /**
     * Write the final root block on the HA log and close the file. This "seals"
     * the file, which now represents the entire write set associated with the
     * commit point in the given root block.
     * 
     * @param rootBlock
     *            The final root block for the write set.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public void closeLog(final IRootBlockView rootBlock)
            throws FileNotFoundException, IOException {

        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (m_rootBlock == null) // no root block associated with log.
            throw new IllegalStateException();

        if (haLog.isInfoEnabled())
            haLog.info("rootBlock=" + rootBlock);

        final long expectedCommitCounter = this.m_rootBlock.getCommitCounter() + 1;

        if (expectedCommitCounter != rootBlock.getCommitCounter()) {

            throw new IllegalStateException("CommitCounter: expected="
                    + expectedCommitCounter + ", actual="
                    + rootBlock.getCommitCounter());

        }

//        if (rootBlock.getLastCommitTime() != this.m_rootBlock
//                .getLastCommitTime()) {
//
//            throw new IllegalStateException();
//
//        }

        if (!this.m_rootBlock.getUUID().equals(rootBlock.getUUID())) {

            throw new IllegalStateException("Store UUID: expected="
                    + (m_rootBlock.getUUID()) + ", actual="
                    + rootBlock.getUUID());

        }

        flush(); // current streamed data

        /*
         * The closing root block is written into which ever slot corresponds to
         * its whether that root block is root block zero. Both root blocks are
         * identical up to this point, so we can write the closing root block
         * into either slot. HALogReader will use the commit counters to figure
         * out which root block is the opening root block and which root block
         * is the closing root block.
         */
        writeRootBlock(rootBlock.isRootBlock0(), rootBlock);
        
//        // The closing root block is always in slot 1.
//        writeRootBlock(false/* isRootBlock0 */, rootBlock);

        close();

    }

    /**
     * Writes the root block at the given offset.
     */
    private void writeRootBlock(final boolean isRootBlock0,
            final IRootBlockView rootBlock) throws IOException {

        if (rootBlock == null)
            throw new IllegalArgumentException();

        final long position = isRootBlock0 ? OFFSET_ROOT_BLOCK0
                : OFFSET_ROOT_BLOCK1;

        FileChannelUtility.writeAll(reopener, rootBlock.asReadOnlyBuffer(),
                position);

        if (haLog.isDebugEnabled())
            haLog.debug("wrote root block: " + rootBlock);

    }

    /**
     * 
     * @param msg
     * @param data
     */
    public void write(final IHAWriteMessage msg, final ByteBuffer data)
            throws IOException {

        assertOpen();

        /*
         * Check if this really is a valid message for this file. If it is not,
         * then close the file and return immediately
         */
        if (m_rootBlock.getCommitCounter() != msg.getCommitCounter())
            throw new IllegalStateException("commitCounter="
                    + m_rootBlock.getCommitCounter() + ", but msg=" + msg);

        if (m_rootBlock.getLastCommitTime() != msg.getLastCommitTime())
            throw new IllegalStateException("lastCommitTime="
                    + m_rootBlock.getLastCommitTime() + ", but msg=" + msg);

        if (m_nextSequence != msg.getSequence())
            throw new IllegalStateException("nextSequence=" + m_nextSequence
                    + ", but msg=" + msg);

        if (haLog.isInfoEnabled())
            haLog.info("msg=" + msg + ", position=" + m_position);

        if (m_position < headerSize0)
            throw new AssertionError("position=" + m_position
                    + ", but headerSize=" + headerSize0);

        /*
         * Write the HAWriteMessage onto the channel.
         */
        {
            // serialized message object (pos=0; limit=nbytes)
            final ByteBuffer tmp = bufferObject(msg);

            final int nbytes = tmp.limit();

            FileChannelUtility.writeAll(reopener, tmp, m_position);

            m_position += nbytes;
            
            m_nextSequence++;
            
        }
        
        switch(m_rootBlock.getStoreType()) {
        case RW: {
            /*
             * Write the WriteCache block on the channel.
             */
            final int nbytes = msg.getSize();
            assert data.position() == 0;
            assert data.limit() == nbytes;
            // Note: duplicate() to avoid side effects on ByteBuffer!!!
            FileChannelUtility.writeAll(reopener, data.duplicate(), m_position);
            m_position += nbytes;
        }
        case WORM: {
            /*
             * We will use the HA failover read API to recover the block from a
             * node in the quorum when we need to replay the HA log.
             */
            break;
        }
        default:
            throw new AssertionError();
        }

    }

    /**
     * Utility to return a ByteBuffer containing the external version of the
     * object.
     * 
     * @return The {@link ByteBuffer}. The position will be zero. The limit will
     *         be the #of bytes in the serialized object.
     */
    private ByteBuffer bufferObject(final Object obj) throws IOException {

        // Note: pos=0; limit=capacity=length.
        return ByteBuffer.wrap(SerializerUtil.serialize(obj));

    }

	/**
     * Close the file (does not flush).
     */
    private void close() throws IOException {
        try {
            if (m_channel != null) {
                m_channel.close();
            }
        } finally {
            reset();
        }
    }

    /**
     * Clear internal fields.
     */
    private void reset() {
        
        m_log = null;
        
        m_raf = null;
        
        m_channel = null;
        
        m_position = headerSize0;
        
        m_rootBlock = null;

        m_nextSequence = 0L;
   
    }

    /**
     * When the HA leader commits it must flush the log
     */
    private void flush() throws IOException {

        if (m_channel != null) {
        
            m_channel.force(true);
            
        }
        
    }

    /**
     * On various error conditions we may need to remove the log
     * 
     * @throws IOException
     */
    private void remove() throws IOException {

        try {

            if (m_channel != null) {

                /*
                 * Conditional remove iff file is open. Will not remove
                 * something that has been closed.
                 */

                m_channel.close();

                if (m_log.exists() && !m_log.delete()) {

                    /*
                     * It is a problem if a file exists and we can not delete
                     * it. We need to be able to remove the file and replace it
                     * with a new file when we log the write set for this commit
                     * point.
                     */
                    
                    throw new IOException("Could not delete: " + m_log);

                }
                
            }

        } finally {

            reset();
            
        }
        
    }

    /**
     * Disable the current log file if one is open.
     */
    public void disable() throws IOException {
        
        if (haLog.isInfoEnabled())
            haLog.info("");

        /*
         * Remove unless log file was already closed.
         */

        remove();
        
    }

}
