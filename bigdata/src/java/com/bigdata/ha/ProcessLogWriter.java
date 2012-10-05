package com.bigdata.ha;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Formatter;

import org.apache.log4j.Logger;

import com.bigdata.btree.BytesUtil;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.ha.msg.IHAWriteMessage;

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
public class ProcessLogWriter {

    /**
     * Logger for HA events.
     */
    private static final Logger haLog = Logger.getLogger("com.bigdata.haLog");

    /** HA log directory. */
    private final File m_dir;

    /**
     * The root block of the leader at the start of the current write set.
     */
    private IRootBlockView m_rootBlock;

    /** Current write cache block sequence counter. */
    private long m_sequence = 0;

    /** current log file. */
    private File m_log = null;

    public static final String HA_LOG_EXT = ".ha-log";
    
    /** current output file channel. */
    private RandomAccessFile m_raf = null;
    private FileChannel m_channel = null;
    final static int ROOTBLOCK_0 = 0;
    final static int ROOTBLOCK_1 = RootBlockView.SIZEOF_ROOT_BLOCK;
    final static int START_DATA = ROOTBLOCK_1 + RootBlockView.SIZEOF_ROOT_BLOCK;

    public ProcessLogWriter(final File logDir) {

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

        m_sequence = 0L;

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

            /*
             * Note: We use commitCounter+1 so the file will be labeled by the
             * commit point that will be achieved when that log file is applied
             * to a journal whose current commit point is [commitCounter].
             */

            final long commitCounter = rootBlock.getCommitCounter();

            f.format("%020d" + HA_LOG_EXT, (commitCounter + 1));

            logFile = sb.toString();

        }

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
        m_channel.position(START_DATA);

        // On open write rootblock to 0 and 1
        writeRootBlock(ROOTBLOCK_0, rootBlock);       
        writeRootBlock(ROOTBLOCK_1, rootBlock);       
 
    }

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

        writeRootBlock(rootBlock.isRootBlock0() ? ROOTBLOCK_0 : ROOTBLOCK_1, rootBlock);
        
        close();
        
    }

    /**
     * Writes the rootblock at the given offset, but preserves the raf offset
     * for streamed output.
     */
    private void writeRootBlock(final int writePos, final IRootBlockView rootBlock)
            throws IOException {

        if (rootBlock == null)
            throw new IllegalArgumentException();
        
        final long saveSeek = m_channel.position();
        
        try {
        	m_channel.position(writePos);
	
	        m_channel.write(rootBlock.asReadOnlyBuffer());
	
	        if (haLog.isDebugEnabled())
	            haLog.debug("wrote root block: " + rootBlock);
        } finally {
        	m_channel.position(saveSeek);
        }

    }

    /**
     * 
     * @param msg
     * @param data
     */
    public void write(final IHAWriteMessage msg, final ByteBuffer data)
            throws IOException {

        if (m_channel == null)
            return;

        /*
         * Check if this really is a valid message for this file. If it is not,
         * then close the file and return immediately
         */
        if (m_rootBlock.getCommitCounter() != msg.getCommitCounter())
            return;

        if (m_rootBlock.getLastCommitTime() != msg.getLastCommitTime())
            return;

        if (m_sequence != msg.getSequence())
            return;

        if (haLog.isInfoEnabled())
            haLog.info("msg=" + msg);
        
        // write serialized message object
        m_channel.write(bufferObject(msg));

        switch(m_rootBlock.getStoreType()) {
        case RW: {
            m_channel.write(data);
        }
        case WORM:
            break;
        }

    }

    /**
     * Utility to return a ByteBuffer containing the external version of the object
     */
    private ByteBuffer bufferObject(final Object obj) throws IOException {
        final ByteArrayOutputStream baout = new ByteArrayOutputStream();
        final ObjectOutputStream objout = new ObjectOutputStream(baout);
        
        objout.writeObject(obj);
        objout.flush();
        objout.close();
        
		return ByteBuffer.wrap(baout.toByteArray());
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
        
        m_rootBlock = null;

        m_sequence = 0L;
   
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
    
    // FIXME write utility to dump one or more log files.
    public static void main(final String[] args) {

    }

}
