/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Nov 3, 2010
 */

package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.io.ChecksumUtility;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;

/**
 * Utility class will read both root blocks of a file and indicate which one
 * is current.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 */
public class RootBlockUtility {

    private static final Logger log = Logger.getLogger(RootBlockUtility.class);

    /**
     * The 1st root block.
     */
    public final IRootBlockView rootBlock0;

    /**
     * The 2nd root block.
     */
    public final IRootBlockView rootBlock1;

    /**
     * The current root block. For a new file, this is "rootBlock0". For an
     * existing file it is based on an examination of both root blocks.
     */
    public final IRootBlockView rootBlock;

    /**
     * 
     * @param opener
     * @param file
     * @param validateChecksum
     * @param alternateRootBlock
     *            When <code>true</code>, the alternate root block will be
     *            chosen. This flag only makes sense when you have two root
     *            blocks to choose from and you want to choose the other one.
     * @param ignoreBadRootBlock
     *            When <code>true</code>, a bad root block will be ignored.
     * 
     * @throws IOException
     * 
     * @see com.bigdata.journal.Options#ALTERNATE_ROOT_BLOCK
     * @see com.bigdata.journal.Options#IGNORE_BAD_ROOT_BLOCK
     */
    public RootBlockUtility(final IReopenChannel<FileChannel> opener,
            final File file, final boolean validateChecksum,
            final boolean alternateRootBlock, final boolean ignoreBadRootBlock)
            throws IOException {

        final ChecksumUtility checker = validateChecksum ? ChecksumUtility.threadChk
                .get()
                : null;

        final ByteBuffer tmp0 = ByteBuffer
                .allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
        final ByteBuffer tmp1 = ByteBuffer
                .allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
        FileChannelUtility.readAll(opener, tmp0, FileMetadata.OFFSET_ROOT_BLOCK0);
        FileChannelUtility.readAll(opener, tmp1, FileMetadata.OFFSET_ROOT_BLOCK1);
        tmp0.position(0); // resets the position.
        tmp1.position(0);
        IRootBlockView rootBlock0 = null, rootBlock1 = null;
        try {
            rootBlock0 = new RootBlockView(true, tmp0, checker);
            if (log.isInfoEnabled())
                log.info("rootBlock0: " + rootBlock0);
        } catch (RootBlockException ex) {
            log.error("Bad root block zero: " + ex);
        }
        try {
            rootBlock1 = new RootBlockView(false, tmp1, checker);
            if (log.isInfoEnabled())
                log.info("rootBlock1: " + rootBlock1);
        } catch (RootBlockException ex) {
            log.error("Bad root block one: " + ex);
        }
        if (rootBlock0 == null && rootBlock1 == null) {
            throw new RuntimeException(
                    "Both root blocks are bad - journal is not usable: " + file);
        }

        // save references.
        this.rootBlock0 = rootBlock0;
        this.rootBlock1 = rootBlock1;

        this.rootBlock = chooseRootBlock(rootBlock0, rootBlock1,
                ignoreBadRootBlock, alternateRootBlock);
    }
    
    public RootBlockUtility(final IRootBlockView rb0, final IRootBlockView rb1) {
        this.rootBlock0 = rb0;
        this.rootBlock1 = rb1;
        this.rootBlock = chooseRootBlock(rootBlock0, rootBlock1,
                false/* ignoreBadRootBlock */, false/* alternateRootBlock */);
    }

    /**
     * Return the chosen root block. The root block having the greater
     * {@link IRootBlockView#getCommitCounter() commit counter} is chosen by
     * default.
     * <p>
     * Note: For historical compatibility, <code>rootBlock1</code> is chosen if
     * both root blocks have the same {@link IRootBlockView#getCommitCounter()}.
     * 
     * @param rootBlock0
     *            Root block 0 (may be <code>null</code> if this root block is bad).
     * @param rootBlock1
     *            Root block 1 (may be <code>null</code> if this root block is bad).
     * 
     * @return The chosen root block.
     * 
     * @throws RuntimeException
     *             if no root block satisfies the criteria.
     */
    public static IRootBlockView chooseRootBlock(
            final IRootBlockView rootBlock0, final IRootBlockView rootBlock1) {

        return chooseRootBlock(rootBlock0, rootBlock1,
                false/* alternateRootBlock */, false/* ignoreBadRootBlock */);

    }

    /**
     * Return the chosen root block. The root block having the greater
     * {@link IRootBlockView#getCommitCounter() commit counter} is chosen by
     * default.
     * <p>
     * Note: For historical compatibility, <code>rootBlock1</code> is chosen if
     * both root blocks have the same {@link IRootBlockView#getCommitCounter()}.
     * 
     * @return The chosen root block.
     * 
     * @throws RuntimeException
     *             if no root block satisfies the criteria.
     */
    public IRootBlockView chooseRootBlock() {

        return chooseRootBlock(rootBlock0, rootBlock1,
                false/* alternateRootBlock */, false/* ignoreBadRootBlock */);

    }

    /**
     * Return the chosen root block. The root block having the greater
     * {@link IRootBlockView#getCommitCounter() commit counter} is chosen by
     * default.
     * <p>
     * Note: For historical compatibility, <code>rootBlock1</code> is chosen if
     * both root blocks have the same {@link IRootBlockView#getCommitCounter()}.
     * 
     * @param rootBlock0
     *            Root block 0 (may be <code>null</code> if this root block is
     *            bad).
     * @param rootBlock1
     *            Root block 1 (may be <code>null</code> if this root block is
     *            bad).
     * @param alternateRootBlock
     *            When <code>true</code>, the alternate root block will be
     *            chosen. This flag only makes sense when you have two root
     *            blocks to choose from and you want to choose the other one.
     * @param ignoreBadRootBlock
     *            When <code>true</code>, a bad root block will be ignored.
     * 
     * @return The chosen root block.
     * 
     * @throws RuntimeException
     *             if no root block satisfies the criteria.
     */
    public static IRootBlockView chooseRootBlock(
            final IRootBlockView rootBlock0, final IRootBlockView rootBlock1,
            final boolean alternateRootBlock,final boolean ignoreBadRootBlock) {

        final IRootBlockView rootBlock;
        
        if (!ignoreBadRootBlock
                && (rootBlock0 == null || rootBlock1 == null)) {
            /*
             * Do not permit the application to continue with a damaged
             * root block.
             */
            throw new RuntimeException(
                    "Bad root block(s): rootBlock0 is "
                            + (rootBlock0 == null ? "bad" : "ok")
                            + ", rootBlock1="
                            + (rootBlock1 == null ? "bad" : "ok"));
        }

        if (alternateRootBlock) {
        
            /*
             * A request was made to use the alternative root block.
             */
            
            if (rootBlock0 == null || rootBlock1 == null) {

                /*
                 * Note: The [alternateRootBlock] flag only makes sense when you
                 * have two root blocks to choose from and you want to choose
                 * the other one.
                 */

                throw new RuntimeException(
                        "Can not use alternative root block since one root block is damaged.");
            } else {

                log.warn("Using alternate root block");
                
            }
            
        }
        
        /*
         * Choose the root block based on the commit counter.
         * 
         * Note: The commit counters MAY be equal. This will happen if
         * we rollback the journal and override the current root block
         * with the alternate root block.  It is also true when we first
         * create a Journal.
         * 
         * Note: If either root block was damaged then that rootBlock
         * reference will be null and we will use the other rootBlock
         * reference automatically. (The case where both root blocks are
         * bad is trapped above.)
         */
        final long cc0 = rootBlock0 == null ? -1L : rootBlock0
                .getCommitCounter();

        final long cc1 = rootBlock1 == null ? -1L : rootBlock1
                .getCommitCounter();
        
        if (rootBlock0 == null) {
        
            // No choice. The other root block does not exist.
            rootBlock = rootBlock1;
            
        } else if (rootBlock1 == null) {
            
            // No choice. The other root block does not exist.
            rootBlock = rootBlock0;
            
        } else {
            
            /*
             * A choice exists, compare the commit counters.
             * 
             * Note: As a historical artifact, this logic will choose
             * [rootBlock1] when the two root blocks have the same
             * [commitCounter]. With the introduction of HA support, code in
             * AbstractJournal#setQuorumToken() now depends on this choice
             * policy to decide which root block it will take as the current
             * root block.
             */

            rootBlock = (cc0 > cc1 //
                    ? (alternateRootBlock ? rootBlock1 : rootBlock0) //
                    : (alternateRootBlock ? rootBlock0 : rootBlock1)//
                    );

        }
        
        if (log.isInfoEnabled())
            log.info("chosenRoot: " + rootBlock);

        return rootBlock;
        
    }

    /**
     * Generate the root blocks. They are for all practical purposes identical.
     * 
     * @param bufferMode
     * @param offsetBits
     * @param createTime
     * @param quorumToken
     * @param storeUUID
     */
    public RootBlockUtility(
            final BufferMode bufferMode,
            final int offsetBits,
            final long createTime, 
            final long quorumToken,
            final UUID storeUUID
            ) {

        if (bufferMode == null)
            throw new IllegalArgumentException("BufferMode is required.");

        if (createTime == 0L)
            throw new IllegalArgumentException("Create time may not be zero.");

        if (storeUUID == null)
            throw new IllegalArgumentException("Store UUID is required.");

        final ChecksumUtility checker = ChecksumUtility.threadChk.get();

        /*
         * WORM: The offset at which the first record will be written. This is
         * zero(0) since the buffer offset (0) is the first byte after the root
         * blocks.
         * 
         * RWStore: The field is ignored. The RWStore skips over a block to have
         * a good byte alignment on the file.
         */
        final long nextOffset = 0L;
        final long closeTime = 0L;
        final long commitCounter = 0L;
        final long firstCommitTime = 0L;
        final long lastCommitTime = 0L;
        final long commitRecordAddr = 0L;
        final long commitRecordIndexAddr = 0L;
        
        final StoreTypeEnum stenum = bufferMode.getStoreType();
        
        final long blockSequence = IRootBlockView.NO_BLOCK_SEQUENCE;

        rootBlock0 = new RootBlockView(true,
                offsetBits, nextOffset, firstCommitTime,
                lastCommitTime, commitCounter, commitRecordAddr,
                commitRecordIndexAddr, storeUUID, //
                blockSequence, quorumToken,//
                0L, 0L, stenum, createTime, closeTime, RootBlockView.currentVersion, checker);
        
        rootBlock1 = new RootBlockView(false,
                offsetBits, nextOffset, firstCommitTime,
                lastCommitTime, commitCounter, commitRecordAddr,
                commitRecordIndexAddr, storeUUID, //
                blockSequence, quorumToken,//
                0L, 0L, stenum, createTime, closeTime, RootBlockView.currentVersion, checker);

        this.rootBlock = rootBlock0;

    }
    
    /**
     * Dumps the root blocks for the specified file.
     * 
     * @param args
     *            <code>filename</code>
     *            
     * @throws IOException
     */
    public static void main(final String[] args) throws IOException {

        if (args.length == 0) {
            
            System.err.println("usage: <filename>");
            
            System.exit(1);
            
        }

        final File file = new File(args[0]);
        
        if (!file.exists()) {
            
            System.err.println("Not found: " + file);
            
            System.exit(1);
            
        }
        
        /**
         * Used to re-open the {@link FileChannel} in this class.
         */
        final IReopenChannel<FileChannel> opener = new IReopenChannel<FileChannel>() {

            // read-only.
            static private final String fileMode = "r";
            
            private RandomAccessFile raf = null;
            
            public String toString() {

                return file.toString();

            }

            public FileChannel reopenChannel() throws IOException {

                if (raf != null && raf.getChannel().isOpen()) {

                    /*
                     * The channel is still open. If you are allowing concurrent reads
                     * on the channel, then this could indicate that two readers each
                     * found the channel closed and that one was able to re-open the
                     * channel before the other such that the channel was open again by
                     * the time the 2nd reader got here.
                     */

                    return raf.getChannel();

                }

                // open the file.
                this.raf = new RandomAccessFile(file, fileMode);

                return raf.getChannel();

            }

        };

        // validate the root blocks using their checksums.
        final boolean validateChecksum = true;

        // option is ignored since we are not not opening the Journal.
        final boolean alternateRootBlock = false;

        // Open even if one root block is bad.
        final boolean ignoreBadRootBlock = true;
        
        final RootBlockUtility u = new RootBlockUtility(opener, file,
                validateChecksum, alternateRootBlock, ignoreBadRootBlock);
        
        System.out.println("rootBlock0: " + u.rootBlock0);
        
        System.out.println("rootBlock1: " + u.rootBlock1);

        System.out.println("Current root block is rootBlock"
                + (u.rootBlock == u.rootBlock0 ? "0" : "1"));
        
    }
    
}
