/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

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

import org.apache.log4j.Logger;

import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.util.ChecksumUtility;

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

    public RootBlockUtility(final IReopenChannel<FileChannel> opener,
            final File file, final boolean validateChecksum,
            final boolean alternateRootBlock) throws IOException {
        
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
        } catch (RootBlockException ex) {
            log.warn("Bad root block zero: " + ex);
        }
        try {
            rootBlock1 = new RootBlockView(false, tmp1, checker);
        } catch (RootBlockException ex) {
            log.warn("Bad root block one: " + ex);
        }
        if (rootBlock0 == null && rootBlock1 == null) {
            throw new RuntimeException(
                    "Both root blocks are bad - journal is not usable: " + file);
        }
        // save references.
        this.rootBlock0 = rootBlock0;
        this.rootBlock1 = rootBlock1;
        
        if(alternateRootBlock) {
            /*
             * A request was made to use the alternative root block.
             */
            if (rootBlock0 == null || rootBlock1 == null) {
                /*
                 * Note: The [alternateRootBlock] flag only makes sense
                 * when you have two to choose from and you want to
                 * choose the other one. In this case, your only choice
                 * is to use the undamaged root block.
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
         * with the alternate root block.
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
            // A choice exists, compare the timestamps.
            this.rootBlock = (cc0 > cc1 //
                    ? (alternateRootBlock ? rootBlock1 : rootBlock0) //
                    : (alternateRootBlock ? rootBlock0 : rootBlock1)//
                    );
        }
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

        final boolean validateChecksum = true;
        
        final boolean alternateRootBlock = false;
        
        final RootBlockUtility u = new RootBlockUtility(opener, file,
                validateChecksum, alternateRootBlock);
        
        System.out.println("rootBlock0: " + u.rootBlock0);
        
        System.out.println("rootBlock1: " + u.rootBlock1);

        System.out.println("Current root block is rootBlock"
                + (u.rootBlock == u.rootBlock0 ? "0" : "1"));
        
    }
    
}
