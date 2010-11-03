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
    public IRootBlockView rootBlock0;

    /**
     * The 2nd root block.
     */
    public IRootBlockView rootBlock1;

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
        if (alternateRootBlock)
            log.warn("Using alternate root block");
        /*
         * Choose the root block based on the commit counter.
         * 
         * Note: The commit counters MAY be equal. This will happen if we
         * rollback the journal and override the current root block with the
         * alternate root block.
         */
        final long cc0 = rootBlock0 == null ? -1L : rootBlock0
                .getCommitCounter();
        final long cc1 = rootBlock1 == null ? -1L : rootBlock1
                .getCommitCounter();
        this.rootBlock = (cc0 > cc1 ? (alternateRootBlock ? rootBlock1
                : rootBlock0) : (alternateRootBlock ? rootBlock0 : rootBlock1));
    }

}
