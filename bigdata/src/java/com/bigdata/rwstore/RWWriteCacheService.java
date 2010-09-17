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

package com.bigdata.rwstore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;

import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.BufferedWrite;
import com.bigdata.io.writecache.WriteCache;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.io.writecache.WriteCache.FileChannelScatteredWriteCache;
import com.bigdata.quorum.Quorum;

/**
 * Defines the WriteCacheService to be used by the RWStore.
 * @author mgc
 *
 */
public class RWWriteCacheService extends WriteCacheService {

    protected static final Logger log = Logger.getLogger(RWWriteCacheService.class);
    
    public RWWriteCacheService(final int nbuffers, final long fileExtent,
            final IReopenChannel<? extends Channel> opener,
            final Quorum quorum) throws InterruptedException,
            IOException {

        super(nbuffers, true/* useChecksum */, fileExtent, opener,
                quorum);
    }

    /**
     * Provide default FileChannelScatteredWriteCache
     */
    @Override
    public WriteCache newWriteCache(final ByteBuffer buf,
            final boolean useChecksum,
            final boolean bufferHasData,
            final IReopenChannel<? extends Channel> opener)
            throws InterruptedException {

        final boolean highlyAvailable = getQuorum() != null
                && getQuorum().isHighlyAvailable();

        return new FileChannelScatteredWriteCache(buf, true/* useChecksum */,
        		highlyAvailable,
        		bufferHasData,
                (IReopenChannel<FileChannel>) opener, null);

    }
    
}
