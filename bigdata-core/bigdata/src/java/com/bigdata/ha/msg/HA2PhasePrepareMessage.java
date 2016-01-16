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
package com.bigdata.ha.msg;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.bigdata.io.ChecksumUtility;
import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.RootBlockView;
import com.bigdata.util.BytesUtil;

public class HA2PhasePrepareMessage implements IHA2PhasePrepareMessage, Serializable {

    /**
     * Note: The original {@link #serialVersionUID} was <code>1L</code> - this
     * version was never release. The {@link #serialVersionUID} was changed to
     * <code>2L</code> when adding the {@link #consensusReleaseTime} and
     * {@link #isGatherService} fields. It is not possible to roll forward from
     * the non-released version without shutting down each service before
     * allowing another commit.
     */
    private static final long serialVersionUID = 2L;

    private final IHANotifyReleaseTimeResponse consensusReleaseTime;
    private final boolean isGatherService;
    private final boolean isJoinedService;
    private final boolean isRootBlock0;
    private final byte[] rootBlock;
    private final long timeout;
    private final TimeUnit unit;

    public HA2PhasePrepareMessage(
            final IHANotifyReleaseTimeResponse consensusReleaseTime,
            final boolean isGatherService, final boolean isJoinedService,
            final IRootBlockView rootBlock, final long timeout,
            final TimeUnit unit) {
        
        if (consensusReleaseTime == null)
            throw new IllegalArgumentException();

        if (rootBlock == null)
            throw new IllegalArgumentException();

        if (timeout < 0L)
            throw new IllegalArgumentException();

        if (unit == null)
            throw new IllegalArgumentException();

        this.consensusReleaseTime = consensusReleaseTime;
        
        this.isGatherService = isGatherService;
        
        this.isJoinedService = isJoinedService;
        
        this.isRootBlock0 = rootBlock.isRootBlock0();
        
        /*
         * Convert to a byte[].
         * 
         * Note: This does NOT preserve the isRootBlock0 flag!
         */
        this.rootBlock = BytesUtil.getBytes(rootBlock.asReadOnlyBuffer());
        
        this.timeout = timeout;
        
        this.unit = unit;
        
    }


    @Override
    public IHANotifyReleaseTimeResponse getConsensusReleaseTime() {
        return consensusReleaseTime;
    }

    @Override
    public boolean isGatherService() {
        return isGatherService;
    }

    @Override
    public boolean isJoinedService() {
        return isJoinedService;
    }
    
    @Override
    public boolean isRootBlock0() {
        return isRootBlock0;
    }

    @Override
    public IRootBlockView getRootBlock() {

        return new RootBlockView(isRootBlock0, ByteBuffer.wrap(rootBlock),
                new ChecksumUtility());

    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public TimeUnit getUnit() {
        return unit;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Returns <code>false</code> by default
     */
    @Override
    public boolean voteNo() {

        return false;
        
    }
    
    @Override
    public String toString() {
        return super.toString()+"{"//
                +"consensusReleaseTime="+getConsensusReleaseTime()//
                +",isGatherService="+isGatherService()//
                +",isPrepareService="+isJoinedService()//
                +",isRootBlock0="+isRootBlock0()//
                +",rootBlock()="+getRootBlock()//
                +",timeout="+getTimeout()//
                +",unit="+getUnit()//
                +"}";
    }
    
}
