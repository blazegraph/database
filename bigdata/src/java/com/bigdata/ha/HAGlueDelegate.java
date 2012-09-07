/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.bigdata.journal.ha.HAWriteMessage;

/**
 * Delegation pattern.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HAGlueDelegate implements HAGlue {

    private final HAGlue delegate;
    
    public HAGlueDelegate(final HAGlue delegate) {
        
        if(delegate == null)
            throw new IllegalArgumentException();
        
        this.delegate = delegate;
        
    }

    public Future<Void> bounceZookeeperConnection() throws IOException {
        return delegate.bounceZookeeperConnection();
    }

    public UUID getServiceId() throws IOException {
        return delegate.getServiceId();
    }

    public Future<Boolean> prepare2Phase(boolean isRootBlock0,
            byte[] rootBlock, long timeout, TimeUnit unit) throws IOException {
        return delegate.prepare2Phase(isRootBlock0, rootBlock, timeout, unit);
    }

    public Future<byte[]> readFromDisk(long token, UUID storeId, long addr)
            throws IOException {
        return delegate.readFromDisk(token, storeId, addr);
    }

    public InetSocketAddress getWritePipelineAddr() throws IOException {
        return delegate.getWritePipelineAddr();
    }

    public byte[] getRootBlock(UUID storeId) throws IOException {
        return delegate.getRootBlock(storeId);
    }

    public Future<Void> moveToEndOfPipeline() throws IOException {
        return delegate.moveToEndOfPipeline();
    }

    public Future<Void> commit2Phase(long commitTime) throws IOException {
        return delegate.commit2Phase(commitTime);
    }

    public Future<Void> abort2Phase(long token) throws IOException {
        return delegate.abort2Phase(token);
    }

    public Future<Void> receiveAndReplicate(HAWriteMessage msg)
            throws IOException {
        return delegate.receiveAndReplicate(msg);
    }

    @Override
    public UUID getServiceUUID() throws IOException {
        return delegate.getServiceUUID();
    }

    @Override
    public Class getServiceIface() throws IOException {
        return delegate.getServiceIface();
    }

    @Override
    public String getHostname() throws IOException {
        return delegate.getHostname();
    }

    @Override
    public String getServiceName() throws IOException {
        return delegate.getServiceName();
    }

    @Override
    public void destroy() throws RemoteException {
        delegate.destroy();
    }

}
