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
 * Created on Jan 25, 2012
 */

package com.bigdata.journal;

import java.io.IOException;
import java.rmi.RemoteException;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;

/**
 * Delegation pattern for an {@link ITransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DelegateTransactionService implements
        IDistributedTransactionService {

    private final IDistributedTransactionService proxy;

    public DelegateTransactionService(final IDistributedTransactionService proxy) {
        if (proxy == null)
            throw new IllegalArgumentException();
        this.proxy = proxy;
    }

    @Override
    public UUID getServiceUUID() throws IOException {
        return proxy.getServiceUUID();
    }

    @Override
    public String getServiceName() throws IOException {
        return proxy.getServiceName();
    }

    @Override
    public Class getServiceIface() throws IOException {
        return proxy.getServiceIface();
    }

    @Override
    public String getHostname() throws IOException {
        return proxy.getHostname();
    }

    @Override
    public void destroy() throws RemoteException {
        proxy.destroy();
    }

    @Override
    public long nextTimestamp() throws IOException {
        return proxy.nextTimestamp();
    }

    @Override
    public long prepared(long tx, UUID dataService) throws IOException,
            InterruptedException, BrokenBarrierException {
        return proxy.prepared(tx, dataService);
    }

    @Override
    public void notifyCommit(long commitTime) throws IOException {
        proxy.notifyCommit(commitTime);
    }

    @Override
    public long newTx(long timestamp) throws IOException {
        return proxy.newTx(timestamp);
    }

    @Override
    public long getReleaseTime() throws IOException {
        return proxy.getReleaseTime();
    }

    @Override
    public long getLastCommitTime() throws IOException {
        return proxy.getLastCommitTime();
    }

    @Override
    public void declareResources(long tx, UUID dataService, String[] resource)
            throws IOException {
        proxy.declareResources(tx, dataService, resource);
    }

    @Override
    public boolean committed(long tx, UUID dataService) throws IOException,
            InterruptedException, BrokenBarrierException {
        return proxy.committed(tx, dataService);
    }

    @Override
    public long commit(long tx) throws ValidationError, IOException {
        return proxy.commit(tx);
    }

    @Override
    public void abort(long tx) throws IOException {
        proxy.abort(tx);
    }

}
