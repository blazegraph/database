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
import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import com.bigdata.ha.msg.IHA2PhaseAbortMessage;
import com.bigdata.ha.msg.IHA2PhaseCommitMessage;
import com.bigdata.ha.msg.IHA2PhasePrepareMessage;
import com.bigdata.ha.msg.IHADigestRequest;
import com.bigdata.ha.msg.IHADigestResponse;
import com.bigdata.ha.msg.IHAGlobalWriteLockRequest;
import com.bigdata.ha.msg.IHALogDigestRequest;
import com.bigdata.ha.msg.IHALogDigestResponse;
import com.bigdata.ha.msg.IHALogRequest;
import com.bigdata.ha.msg.IHALogRootBlocksRequest;
import com.bigdata.ha.msg.IHALogRootBlocksResponse;
import com.bigdata.ha.msg.IHAReadRequest;
import com.bigdata.ha.msg.IHAReadResponse;
import com.bigdata.ha.msg.IHARebuildRequest;
import com.bigdata.ha.msg.IHARootBlockRequest;
import com.bigdata.ha.msg.IHARootBlockResponse;
import com.bigdata.ha.msg.IHASyncRequest;
import com.bigdata.ha.msg.IHAWriteMessage;
import com.bigdata.ha.msg.IHAWriteSetStateRequest;
import com.bigdata.ha.msg.IHAWriteSetStateResponse;
import com.bigdata.journal.ValidationError;

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

    public Future<Boolean> prepare2Phase(IHA2PhasePrepareMessage msg)
            throws IOException {
        return delegate.prepare2Phase(msg);
    }

    public Future<IHAReadResponse> readFromDisk(
            IHAReadRequest readMessage) throws IOException {
        return delegate.readFromDisk(readMessage);
    }

    public InetSocketAddress getWritePipelineAddr() throws IOException {
        return delegate.getWritePipelineAddr();
    }

    public IHARootBlockResponse getRootBlock(IHARootBlockRequest msg) throws IOException {
        return delegate.getRootBlock(msg);
    }

    public Future<Void> moveToEndOfPipeline() throws IOException {
        return delegate.moveToEndOfPipeline();
    }

    public Future<Void> commit2Phase(IHA2PhaseCommitMessage commitMessage)
            throws IOException {
        return delegate.commit2Phase(commitMessage);
    }

    public Future<Void> abort2Phase(IHA2PhaseAbortMessage abortMessage)
            throws IOException {
        return delegate.abort2Phase(abortMessage);
    }

    public Future<Void> receiveAndReplicate(final IHASyncRequest req,
            final IHAWriteMessage msg) throws IOException {
        return delegate.receiveAndReplicate(req, msg);
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

    @Override
    public long nextTimestamp() throws IOException {
        return delegate.nextTimestamp();
    }

    @Override
    public long newTx(long timestamp) throws IOException {
        return delegate.newTx(timestamp);
    }

    @Override
    public long commit(long tx) throws ValidationError, IOException {
        return delegate.commit(tx);
    }

    @Override
    public void abort(long tx) throws IOException {
        delegate.abort(tx);
    }

    @Override
    public void notifyCommit(long commitTime) throws IOException {
        delegate.notifyCommit(commitTime);
    }

    @Override
    public long getLastCommitTime() throws IOException {
        return delegate.getLastCommitTime();
    }

    @Override
    public long getReleaseTime() throws IOException {
        return delegate.getReleaseTime();
    }

    @Override
    public IHALogRootBlocksResponse getHALogRootBlocksForWriteSet(
            IHALogRootBlocksRequest msg) throws IOException {
        return delegate.getHALogRootBlocksForWriteSet(msg);
    }

    @Override
    public Future<Void> sendHALogForWriteSet(IHALogRequest msg)
            throws IOException {
        return delegate.sendHALogForWriteSet(msg);
    }

    @Override
    public int getNSSPort() throws IOException {
        return delegate.getNSSPort();
    }

    @Override
    public RunState getRunState() throws IOException {
        return delegate.getRunState();
    }

    @Override
    public Future<Void> sendHAStore(IHARebuildRequest msg) throws IOException {
        return delegate.sendHAStore(msg);
    }

    @Override
    public IHADigestResponse computeDigest(final IHADigestRequest req)
            throws IOException, NoSuchAlgorithmException, DigestException {
        return delegate.computeDigest(req);
    }

    @Override
    public IHALogDigestResponse computeHALogDigest(final IHALogDigestRequest req)
            throws IOException, NoSuchAlgorithmException, DigestException {
        return delegate.computeHALogDigest(req);
    }

    @Override
    public Future<Void> globalWriteLock(final IHAGlobalWriteLockRequest req)
            throws IOException, TimeoutException, InterruptedException {
        return delegate.globalWriteLock(req);
    }

    @Override
    public IHAWriteSetStateResponse getHAWriteSetState(
            IHAWriteSetStateRequest req) {
        return delegate.getHAWriteSetState(req);
    }

}
