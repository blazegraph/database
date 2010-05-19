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

package com.bigdata.journal.ha;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.Environment;
import com.bigdata.journal.IRootBlockView;

/**
 * The {@link HADelegate} provides the concrete implementation of the remote
 * {@link HAGlue} interface, as delegated by the {@link HADelegator} class.
 * {@link HAGlue} is a {@link Remote} interface and its methods declare than
 * they throw {@link IOException} or {@link RemoteException}.  The methods on
 * this class have the same method signatures, but DO NOT declare these thrown
 * exceptions since they are not being accessed using RMI.
 * 
 * @author Martyn Cutcher
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public abstract class HADelegate {

	final protected Environment environment;

    private final InetSocketAddress writePipelineAddr;
    
    public Environment getEnvironment() {

        return environment;
        
    }
    
	public HADelegate(final Environment environment) {
		
	    this.environment = environment;
	    
        try {
            writePipelineAddr =  new InetSocketAddress(getPort(0));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

	}

	public InetSocketAddress getWritePipelineAddr() {
		
	    return writePipelineAddr;
	    
	}

    protected int getPort(int suggestedPort) throws IOException {
        ServerSocket openSocket;
        try {
            openSocket = new ServerSocket(suggestedPort);
        } catch (BindException ex) {
            // the port is busy, so look for a random open port
            openSocket = new ServerSocket(0);
        }
        final int port = openSocket.getLocalPort();
        openSocket.close();
        return port;
    }

	public abstract RunnableFuture<Void> abort2Phase(long token);

	public abstract RunnableFuture<Void> commit2Phase(final long commitTime);

	public abstract RunnableFuture<Boolean> prepare2Phase(final IRootBlockView rootBlock);

	public abstract RunnableFuture<ByteBuffer> readFromDisk(long token, long addr);

    /**
     * @param msg
     * @return
     * @throws IOException
     *             This method uses RMI to talk with the downstream node and can
     *             throw {@link IOException}s arising from the RMI.
     */
    public abstract Future<Void> receiveAndReplicate(HAWriteMessage msg)
            throws IOException;

    public abstract RunnableFuture<Void> create(final IRootBlockView rootBlock);
    
}
