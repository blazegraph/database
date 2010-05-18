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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.Environment;
import com.bigdata.journal.IRootBlockView;

/**
 * The {@link HADelegate} provides the concrete implementation of the remote
 * {@link HAGlue} interface, as delegated by the {@link HADelegator} class.
 * 
 * @author Martyn Cutcher
 * 
 */
public abstract class HADelegate {

	final protected Environment environment;

	public HADelegate(final Environment environment) {
		
	    this.environment = environment;
	    
	}

	public InetSocketAddress getWritePipelineAddr() {
		
	    return environment.getWritePipelineAddr();
	    
	}

    public Environment getEnvironment() {

        return environment;
        
	}
	
	public abstract RunnableFuture<Void> abort2Phase(long token) throws IOException;

	public abstract RunnableFuture<Void> commit2Phase(final long commitTime) throws IOException;

	public abstract RunnableFuture<Boolean> prepare2Phase(final IRootBlockView rootBlock) throws IOException;

	public abstract RunnableFuture<ByteBuffer> readFromDisk(long token, long addr);

	public abstract Future<Void> receiveAndReplicate(HAWriteMessage msg) throws IOException;

}
