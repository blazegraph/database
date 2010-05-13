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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.IRootBlockView;

/**
 * A delegate pattern is used to separate the implentaiton from the Remote HAGlue interface.
 * This enables the HADelegate implementation class to support multiple, possibly non-remote interfaces.
 * 
 * @author Martyn Cutcher
 *
 */
public class HADelegator implements HAGlue {
	
	private HADelegate delegate;
	
	public HADelegator(HADelegate delegate) {
		this.delegate = delegate;
	}

	public RunnableFuture<Void> abort2Phase(long token) throws IOException {
		return delegate.abort2Phase(token);
	}

	public RunnableFuture<Void> commit2Phase(long commitTime) throws IOException {
		return delegate.commit2Phase(commitTime);
	}

	public InetAddress getWritePipelineAddr() {
		return delegate.getWritePipelineAddr();
	}

	public int getWritePipelinePort() {
		return delegate.getWritePipelinePort();
	}

	public RunnableFuture<Boolean> prepare2Phase(IRootBlockView rootBlock) throws IOException {
		return delegate.prepare2Phase(rootBlock);
	}

	public RunnableFuture<ByteBuffer> readFromDisk(long token, long addr) throws IOException {
		return delegate.readFromDisk(token, addr);
	}

	public RunnableFuture<Void> writeCacheBuffer(long fileExtent) throws IOException {
		return delegate.writeCacheBuffer(fileExtent);
	}

}
