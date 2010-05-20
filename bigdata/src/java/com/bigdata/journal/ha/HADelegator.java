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
import java.rmi.Remote;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;

import com.bigdata.journal.IRootBlockView;

/**
 * A delegate pattern is used to separate the implementation from the
 * {@link Remote} {@link HAGlue} interface. This enables the {@link HADelegate}
 * implementation class to support multiple, possibly non-remote interfaces.
 * 
 * @author Martyn Cutcher
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class HADelegator implements HAGlue {
	
	private HADelegate delegate;
	
	public HADelegator(final HADelegate delegate) {
		this.delegate = delegate;
	}

	public RunnableFuture<Void> abort2Phase(long token) throws IOException {
		return delegate.abort2Phase(token);
	}

	public RunnableFuture<Void> commit2Phase(long commitTime) throws IOException {
		return delegate.commit2Phase(commitTime);
	}

	public InetSocketAddress getWritePipelineAddr() {
		return delegate.getWritePipelineAddr();
	}

	public RunnableFuture<Boolean> prepare2Phase(IRootBlockView rootBlock) throws IOException {
		return delegate.prepare2Phase(rootBlock);
	}

	public RunnableFuture<ByteBuffer> readFromDisk(long token, long addr) throws IOException {
		return delegate.readFromDisk(token, addr);
	}

	public Future<Void> receiveAndReplicate(final HAWriteMessage msg) throws IOException {
		return delegate.receiveAndReplicate(msg);
	}

    public IRootBlockView getRootBlock() throws IOException {
        return delegate.getRootBlock();
    }
}
