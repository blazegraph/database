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

package com.bigdata.journal;

import java.nio.ByteBuffer;

import com.bigdata.btree.IndexInconsistentError;

/**
 * Provides the callback to save the previous root block and store the address
 * with the current CommitRecord.  This enables access to historical root blocks
 * since the next CommitRecord is accessible from the CommitRecordIndex.  This
 * is effective if slightly circuitous.
 *  
 * @author Martyn Cutcher
 *
 */
public class RootBlockCommitter implements ICommitter {
    
	final private AbstractJournal journal;
	private volatile Throwable error = null;

	public RootBlockCommitter(final AbstractJournal journal) {
		this.journal = journal;
	}
	
	/**
	 * Write the current root block to the Journal and return its address
	 * to be stored in the CommitRecord.
	 */
	@Override
	public long handleCommit(final long commitTime) {
		
        if (error != null)
            throw new IndexInconsistentError(error);

	    final IRootBlockView view = journal.getRootBlockView();
		
		final ByteBuffer rbv = view.asReadOnlyBuffer();
		/*
		 * FIXME There is an API issue with the RWStore which does not allow
		 * us to pass in a read-only buffer.  Write unit tests for this on
		 * the core IRawStore test suite and fix the RWStore.  Also write
		 * unit tests when the array backing the ByteBuffer can be accessed
		 * but has a non-zero array offset (a mutable slice of a ByteBuffer).
		 */
//		return journal.write(rbv);
		final ByteBuffer bb = ByteBuffer.allocate(rbv.capacity());
		for (int i = 0; i < rbv.capacity(); i++) {
			bb.put(rbv.get());
		}
		bb.flip();
		
		return journal.write(bb);
	}

    @Override
    public void invalidate(final Throwable t) {

        if (t == null)
            throw new IllegalArgumentException();

        if (error == null)
            error = t;

    }
    
}
