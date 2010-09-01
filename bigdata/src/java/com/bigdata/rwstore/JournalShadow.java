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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.bigdata.journal.AbstractJournal;
import com.bigdata.journal.IJournal;
import com.bigdata.journal.JournalDelegate;
import com.bigdata.journal.RWStrategy;

/**
 * A JournalShadow wraps a Journal as a JournalDelegate but provides itself
 * as the allocation context to be passed through to any interested
 * BufferStrategy.
 * 
 * This is the path by which RWStore allocators are provided the context for
 * the allocations and deletes made
 * 
 * @author Martyn Cutcher
 *
 */
public class JournalShadow extends JournalDelegate implements IAllocationContext {
	static AtomicLong s_idCounter = new AtomicLong(23);
	int m_id = (int) s_idCounter.incrementAndGet();
	
	private JournalShadow(AbstractJournal source) {
		super(source);
	}

	public long write(ByteBuffer data) {
		return delegate.write(data, this);
	}

	public long write(ByteBuffer data, long oldAddr) {
		return delegate.write(data, oldAddr, this);
	}

	public void delete(long oldAddr) {
		delegate.delete(oldAddr, this);
	}

	public int compareTo(Object o) {
		if (o instanceof JournalShadow) {
			JournalShadow js = (JournalShadow) o;
			return m_id - js.m_id;
		} else {
			return -1;
		}
	}

	/**
	 * TODO: should retrieve from localTransactionService or Journal
	 * properties
	 */
	public long minimumReleaseTime() {
		return 0;
	}
	
	/**
	 * Release itself from the wrapped Journal, this unlocks the allocator for
	 * the RWStore
	 */
	public void detach() {
		delegate.detachContext(this);
	}

	/**
	 * This factory pattern creates a shadow for a RWStrategy-backed Journal
	 * to support protected allocations while allowing for deletion and 
	 * re-allocation where possible.  If the Journal is not backed by a
	 * RWStrategy, then the original Journal is returned.
	 * 
	 * @param journal - the journal to be shadowed
	 * @return the shadowed journal if necessary
	 */
	public static IJournal newShadow(AbstractJournal journal) {
		if (journal.getBufferStrategy() instanceof RWStrategy) {
			return new JournalShadow(journal);
		} else {
			return journal;
		}
	}
}
