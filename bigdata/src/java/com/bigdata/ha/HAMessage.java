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

package com.bigdata.ha;

import java.io.Serializable;

import com.bigdata.journal.IRootBlockView;

/**
 * An HAMessages are exchanged between Master and Slave Journals.
 * 
 * They provide the abstraction that support either direct or remote invocation.
 * 
 * The MasterJournal either applies the messages directly to the SlaveJournals or sends
 * to the SlaveJournal sockets to be aplied there.
 * 
 * 
 * 
 * @author Martyn Cutcher
 *
 */
public class HAMessage {
	public static abstract class HASlaveMessage implements Serializable {
		public abstract void apply(SlaveJournal slave);
	}
	public static class HACheckRootBlock extends HASlaveMessage {
		private static final long serialVersionUID = -1973982455741465695L;

		IRootBlockView m_rbv;
		public HACheckRootBlock(IRootBlockView rbv) {
			m_rbv = rbv;
		}
		public void apply(SlaveJournal slave) {
			slave.checkRootBlock(m_rbv);
		}
	}
	
	public static class HACommitRootBlock extends HASlaveMessage {
		private static final long serialVersionUID = -4951186447654195851L;

		IRootBlockView m_rbv;
		public HACommitRootBlock(IRootBlockView rbv) {
			m_rbv = rbv;
		}
		public void apply(SlaveJournal slave) {
			slave.commitRootBlock(m_rbv);
		}
	}
	
	public static class HAAddWrite extends HASlaveMessage {
		private static final long serialVersionUID = 2225206026110500393L;

		long m_newaddr;
		long m_oldaddr;
		byte[] m_data;
		long m_chk;
		
		public HAAddWrite(long newaddr, long oldaddr, byte[] data, long chk) {
			m_newaddr = newaddr;
			m_oldaddr = oldaddr;
			m_data = data;
			m_chk = chk;
		}
		public void apply(SlaveJournal slave) {
			slave.addWrite(m_newaddr, m_oldaddr, m_data, m_chk);
		}
	}
	
	public static class HAExtendFile extends HASlaveMessage {
		private static final long serialVersionUID = -8790602788804119840L;

		long m_extent;
		
		public HAExtendFile(long extent) {
			m_extent = extent;
		}
		public void apply(SlaveJournal slave) {
			slave.extendFile(m_extent);
		}
	}
	
	public static class HAUpdateAllocationBlock extends HASlaveMessage {
		private static final long serialVersionUID = 9092248411077927784L;

		int m_index;
		long m_addr;
		byte[] m_data;
		
		public HAUpdateAllocationBlock(int index, long addr, byte[] data) {
			m_index = index;
			m_addr = addr;
			m_data = data;
		}
		public void apply(SlaveJournal slave) {
			slave.updateAllocationBlock(m_index, m_addr, m_data);
		}
	}
	
	public static class HAUpdateMetaAllocation extends HASlaveMessage {

		private static final long serialVersionUID = 486085537980300740L;
		
		long m_addr;
		byte[] m_data;
		
		public HAUpdateMetaAllocation(long addr, byte[] data) {
			m_addr = addr;
			m_data = data;
		}
		public void apply(SlaveJournal slave) {
			slave.updateMetaAllocation(m_addr, m_data);
		}
	}
	
	public static abstract class HAMasterMessage {
		public abstract void apply(MasterJournal slave);		
	}
}
