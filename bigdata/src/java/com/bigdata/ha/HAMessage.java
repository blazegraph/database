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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.bigdata.journal.IRootBlockView;
import com.bigdata.journal.ha.MasterJournal;

/**
 * An HAMessages are exchanged between Master and Slave Journals.
 * 
 * They provide the abstraction that support either direct or remote invocation.
 * 
 * The MasterJournal either applies the messages directly to the SlaveJournals or sends
 * to the SlaveJournal sockets to be aplied there.
 * 
 * Messages are sent to the SlaveJournals' HASockets and applied.  Although the message is sent ot the
 * SlaveSocket, it does not wait for the reply on that socket, rather the reply is sent to the
 * MasterSocket as a message that references the original request. A Timer task can check for
 * unhandled responses.
 * 
 * TODO: Refactor to provide GenMessage that could be used for general asynchronous messaging rather
 * than just HA. GenMessages would support a general framework of message exchanges with single
 * Socket for each message receiver.
 * 
 * TODO: We would also like to be able to use the GenMessage integrated with FunctionalJava to allow
 * both threaded and unthreaded patterns.  So that the Functional patterns could scaleup to GenMessage
 * interactions.
 * 
 * TODO: Process Message concurrently
 * 
 * @author Martyn Cutcher
 *
 */
public class HAMessage {
	
	/**
	 * Generic Response object
	 */
	public static class Response implements Externalizable {
		final Integer m_srcId;
		Throwable m_exception;
		
		public Response() {
			m_srcId = 0;
		}

		public Response(Integer id) {
			m_srcId = id;
		}
		
		public Integer getSrcId() {
			return m_srcId;
		}

		public void setException(Throwable t) {
			m_exception = t;
		}

		public Throwable getException() {
			return m_exception;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}

	public static abstract class HASlaveMessage implements Externalizable {
		static final AtomicInteger s_msgId = new AtomicInteger(23);
		Integer m_msgId = null;
		
		public abstract void apply(SlaveJournal slave);
		
		Response m_response = null;
		
		/*
		 * getResponse must be overridden by any subclasses that need to return data.
		 * 
		 * The default class can be used to indicate completion of a request and contains
		 * any Exception information that may have been set.
		 */
		public Response getResponse() {
			if (m_response == null) {
				m_response = new Response(m_msgId);
			}
			
			return m_response;
		}
		
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			m_msgId = in.readInt();			
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeInt(m_msgId);
		}
	}
	
	public static class HACheckRootBlock extends HASlaveMessage {

		IRootBlockView m_rbv;
		public HACheckRootBlock() {}
		public HACheckRootBlock(IRootBlockView rbv) {
			m_rbv = rbv;
			m_msgId = s_msgId.addAndGet(1);
		}
		public void apply(SlaveJournal slave) {
			slave.checkRootBlock(m_rbv);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			
		}
	}
	
	public static class HACommitRootBlock extends HASlaveMessage {

		IRootBlockView m_rbv;
		public HACommitRootBlock() { }
		public HACommitRootBlock(IRootBlockView rbv) {
			m_rbv = rbv;
		}
		public void apply(SlaveJournal slave) {
			slave.commitRootBlock(m_rbv);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
	
	/**
	 * The big question, is how much benefit would be gained by being able to
	 * utilize direct buffers so that the deserialization could avoid reading
	 * into an intermediate buffer before writing to a "possible" direct buffer
	 * of the target Journals WriteCacheService. 
	 * 
	 * This probably comes down to the extent that the HA protocol batches up
	 * larger writes that can be processed with fewer interactions, thus doubly
	 * benefitting from the larger writes and decreased cost of message synchronization.
	 *
	 */
	public static class HAAddWrite extends HASlaveMessage {

		long m_newaddr;
		long m_oldaddr;
		ByteBuffer m_data;
		int m_chk;
		
		public HAAddWrite() { }
		
		public HAAddWrite(long newaddr, long oldaddr, ByteBuffer data, int chk) {
			m_newaddr = newaddr;
			m_oldaddr = oldaddr;
			m_data = data;
			m_chk = chk;
		}
		public void apply(SlaveJournal slave) {
			slave.addWrite(m_newaddr, m_oldaddr, m_data, m_chk);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			
			m_newaddr = in.readLong();
			m_oldaddr = in.readLong();
			m_chk = in.readInt();
			int datalen = in.readInt();
			byte[] tmp = new byte[datalen];
			in.read(tmp);
			m_data = ByteBuffer.wrap(tmp);
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			
			out.writeLong(m_newaddr);
			out.writeLong(m_oldaddr);
			out.writeInt(m_chk);
			byte[] tmp = m_data.array();
			out.writeInt(tmp.length);
			out.write(tmp);			
		}
	}
	
	public static class HAExtendFile extends HASlaveMessage {

		long m_extent;
		
		public HAExtendFile() { }
		public HAExtendFile(long extent) {
			m_extent = extent;
		}
		public void apply(SlaveJournal slave) {
			slave.extendFile(m_extent);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
	
	public static class HAUpdateAllocationBlock extends HASlaveMessage {
		private static final long serialVersionUID = 9092248411077927784L;

		int m_index;
		long m_addr;
		byte[] m_data;
		
		public HAUpdateAllocationBlock() { }
		public HAUpdateAllocationBlock(int index, long addr, byte[] data) {
			m_index = index;
			m_addr = addr;
			m_data = data;
		}
		public void apply(SlaveJournal slave) {
			slave.updateAllocationBlock(m_index, m_addr, m_data);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
	
	public static class HAUpdateMetaAllocation extends HASlaveMessage {

		long m_addr;
		byte[] m_data;
		
		public HAUpdateMetaAllocation() { }
		public HAUpdateMetaAllocation(long addr, byte[] data) {
			m_addr = addr;
			m_data = data;
		}
		public void apply(SlaveJournal slave) {
			slave.updateMetaAllocation(m_addr, m_data);
		}
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
	
	public static abstract class HAMasterMessage {
		public abstract void apply(MasterJournal slave);		
	}
}
