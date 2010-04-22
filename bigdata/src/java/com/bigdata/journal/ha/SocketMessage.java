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

import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCache.RecordMetadata;

/**
 * SocketMessage is a control message that is input from a Socket stream.
 * Significantly, once built it can continue to read from the stream, so the message
 * data is not considered to be contined within the message. 
 * 
 * This approach facillitates chaining of data, for example, a client may provide an
 * output stream to which data could be copied from the input.
 * 
 * @author Martyn Cutcher
  */

public abstract class SocketMessage<T> implements Externalizable {

	public void apply(T client) {
		
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * The HAWriteMessage transmits a WriteCache buffer, the record map, serialized as a set of
	 * <offset, address, length> tuples, is included in the message.
	 * 
	 * The message will pass data on to the next service in the chain if present, and write data
	 * to the WriteCache of the local service.
	 */
	static class HAWriteMessage extends SocketMessage<IFoo> {
		Collection<RecordMetadata> map;
		WriteCache wc;
		
		public HAWriteMessage() {}
		
		public HAWriteMessage(WriteCache wc) {
			this.wc = wc;
		}

		public void setRecordMap(Collection<RecordMetadata> map) {
			this.map = map;
			
			// TODO: ensure sort map on offset of entries - for DiskWORM there should be but a single entry
		}
		
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			map = new ArrayList<RecordMetadata>();
			
			int entries = in.readInt();
			for (int i = 0; i < entries; i++) {
				long fileOffset = in.readLong();
				int bufferOffset = in.readInt();
				int recordLength = in.readInt();
				
				map.add(new RecordMetadata(fileOffset, bufferOffset, recordLength));
			}
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			if (map == null) {
				throw new IllegalStateException("HAWriteMessage expects valid RecordMetaData");
			}
			out.writeInt(map.size());
			Iterator<RecordMetadata> entries = map.iterator();
			while (entries.hasNext()) {
				RecordMetadata entry = entries.next();
				
				out.writeLong(entry.fileOffset);
				out.writeInt(entry.bufferOffset);
				out.writeInt(entry.recordLength);
			}
		}

		public void apply(IFoo client) {
			ObjectInputStream in = client.getInputStream(); // retrieve input stream
			
			WriteCache wc = client.getWriteCache();
			ObjectOutputStream out = client.getNextStream();
			
			if (out != null) {
				try {
					// send this message object
					out.writeObject(this);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			try {
				wc.setRecordMap(map);
				
				int len = in.readInt();
				if (len > 0) {
					if (out != null) {
						out.writeInt(len);
					}
					int bytesRem = len;
					int rdlen = 2048;
					byte[] buf = new byte[rdlen];
					while (bytesRem > 0) {
						if (bytesRem > rdlen) {
							rdlen = bytesRem;
						}
						
						int chklen = in.read(buf, 0, rdlen);
						assert(chklen == rdlen);
						
						if (out != null) {
							out.write(buf, 0, rdlen);
						}
						
						bytesRem -= rdlen;
					}
				}
			} catch (IOException ioe) {
				
			}			
		}
	}
	
}
