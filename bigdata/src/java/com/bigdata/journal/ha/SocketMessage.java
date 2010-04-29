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

import org.apache.log4j.Logger;

import com.bigdata.io.ObjectSocketChannelStream;
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

	protected static final Logger log = Logger.getLogger(SocketMessage.class);

	public void apply(T client) {
		
	}
	
//	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	}

//	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
	}
	
	/**
	 * The HAWriteMessage transmits a WriteCache buffer, the record map, serialized as a set of
	 * <offset, address, length> tuples, is included in the message.
	 * 
	 * The message will pass data on to the next service in the chain if present, and write data
	 * to the WriteCache of the local service.
	 */
	static class HAWriteMessage extends SocketMessage<IHAClient> {
		WriteCache wc;
		
		public HAWriteMessage() {}
		
		public HAWriteMessage(WriteCache wc) {
			if (wc == null) {
				throw new IllegalArgumentException("Null WriteCache");
			}
			this.wc = wc;
		}
		
		public void send(ObjectSocketChannelStream ostr) {
			if (wc == null) {
				throw new IllegalStateException("send cannot be called with no WriteCache");
			}

			try {
				ostr.getOutputStream().writeObject(this);
				
				wc.sendTo(ostr);
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
		}

		/**
		 * For the WriteMessage
		 */
		public void apply(IHAClient client) {
			ObjectSocketChannelStream in = client.getInputSocket(); // retrieve input stream
			
			wc = client.getWriteCache();
			ObjectSocketChannelStream out = client.getNextSocket();
			
			if (out != null) {
				try {
					// send this message object
					out.getOutputStream().writeObject(this);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			try {
				log.info("Calling receiveAndForward");
				wc.receiveAndForward(in, out);
			} catch (Exception ioe) {
				ioe.printStackTrace();
			}			
			log.info("HAWritemessage apply: done");
		}
	}

	/**
	 * The HATruncateMessage send a request to truncate the file on the SocketStream.
	 * 
	 * The message will pass data on to the next service in the chain if present.
	 */
	static class HATruncateMessage extends SocketMessage<IHAClient> {
		long extent;
		
		public HATruncateMessage() {}
		
		public HATruncateMessage(long extent) {
			this.extent = extent;
		}
		
		public void send(ObjectSocketChannelStream ostr) {
			try {
				System.out.println("HATruncateMessage send");
				ostr.getOutputStream().writeObject(this);				
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (IllegalStateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			extent = in.readLong();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			log.info("HATruncateMessagem writeExternal");
			out.writeLong(extent);
		}

		/**
		 * For the WriteMessage
		 */
		public void apply(IHAClient client) {			
			ObjectSocketChannelStream out = client.getNextSocket();
			
			if (out != null) {
				try {
					// send this message object
					out.getOutputStream().writeObject(this);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}				
			}
			try {
				System.out.println("Truncating file " + extent);
				log.info("Truncating file");
				client.truncate(extent);
			} catch (Exception ioe) {
				ioe.printStackTrace();
			}			
		}
	}
	
	public void send(ObjectOutputStream ostr) {
		try {
			ostr.writeObject(this);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
}
