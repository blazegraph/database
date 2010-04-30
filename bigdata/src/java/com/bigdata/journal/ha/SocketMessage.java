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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.io.WriteCache;
import com.bigdata.io.WriteCache.RecordMetadata;
import com.bigdata.journal.ha.SocketMessage.HAWriteMessage.HAWriteConfirm;

/**
 * SocketMessage is a control message that is input from a Socket stream.
 * Significantly, once built it can continue to read from the stream, so the message
 * data is not considered to be contined within the message. 
 * 
 * This approach facillitates chaining of data, for example, a client may provide an
 * output stream to which data could be copied from the input.
 * 
 * An HAAcknowledge message is returned for each message processed if requested.  A
 * message handler can be registered to process the response which can be awaited by
 * waiting on the handler that will signal itself when the callback is made:
 * 
 * HAWriteMessage msg;
 * msg.register(handler); // new IWriteCallback() {.....}
 * connect.send(msg); // HAConnect
 * msg.await();
 * 
 * Since the message is sent from another thread, the caller has a choice of three options:
 * 1) Simply fire and forget, possibly relying on another async callback process
 * 2) Register a handler to be notified by an Ack message
 * 3) and/or wait for a signal to be sent to the original message object by the Ack message
 * 
 * For a Write message the classes of interest are:
 * HAWriteMessage - the message sent to via the HAConnect to a port server by HAServer
 * HAWriteConfirm - returned by HAWriteMessage.apply to the HAConnect sender
 * IWriteCallback - - optionally associated with original HAWriteMessage
 * 
 * For a Truncate message the classes of interest are:
 * HATruncateMessage - the message sent to via the HAConnect to a port server by HAServer
 * HATruncateConfirm - returned by HATruncateMessage.apply to the HAConnect sender
 * ITruncateCallback - - optionally associated with original HATruncateMessage
 * 
 * @author Martyn Cutcher
  */

public abstract class SocketMessage<T> implements Externalizable {
	static protected AtomicLong ids = new AtomicLong(0);
	
	protected static final Logger log = Logger.getLogger(SocketMessage.class);
	
	long id;
	
	void setId() {
		id = ids.incrementAndGet();
	}
	
	ReentrantLock completeLock = new ReentrantLock();
	Condition waitCondition = completeLock.newCondition();
	
	public void await() throws InterruptedException {
		completeLock.lockInterruptibly();
		try {
			waitCondition.await();
		} finally {
			completeLock.unlock();
		}
	}
	
	protected void ackNotify() throws InterruptedException {
		completeLock.lockInterruptibly();
		try {
			waitCondition.signalAll();
		} finally {
			completeLock.unlock();
		}
	}
	
	Object handler = null;

	private HAServer server;
	void setHandler(Object handler) {
		this.handler = handler;
	}

	public abstract void apply(T client);
	
//	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		id = in.readLong();
		
		System.out.println("Reading msg ID: " + id);
	}

//	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		System.out.println("Writing msg ID: " + id);
		
		out.writeLong(id);
	}
	
	interface AckHandler { }
	
	/**
	 * The AckMessage is returned to the sender and is twinned with its message source.
	 * The HAConnect manages this twinning process, and after calling "apply" if a client
	 * is registered, will signal the twinned message to awake any control thread awaiting
	 * the message completion. 
	 */
	static abstract class AckMessage<T,M extends SocketMessage<?>> extends SocketMessage<T> {
		
		M src;
		public long twinId;
		
		public void setMessageSource(SocketMessage<?> socketMessage) {
			this.src = (M) socketMessage;
		}
		public M getMessageSource() {
			return src;
		}
		
		/**
		 * Method delegation enables AckMessage to be treated generically and then call type specific method.
		 * @param client
		 */
		@SuppressWarnings("unchecked")
		public void processAck() {
			try {
				apply((T) src.handler);
			} finally {
				try {
					System.out.println("Calling ackNotify: " + this);
					src.ackNotify();
				} catch (InterruptedException e) {
					e.printStackTrace(); // FIXME: not sure what to do here in general
				}
			}
		}
				
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
			
			twinId = in.readLong();
		}

//		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			
			out.writeLong(twinId);
		}
	}

		/**
	 * The HAWriteMessage transmits a WriteCache buffer, the record map, serialized as a set of
	 * <offset, address, length> tuples, is included in the message.
	 * 
	 * The message will pass data on to the next service in the chain if present, and write data
	 * to the WriteCache of the local service.
	 */
	public static class HAWriteMessage extends SocketMessage<IHAClient> {
		WriteCache wc;
		
		public HAWriteMessage() {}
		
		public HAWriteMessage(WriteCache wc) {
			if (wc == null) {
				throw new IllegalArgumentException("Null WriteCache");
			}
			this.wc = wc;
			setId();
		}

		@Override
        public void send(ObjectSocketChannelStream ostr) throws IOException, InterruptedException {

		    if (wc == null) {
                throw new IllegalStateException(
                        "send cannot be called with no WriteCache");
            }

            ostr.getOutputStream().writeObject(this);

            wc.sendTo(ostr);

        }
		
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
			super.readExternal(in);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
		}

		/**
		 * For the WriteMessage
		 */
		public void apply(IHAClient client) {
			
			HAWriteConfirm ack = new HAWriteConfirm(id);
			
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
				
				acknowledge(ack);
			} catch (Exception ioe) {
				ioe.printStackTrace();
			}			
			log.info("HAWritemessage apply: done");
		}
		
		public interface IWriteCallback {

			void ack(HAWriteConfirm writeConfirm);
			
		}
		
		static class HAWriteConfirm extends AckMessage<IWriteCallback,HAWriteMessage> {
			
			public HAWriteConfirm() {} // for deserialization

			public HAWriteConfirm(long twinid) {
				this.twinId = twinid;
				setId();
			}

			@Override
			public void apply(IWriteCallback client) {
				if (client != null) {
					client.ack(this);
				}
			}			
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
			setId();
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
			super.readExternal(in);
			
			extent = in.readLong();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			super.writeExternal(out);
			
			log.info("HATruncateMessagem writeExternal");
			out.writeLong(extent);
		}

		/**
		 * For the WriteMessage
		 */
		public void apply(IHAClient client) {			
			HATruncateConfirm ack = new HATruncateConfirm(id);

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
				
				acknowledge(ack);
			} catch (Exception ioe) {
				ioe.printStackTrace();
			}			
		}
	}
	
	public interface ITruncateCallback {

		void ack(HATruncateConfirm writeConfirm);
		
	}
	
	static class HATruncateConfirm extends AckMessage<ITruncateCallback,HAWriteMessage> {
		
		public HATruncateConfirm() {} // for deserialization

		public HATruncateConfirm(long twinid) {
			this.twinId = twinid;
			setId();
		}

		@Override
		public void apply(ITruncateCallback client) {
			if (client != null) {
				client.ack(this);
			}
		}			
	}

    public void send(ObjectSocketChannelStream ostr) throws IOException,
            InterruptedException {

        ostr.writeObject(this);
        
	}

	public void acknowledge(AckMessage<?,?> ack) throws IOException {
		if (server == null) {
			throw new IllegalStateException("No HASerevr set for this message");
		}
		server.acknowledge(ack);
	}

	public void setHAServer(HAServer server) {
		this.server = server;
	}
	
}
