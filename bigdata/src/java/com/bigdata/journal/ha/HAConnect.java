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
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.journal.ha.SocketMessage.AckMessage;
import com.bigdata.journal.ha.SocketMessage.HATruncateMessage;

/**
 * HAConnect is the port class that enables messages to be sent to a downstream HAServer.
 * 
 * It's primary purpose is to manage the twinning of SocketMessages, so that AckMessages
 * are able to call the appropriate response handler and signal message completion.
 * 
 * Twinned messages are managed using a ConcurrentHashMap, the AckMessage holding the id of the original
 * (twin) message.
 */
public class HAConnect extends Thread {
	private ConcurrentHashMap<Long, SocketMessage<?>> m_msgs = new ConcurrentHashMap<Long, SocketMessage<?>>();
	
	private ObjectSocketChannelStream m_out;

	public HAConnect(InetSocketAddress inetSocketAddress) {
		SocketChannel socketChannel;
		try {
			socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(true);

			// Kick off connection establishment
			// socketChannel.connect(new InetSocketAddress("localhost", port));
			socketChannel.connect(inetSocketAddress);
			socketChannel.finishConnect();
			// Thread.sleep(2000);

			// and set the output stream
			m_out = new ObjectSocketChannelStream(wrapChannel(socketChannel));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Fix for deadlock bug in NIO
	 * @param channel
	 * @return
	 */
	private static ByteChannel wrapChannel(final ByteChannel channel) {
		return new ByteChannel() {
			public int write(ByteBuffer src) throws IOException {
				return channel.write(src);
			}

			public int read(ByteBuffer dst) throws IOException {
				return channel.read(dst);
			}

			public boolean isOpen() {
				return channel.isOpen();
			}

			public void close() throws IOException {
				channel.close();
			}
		};
	}

	/**
	 * The main thread processes the AckMessages
	 */
	public void run() {
		ObjectInputStream instr = m_out.getInputStream();
		while (true) {
			AckMessage<?, ?> msg;
			try {
				msg = (AckMessage<?,?>) instr.readObject();
				
				System.out.println("Acknowledging " + msg.getClass().getName());
				SocketMessage<?> twin = m_msgs.get(msg.twinId);
				msg.setMessageSource(twin);
				msg.processAck();
				if (twin != null) {
					m_msgs.remove(msg.twinId);
				}
				
				System.out.println("Message acknowledged");					
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	/**
	 * Store the message in the msgs map and send it downstream.  The msg.id is used later
	 * to retrieve the message to twin with the Ack
	 */
	public void send(SocketMessage<?> msg) {
		send(msg, false);
	}
	public void send(SocketMessage<?> msg, boolean wait) {
		m_msgs.put(msg.id, msg);
		
		System.out.println("--------------------");
		System.out.println("HAConnect: sending " + msg);
		
		msg.send(m_out);
		
		System.out.println("HAConnect: SENT");
		System.out.println("--------------------");
		
		if (wait) {
			try {
				msg.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
}
