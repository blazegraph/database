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
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.journal.ha.SocketMessage.AckMessage;

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
    
    protected static final Logger log = Logger.getLogger(HAConnect.class);

	final private ConcurrentHashMap<Long, SocketMessage<?>> m_msgs = new ConcurrentHashMap<Long, SocketMessage<?>>();
	
	private final InetSocketAddress inetSocketAddress;

	private ObjectSocketChannelStream m_out = null;

    /**
     * Create a connection to a downstream service - you MUST {@link #start()}
     * the thread.
     * 
     * @param inetSocketAddress
     * @throws IOException
     */
    public HAConnect(final InetSocketAddress inetSocketAddress)
            throws IOException {

        this.inetSocketAddress = inetSocketAddress;
        
        try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// just a delay to allow server to start in debug;
		} // FIXME: remove
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

        SocketChannel socketChannel = null;
        try {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);

            // Kick off connection establishment
            // socketChannel.connect(new InetSocketAddress("localhost", port));
            log.info("Connecting to " + inetSocketAddress);
            socketChannel.connect(inetSocketAddress);
            socketChannel.finishConnect();
            // Thread.sleep(2000);

            // and set the output stream
            m_out = new ObjectSocketChannelStream(wrapChannel(socketChannel));

            final ObjectInputStream instr = m_out.getInputStream();
            while (true) {

                final AckMessage<?, ?> msg = (AckMessage<?, ?>) instr
                        .readObject();

                if (log.isTraceEnabled())
                    log.trace("Acknowledging " + msg.getClass().getName());

                final SocketMessage<?> twin = m_msgs.get(msg.twinId);
                msg.setMessageSource(twin);
                msg.processAck();
                if (twin != null) {
                    m_msgs.remove(msg.twinId);
                } else {
                	log.warn("Twin not found for message: " + msg.twinId);
                }

                if (log.isTraceEnabled())
                    log.trace("Message acknowledged");
            }

        } catch (ClosedByInterruptException cie) {
        	// standard close
        } catch (Throwable e) {
            
            log.error("Problem with connection on: " + inetSocketAddress ,e);
            
            throw new RuntimeException(e);
            
        } finally {

            log.info("Shutdown");
            if (socketChannel != null) {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    log.error(e,e);
                }
            }
            m_out = null;
            
        }

    }

    /**
     * Store the message in the msgs map and send it downstream. The msg.id is
     * used later to retrieve the message to twin with the Ack
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    public void send(SocketMessage<?> msg) throws IOException,
            InterruptedException {

        send(msg, false/* wait */);

	}

    public void send(SocketMessage<?> msg, boolean wait) throws IOException,
            InterruptedException {

    	if (log.isTraceEnabled())
    		log.trace("sending message: " + msg + ", wait: " + wait);
    	
        if (m_out == null)
            throw new IllegalStateException("Not running?");

        m_msgs.put(msg.id, msg);
		
		msg.send(m_out);
		
		if (wait) {
			try {
				msg.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
    
    public ObjectOutputStream getOutputStream() {
    	return m_out.getOutputStream();
    }
}
