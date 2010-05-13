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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.bigdata.io.ObjectSocketChannelStream;
import com.bigdata.journal.ha.SocketMessage.AckMessage;

/**
 * The HAServer processes HAMessages and dispatches to a message client.
 * 
 * There are two modes of use depending on whether the control is message
 * driven or message requested.
 * 
 * If message driven then the run process will receive and apply messages.  The
 * alternative is for some other external trigger - such as RMI - to call the
 * readMessage method.
 * 
 * The HAServer communicates with the HAConnect on the other side of the socket, 
 * sending ACK messages.  When the process is message driven, the ACK is sent
 * from the message processing loop.  If he message is requested, it is the 
 * responsibility of the requestor to trigger the ACK.  However, this can be
 * a simple ACK to confirm message receipt rather than an ACK on full processing
 * since the RMI trigger will take responsibility.
 * 
 * @author Martyn Cutcher
 * 
 */
public class HAServer extends Thread {

	protected static final Logger log = Logger.getLogger(HAServer.class);

	final InetAddress addr;
	final int port;
	final IHAClient client;
	private Selector selector;
	private SelectionKey serverKey;
	private ServerSocketChannel server;

	private ObjectSocketChannelStream str;

	private boolean messageDrive;

	private ObjectInputStream m_instr;

	final private ReentrantLock inputStreamReady = new ReentrantLock();

	/**
	 * @param addr
	 *            The internet address on which the server will listen.
	 * @param port
	 *            The port that this server listens at.
	 * @param client
	 *            The object with access to the local service and the downstream
	 *            service.
	 * @param active
	 *            Determines if the process is message driven
	 * @throws IOException
	 */
	public HAServer(InetAddress addr, int port, IHAClient client, boolean messageDrive) {
		this.addr = addr;
		this.port = port;
		this.client = client;
		this.messageDrive = messageDrive;
		this.setDaemon(true);
		log.info("Created for " + addr + ":" + port);
	}

	public void run() {
		try {
			selector = Selector.open();
			server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(addr, port));
			if(log.isInfoEnabled())
			    log.info("Listening on" + addr + ":" + port);
			// server.socket().bind(new InetSocketAddress(port));
			server.configureBlocking(true);
			// serverKey = server.register(selector, SelectionKey.OP_ACCEPT);
			runBlock();
		} catch (ClosedByInterruptException cie) {
			/*
			 * Normal shutdown.
			 */
			log.info("Shutdown");
		} catch (InterruptedException t) {
			/*
			 * Normal shutdown.
			 */
			log.info("Shutdown");
		} catch (Throwable t) {
			log.error(t, t);
			throw new RuntimeException(t);
		} finally {
			if (server != null) {
				try {
					server.close();
				} catch (IOException e) {
					log.error(e, e);
				}
			}
		}
	}

    /**
     * Wait for a client connection. This only supports one client connection.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws Exception
     */
    private void runBlock() throws IOException, InterruptedException, Exception {
        while (true) {
            
            final SocketChannel client = server.accept();
            log.info("Accepted connection " + this);
            client.configureBlocking(true);

            str = new ObjectSocketChannelStream(client);
            this.client.setInputSocket(str);
            // Now process messages
            // Retrieving the input stream may be a problem for non message driven protocols since
            // it may not return until data is sent to the stream.
            inputStreamReady.lock();
            try {
            	m_instr = str.getInputStream();
            } finally {
            	inputStreamReady.unlock();
            }
            log.info("Set input stream to " + m_instr);
            if (messageDrive) {
	            while (true) {
	                if (log.isTraceEnabled())
	                    log.trace("Reading next message from " + str);
	
	                final SocketMessage msg = (SocketMessage) m_instr.readObject();
	                msg.setHAServer(this);
	
	                if (log.isTraceEnabled())
	                    log.trace("Applying " + msg.getClass().getName());
	       
					try {
						msg.apply(this.client);
					} catch (Throwable t) {
	
		                if (log.isTraceEnabled())
		                    log.trace("Propagating error", t);
	
		                msg.establishAck().setError(t);
					}
					acknowledge(msg.establishAck());
	
	                if (log.isTraceEnabled())
	                    log.trace("Message applied");
	                
	            }
            }
        }
    }
    
    public SocketMessage readMessage() throws IOException, ClassNotFoundException {
        inputStreamReady.lock();
        try {
    	if (m_instr == null) {
    		log.warn("read message request on null stream for " + this);
    		throw new IllegalStateException("readMessage request on null stream");
    	}
    	
        final SocketMessage msg = (SocketMessage) m_instr.readObject();
    	if (log.isTraceEnabled())
    		log.trace("processing message");
    	msg.setHAServer(this);
        
        return msg;
        } finally {
        	inputStreamReady.unlock();
        }
   }

	/**
	 * Note: This is an alternative implementation of {@link #runBlock()} which
	 * could be developed to handle multiple client connections.
	 */
	private void runNoBlock() {
		while (true) {
			try {
				selector.select();

				final Set<SelectionKey> keys = selector.selectedKeys();
				final Iterator<SelectionKey> iter = keys.iterator();
				while (iter.hasNext()) {
					final SelectionKey key = (SelectionKey) iter.next();
					iter.remove();

					if (key == serverKey) {
						if (key.isAcceptable()) {
							SocketChannel client = server.accept();
							if (log.isTraceEnabled())
								log.trace("Accepted connection");
							client.configureBlocking(false);
							SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
							clientKey.attach(Integer.valueOf(0));
						}
					} else {
						// Okay, not an accept so must be a READ
						SocketChannel client = (SocketChannel) key.channel();

						if (!key.isReadable())
							continue; // what was that then?

						if (log.isTraceEnabled())
							log.trace("READ selection");

						ObjectSocketChannelStream str = new ObjectSocketChannelStream(client);

						// Now process messages
						ObjectInputStream instr = str.getInputStream();
						while (true) {
							SocketMessage msg = (SocketMessage) instr.readObject();
							try {
								msg.apply(client);
							} catch (Throwable t) {
								msg.establishAck().setError(t);
							}
							acknowledge(msg.establishAck());
						}

					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
		}
	}

	public void acknowledge(final AckMessage<?, ?> ack) throws IOException {

		if (log.isTraceEnabled())
			log.trace("Sending Acknowledge " + ack + " from " + addr + ":" + port);

		final ObjectOutputStream ostr = str.getOutputStream();
		ostr.writeObject(ack);
		ostr.flush();
	}
}
