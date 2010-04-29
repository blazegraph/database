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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bigdata.io.ObjectSocketChannelStream;

/**
 * The HAServer processes HAMessages and dispatches to a message client.
 * 
 * @author Martyn Cutcher
 * 
 */
public class HAServer extends Thread {
	protected static final Logger log = Logger.getLogger(SocketMessage.class);

	final int port;
	final IHAClient client;
	Selector selector;
	SelectionKey serverKey;
	ServerSocketChannel server;

    /**
     * @param addr
     *            The internet address on which the server will listen.
     * @param port
     *            The port that this server listens at.
     * @param client
     *            The object with access to the local service and the downstream
     *            service.
     */
	public HAServer(InetAddress addr, int port, IHAClient client) {
		this.port = port;
		this.client = client;

		try {
			selector = Selector.open();
			server = ServerSocketChannel.open();
			server.socket().bind(new InetSocketAddress(addr,port));
			server.configureBlocking(true);
			// serverKey = server.register(selector, SelectionKey.OP_ACCEPT);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

    /*
     * FIXME modify to notice an interrupt() and respond by exiting run(). We
     * need this as mechanism for tearing down the HAServer.
     */
	public void run() {
		runBlock();
	}

	private void runBlock() {
		while (true) {
			try {
				SocketChannel client = server.accept();
				log.info("Accepted connection");
				client.configureBlocking(true);

				ObjectSocketChannelStream str = new ObjectSocketChannelStream(client);
				this.client.setInputSocket(str);
				// Now process messages
				ObjectInputStream instr = str.getInputStream();
				while (true) {
					System.out.println("Reading next message from " + str);
					
					SocketMessage msg = (SocketMessage) instr.readObject();
					
					System.out.println("Applying " + msg.getClass().getName());
					msg.apply(this.client);
					
					System.out.println("Message applied");					
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void runNoBlock() {
		while (true) {
			try {
				selector.select();

				Set keys = selector.selectedKeys();
				Iterator iter = keys.iterator();
				while (iter.hasNext()) {
					SelectionKey key = (SelectionKey) iter.next();
					iter.remove();

					if (key == serverKey) {
						if (key.isAcceptable()) {
							SocketChannel client = server.accept();
							System.out.println("Accepted connection");
							client.configureBlocking(false);
							SelectionKey clientKey = client.register(selector, SelectionKey.OP_READ);
							clientKey.attach(new Integer(0));
						}
					} else {
						// Okay, not an accept so must be a READ
						SocketChannel client = (SocketChannel) key.channel();

						if (!key.isReadable())
							continue; // what was that then?

						System.out.println("READ selection");

						ObjectSocketChannelStream str = new ObjectSocketChannelStream(client);

						// Now process messages
						ObjectInputStream instr = str.getInputStream();
						while (true) {
							SocketMessage msg = (SocketMessage) instr.readObject();
							msg.apply(client);
						}

					}
				}

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
