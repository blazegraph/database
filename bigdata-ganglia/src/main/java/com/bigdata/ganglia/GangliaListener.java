/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package com.bigdata.ganglia;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.bigdata.ganglia.util.BytesUtil;
import com.bigdata.ganglia.util.DaemonThreadFactory;

/**
 * Class listens in to the Ganglia UDP protocol, decodes received messages, and
 * dispatches them to an {@link IGangliaMessageHandler}.
 * <p>
 * The decoded messages are reported without any additional translation. The
 * receiver may need to reconcile host names, {@link IGangliaMetadataMessage}s
 * with those already on hand, and/or reconcile {@link IGangliaMetricMessage}
 * with the {@link IGangliaMetadataMessage}s already on hand. The
 * {@link GangliaListener} does NOT perform such operations by design so it
 * maybe used to obtain a faithful transcription of the chatter on the ganglia
 * network.
 */
public class GangliaListener implements Callable<Void>, IGangliaDefaults {

	static private final Logger log = Logger.getLogger(GangliaListener.class);

	/** The address used by ganglia for hosts to join. */
	private final InetAddress group;

	/** The port on which the Ganglia services are talking. */
	private final int port;
	
	/**
	 * Object responsible for decoding packets.
	 */
	private final IGangliaMessageDecoder decoder;
	
	/** Messages are dispatched to this listener as they are received. */
	private final IGangliaMessageHandler handler;

	private volatile boolean listening = false;
	
    /**
     * Return <code>true</code> iff the listener is running when this method is
     * invoked.
     */
	public boolean isListening() {
	 
	    return listening;
	    
	}
	
	/**
	 * Listen at the default multicast group address and port.
	 * 
	 * @param handler
	 *            The interface to receive the ganglia messages.
	 * 
	 * @throws UnknownHostException
	 *             
	 * @see #DEFAULT_GROUP
	 * @see #DEFAULT_PORT
	 */
	public GangliaListener(final IGangliaMessageHandler handler)
			throws UnknownHostException {

		this(InetAddress.getByName(DEFAULT_GROUP), DEFAULT_PORT, 
				new GangliaMessageDecoder31(), handler);

	}

	/**
	 * Listen at the specified multicast group address and port.
	 * 
	 * @param group
	 *            The multicast address used by ganglia for hosts to join.
	 * @param port
	 *            The port on which the ganglia services are talking.
	 * @param decoder
	 *            Object responsible for decoding packets.
	 * @param handler
	 *            The interface to receive the ganglia messages.
	 */
	public GangliaListener(final InetAddress group, final int port,
			final IGangliaMessageDecoder decoder,
			final IGangliaMessageHandler handler) {

		if (group == null)
			throw new IllegalArgumentException();

		if (port <= 0)
			throw new IllegalArgumentException();

		if (handler == null)
			throw new IllegalArgumentException();
		
		if (decoder == null)
			throw new IllegalArgumentException();

		this.group = group;

		this.port = port;
	
		this.decoder = decoder;
		
		this.handler = handler;

	}

    /**
     * Listens for ganglia messages. Each message is decoded as as it is
     * received. Bad packets are dropped. Valid messages are dispatched using a
     * second thread to avoid latency in the thread which is listening to the
     * ganglia protocol.
     * <p>
     * Note: This method BLOCKS and does NOT notice an interrupt. This is
     * because JDK 6 does not support multicast NIO.
     * 
     * TODO Java 6 does not support non-blocking multicast receive. Write a JDK
     * 7 specific version of class class which uses multicast with non-blocking
     * IO and hence can be interrupted.
     * 
     * @see <a href="http://blogs.oracle.com/alanb/entry/multicasting_with_nio">
     *      Multicasting with NIO.</a>
     * 
     * @see <a
     *      href="http://docs.oracle.com/javase/7/docs/api/java/nio/channels/MulticastChannel.html">
     *      MulticastChannel </a>
     */
	@Override
	public Void call() throws Exception {

		// Socket supports multicast.
	    MulticastSocket datagramSocket = null;
	    
		// Service used to dispatch messages.
		ExecutorService service = null;

		try {

			// Allocate the socket.
			datagramSocket = new MulticastSocket(port);
			
			// Join the group.
			datagramSocket.joinGroup(group);

			// Start the executor.
			service = Executors
					.newSingleThreadScheduledExecutor(new DaemonThreadFactory(
							"GangliaListener"));

			// Buffer reused for each receive.
            final byte[] buffer = new byte[BUFFER_SIZE];

            // Construct packet for receiving data.
			final DatagramPacket packet = new DatagramPacket(buffer,
					0/* off */, buffer.length/* len */);

			listening = true;
			
			// Listen for messages.
			while (true) {

                try {

                    /*
                     * Note: This BLOCKS and does NOT notice an interrupt().
                     * 
                     * Note: In Java 6 you can not have both multicast and
                     * non-blocking receive.
                     */
                    datagramSocket.receive(packet);

                    if (Thread.interrupted()) {
                        /*
                         * Stop listening if we are interrupted.
                         */
                        break;
                    }
                    
                    // Decode the record.
                    final IGangliaMessage msg = decodeRecord(packet.getData(),
                            packet.getOffset(), packet.getLength());

					if (msg != null) {
						
						service.submit(new DispatchTask(handler, msg));
						
					}

				} catch (Throwable t) {

					log.warn(t, t);

				}

			}
			
			return (Void) null;
			
		} finally {

			if (service != null) {

				service.shutdown();

			}

			if (datagramSocket != null) {

				datagramSocket.close();
				
			}

            listening = false;
            
        }

    }

    /**
     * Decode a Ganglia message from the datagram packet.
     * 
     * @param data
     *            The packet data.
     * @param off
     *            The offset of the first byte in the packet.
     * @param len
     *            The #of bytes in the packet.
     * 
     * @return The decoded record.
     */
    protected IGangliaMessage decodeRecord(final byte[] data, final int off,
            final int len) {

		if (log.isTraceEnabled())
			log.trace(BytesUtil.toString(data, off, len));

		return decoder.decode(data, off, len);

	}
	
	/**
	 * Class dispatches a message to a handler and logs anything which is
	 * thrown.
	 */
	private static class DispatchTask implements Callable<Void> {

		private final IGangliaMessageHandler handler;
		private final IGangliaMessage msg;

		public DispatchTask(final IGangliaMessageHandler handler,
				final IGangliaMessage msg) {

			this.handler = handler;

			this.msg = msg;

		}

		public Void call() throws Exception {

			try {

				if (log.isDebugEnabled())
					log.debug(msg);
				
				// Dispatch the decoded record.
				handler.accept(msg);

			} catch (Throwable t) {

				log.warn(msg, t);

			}

			return (Void) null;

		}

	}

	/**
	 * Listens to ganglia services on the default multicast address and port and
	 * writes out the messages that it observes on stdout.
	 * 
	 * @param args
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		final IGangliaMessageHandler handler = new IGangliaMessageHandler() {

			@Override
			public void accept(final IGangliaMessage msg) {

//				if (msg.isMetricMetadata()) {
//					// Write out the message on stderr for debug.
//					System.err.println(msg.toString());
//				} else {
////					if (msg.getMetricName().equals("sys_clock")
////							|| msg.getMetricName().equals("mtu")) {
//						// Write out the message.
//						System.out.println(msg.toString());
////					}
//				}
				// Write out the message.
				System.out.println(msg.toString());
			}

		};

		final GangliaListener listener = new GangliaListener(handler);

		listener.call();

	}

}
