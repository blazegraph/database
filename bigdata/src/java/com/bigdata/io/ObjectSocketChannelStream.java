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

package com.bigdata.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;

/**
 * Utility class that provides dual Channel/ObjectStream access.
 * 
 * @author Martyn Cutcher
 *
 */
public class ObjectSocketChannelStream {
	ObjectOutputStream outStr = null;
	ObjectInputStream inStr = null;
	final ByteChannel channel;
	
	byte[] buf = new byte[2048];

	public ObjectSocketChannelStream(ByteChannel channel) {
		this.channel = channel;
	}
	
	public ByteChannel getChannel() {
		return channel;
	}
	
	public void writeObject(Object obj) throws IOException {
		ByteArrayOutputStream baout = new ByteArrayOutputStream();
		ObjectOutputStream outobj = new ObjectOutputStream(baout);
		
		outobj.writeObject(obj);
		outobj.flush();
		byte[] buf = baout.toByteArray();
		channel.write(ByteBuffer.wrap(buf));
	}
	
	public Object readObject() throws IOException {
		ByteArrayOutputStream baout = new ByteArrayOutputStream();
		ObjectOutputStream outobj = new ObjectOutputStream(baout);
		
		// outobj.writeObject(obj);
		outobj.flush();
		byte[] buf = baout.toByteArray();
		channel.write(ByteBuffer.wrap(buf));
		
		return null;
	}
	
	public ObjectInputStream getInputStream() {
		if (inStr == null) {
			try {
				this.inStr = new ObjectInputStream(Channels.newInputStream(channel));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
		}
		return inStr;
	}
	
	public ObjectOutputStream getOutputStream() {
		if (outStr == null) {
			try {
				this.outStr = new ObjectOutputStream(Channels.newOutputStream(channel));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return outStr;
	}

	private void checkBuffer(int sze) {
		if (buf.length < sze) {
			buf = new byte[sze + 2048];
		}
	}
	public byte[] readByteArray(final int sze) throws IOException {
		checkBuffer(sze);
		
		int totrd = 0;
		int tsze = sze;
		while (tsze > 0) {
			int rdlen = getInputStream().read(buf, totrd, tsze);
			tsze -= rdlen;
			totrd += rdlen;
		}
		System.out.println("Read buffer of " + sze + " bytes, actual: " + totrd);
		
		return buf;
	}

	
	public void write(ByteBuffer tmp) throws IOException {
		int sze = tmp.position();
		tmp.position(0);
		byte[] loc = null;
		if (tmp.hasArray()) {
			loc = tmp.array();
		} else {
			checkBuffer(sze);			
			tmp.get(buf, 0, sze);
			loc = buf;
		}
		System.out.println("Writing buffer of " + sze + " bytes");
		getOutputStream().write(loc, 0, sze);
	}
	
	public static OutputStream newOutputStream(final ByteChannel channel) {
		
	    return new OutputStream() {
			final ByteBuffer buf = ByteBuffer.allocate(2048);

			public synchronized void write(int b) throws IOException {
	        	if (buf.remaining() == 0) {
	        		flush();
	        	}
	        	
	            buf.put((byte) b);
	        }

	        public synchronized void write(byte[] bytes, int off, int len) throws IOException {
	        	int rem = buf.remaining();
	        	while (len > rem) {
	        		len -= rem;
	        		buf.put(bytes, off, rem);
	        		off += rem;
	        		flush();
	        		rem = buf.remaining();
	        	}
	            buf.put(bytes, off, len);
	        }
	        
	        public synchronized void flush() {
	        	try {
					channel.write(buf);
					buf.reset();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	    };
	}

	// Returns an input stream for a ByteBuffer.
	// The read() methods use the relative ByteBuffer get() methods.
	public static InputStream newInputStream(final ByteChannel channel) {
		final ByteBuffer buf = ByteBuffer.allocate(2048);

		return new InputStream() {
	        public synchronized int read() throws IOException {
	            if (!buf.hasRemaining()) {
	                channel.read(buf);
	            }
	            return buf.get();
	        }

	        public synchronized int read(byte[] bytes, int off, int len) throws IOException {
	        	int rem = buf.remaining();

	        	while (rem < len) {
	        		if (rem > 0) {
		            	buf.get(bytes, off, rem);
		            	off += rem;
		            	len -= rem;
	        		}
	            	channel.read(buf);
	            	rem = buf.remaining();
	            }
	            
	            buf.get(bytes, off, len);
	            
	            return len;
	        }
	    };
	}
}
