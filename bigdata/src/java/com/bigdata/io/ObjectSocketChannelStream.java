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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;

/**
 * Utility class that provides dual Channel/ObjectStream access.
 * 
 * @author Martyn Cutcher
 *
 */
public class ObjectSocketChannelStream {
	final ObjectOutputStream outStr;
	final ObjectInputStream inStr;
	final SocketChannel channel;

	public ObjectSocketChannelStream(Socket socket) {
		this.channel = socket.getChannel();
		try {
			this.inStr = new ObjectInputStream(socket.getInputStream());
			this.outStr = new ObjectOutputStream(socket.getOutputStream());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public SocketChannel getChannel() {
		return channel;
	}
	
	public ObjectInputStream getInputStream() {
		return inStr;
	}
	
	public ObjectOutputStream getOutputStream() {
		return outStr;
	}
}
