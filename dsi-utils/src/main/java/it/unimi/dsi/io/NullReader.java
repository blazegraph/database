package it.unimi.dsi.io;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2003-2009 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 2.1 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 */

import java.io.Reader;
import java.io.Serializable;


/** End-of-stream-only reader.
 *
 * <P>This reader will always return end-of-file on any read attempt.
 *
 * <P>This class is a singleton. You cannot create a null reader,
 * but you can obtain an instance of this class using {@link #getInstance()}.
 *
 * @author Sebastiano Vigna
 * @since 0.9.2
 */

public class NullReader extends Reader implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final static NullReader INSTANCE = new NullReader();

	private NullReader() {}
	 
	/** Returns the only instance of this class.
	 * 
	 * @return the only instance of this class.
	 */
	public static NullReader getInstance() {
		return INSTANCE;
	}

	public void close() {}

	public int read( final char[] cbuf, final int off, final int len ) {
		return -1;
	}
}
