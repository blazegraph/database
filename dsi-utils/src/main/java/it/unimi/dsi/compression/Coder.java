package it.unimi.dsi.compression;

/*		 
 * DSI utilities
 *
 * Copyright (C) 2005-2009 Sebastiano Vigna 
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

import it.unimi.dsi.fastutil.booleans.BooleanIterator;
import it.unimi.dsi.io.OutputBitStream;

import java.io.IOException;

/** Coding methods for a specific compression technique. */
public interface Coder {
	/** Encodes a symbol.
	 * 
	 * @param symbol a symbol.
	 * @return a boolean iterator returning the bits coding <code>symbol</code>.
	 */
	BooleanIterator encode( int symbol );

	/** Encodes a symbol.
	 * 
	 * @param symbol a symbol.
	 * @param obs the output bit stream where the encoded symbol will be written.
	 * @return the number of bits written.
	 */
	int encode( int symbol, OutputBitStream obs ) throws IOException;
	
	/** Flushes the coder.
	 * 
	 * <strong>Warning</strong>: this method will <em>not</em> {@link OutputBitStream#flush() flush} <code>obs</code>.
	 *  
	 * @param obs the output bit stream where the flushing bits will be written.
	 * @return the number of bits written to flush the coder.
	 */
	
	int flush( OutputBitStream obs );

	/** Flushes the coder.
	 * 
	 * @return a boolean iterator returning the bits used to flush this coder.
	 */
	
	BooleanIterator flush();
}
