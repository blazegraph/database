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

/** An abstract factory corresponding to an instance of a specific compression technique. 
 * 
 * <P>An implementation of this interface provides coders and decoders. The
 * constructors must provide all data that is required to perform coding
 * and decoding.
 */

public interface Codec {
	/** Returns a coder for the compression technique represented by this coded. 
	 * 
	 * @return a coder for the compression technique represented by this codec. */
	public Coder coder();
	
	/** Returns a decoder for the compression technique represented by this coded. 
	 * 
	 * @return a decoder for the compression technique represented by this codec. */
	public Decoder decoder();
	
	/** Returns the number of symbols handled by this codec. 
	 * 
	 * @return the number of symbols handled by this codec.
	 */
	public int size();
}
