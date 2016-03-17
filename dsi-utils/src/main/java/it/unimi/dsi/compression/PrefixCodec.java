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

import it.unimi.dsi.bits.BitVector;

/** A codec based on a set of prefix-free codewords.
 * 
 * <p>Prefix codec work by building a vector of prefix-free codewords, one for each symbol. The
 * method {@link #codeWords()} returns that vector. Moreover, this interface
 * strengthens the return type of {@link #coder()} to {@link PrefixCoder}.
 */
public interface PrefixCodec extends Codec {
	/** Returns the vector of prefix-free codewords used by this prefix coder.
	 * 
	 * @return the vector of prefix-free codewords used by this prefix coder.
	 */
	public BitVector[] codeWords();
	
	public PrefixCoder coder();
}
