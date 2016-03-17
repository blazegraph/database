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


/** A coder based on a set of prefix-free codewords.
 * 
 * <P>Not all coders are codeword-based (for instance, arithmetic coding
 * is not codeword-based). However, coders that are based on prefix-free codewords are invited
 * to return by means of {@link it.unimi.dsi.compression.Codec#coder()} an
 * implementation of this interface.
 * 
 * <p>Note that the {@linkplain PrefixCodec#coder() coder} returned by a {@link PrefixCodec} is
 * an implementation of this interface.
 */
public interface PrefixCoder extends Coder {

	/** Provides access to the codewords.
	 * 
	 * <strong>Warning</strong>: bit 0 of each bit vector returned by {@link #codeWords()} is
	 * the <em>first (leftmost) bit</em> of the corresponding codeword: in other words, codewords are stored in
	 * right-to-left fashion.
	 * 
	 * @return the codewords.
	 */
	
	BitVector[] codeWords();
}
