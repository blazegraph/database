package it.unimi.dsi.util;


/*		 
 * DSI utilities
 *
 * Copyright (C) 2007-2009 Sebastiano Vigna 
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
        
import it.unimi.dsi.fastutil.longs.LongList;

import java.util.List;

/** A list of longs with long indices.
 * 
 * <p>Unfortunately, {@link List} is limited to positions smaller than or equal to {@link Integer#MAX_VALUE}.
 * Since bits in a not-so-large array need to be indexed with longs, the {@linkplain it.unimi.dsi.bits.BitVector#asLongBigList(int) list view over a bit vector}
 * requires a more powerful interface. The naming follows the <A HREF="http://fastutil.dsi.unimi.it/"><samp>fastutil</samp></A>
 * conventions (actually, this class extends {@link LongList}).
 */

public interface LongBigList extends LongList {
	/** Returns the long at the given position.
	 * 
	 * @param index a position in the list.
	 * @return the corresponding long value.
	 * @see List#get(int)
	 */
	public long getLong( long index );

	/** Removes the long at the given position.
	 * 
	 * @param index a position in the list.
	 * @return the long previously at the specified position.
	 * @see List#remove(int)
	 */
	public long removeLong( long index );

	/** Sets the long at the given position.
	 * 
	 * @param index a position in the list.
	 * @param value a long value.
	 * @return the previous value.
	 * @see List#set(int,Object)
	 */
	public long set( long index, long value );

	/** Adds the long at the given position.
	 * 
	 * @param index a position in the list.
	 * @param value a long value.
	 * @see List#add(int,Object)
	 */
	public void add( long index, long value );
	
	/** The number of elements in this big list.
	 * 
	 * @return the number of elements in this big list.
	 * @see List#size()
	 */
	public long length();
	
	/** Sets the number of elements in this big list.
	 * 
	 * @return this big list.
	 * @see LongList#size(int)
	 */
	public LongBigList length( long newLength );
	
	/** Returns a big sublist view of this big list.
	 * 
	 * @param from the starting element (inclusive).
	 * @param to the ending element (exclusive).
	 * @return a big sublist view of this big list.
	 * @see List#subList(int, int)
	 */
	public LongBigList subList( long from, long to );
}
