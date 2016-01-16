/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.io.writecache;

import java.nio.ByteBuffer;

/**
 * A backing reader can be provided to a WriteCacheService which can
 * use the readRaw callback method as part of an encapsulated caching
 * strategy.
 * 
 * @author Martyn Cutcher
 *
 */
public interface IBackingReader {

    /**
     * Read on the backing file, installing the record into the caller's buffer.
     * {@link ByteBuffer#remaining()} bytes will be read into the caller's
     * buffer, starting at the specified offset in the backing file.
     * 
     * @param fileOffset
     *            The offset of the first byte to be read (absolute offset on
     *            the backing file).
     * @param dst
     *            Where to put the data. Bytes will be written at position until
     *            limit.
     * 
     * @return The caller's buffer, prepared for reading back the installed
     *         record.
     */
	public ByteBuffer readRaw(long fileOffset, ByteBuffer dst);
	
}
