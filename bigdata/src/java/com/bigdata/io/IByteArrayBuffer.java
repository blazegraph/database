/*

 Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
/*
 * Created on Dec 21, 2007
 */

package com.bigdata.io;

/**
 * An interface for reading from and accessing a managed byte[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IByteArrayBuffer {

    /**
     * The backing byte[] buffer. This is re-allocated whenever the capacity of
     * the buffer is too small and reused otherwise.
     */
    public byte[] array();

    /**
     * The capacity of the buffer.
     */
    public int capacity();

    /**
     * The current position in the buffer.
     */
    public int position();

    /**
     * The #of bytes remaining in the buffer before it would overflow.
     */
    public int remaining();

}
