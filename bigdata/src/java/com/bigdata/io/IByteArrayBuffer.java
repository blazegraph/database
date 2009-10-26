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

import it.unimi.dsi.fastutil.io.RepositionableStream;

/**
 * An interface for reading from and accessing a managed byte[]. Implementations
 * of this interface may permit transparent extension of the managed byte[].
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo raise mark(), etc. into this interface?
 */
public interface IByteArrayBuffer extends IDataRecord {

    /**
     * The backing byte[] WILL be transparently replaced if the buffer capacity
     * is extended. {@inheritDoc}
     */
    byte[] array();

    /**
     * The offset of the slice into the backing byte[] is always zero.
     * {@inheritDoc}
     */
    int off();

    /**
     * The length of the slice is always the capacity of the backing byte[].
     * {@inheritDoc}
     */
    int len();
    
    /**
     * The capacity of the buffer.
     */
    int capacity();

    /**
     * The current position in the buffer.
     * <p>
     * Note: The method name was choose to avoid a collision with
     * {@link RepositionableStream#position()}.
     */
    int pos();

    /**
     * The read limit (there is no write limit on the buffer since the capacity
     * will be automatically extended on overflow).
     */
    int limit();

    /**
     * The #of bytes remaining in the buffer before it would overflow.
     */
    int remaining();

}
