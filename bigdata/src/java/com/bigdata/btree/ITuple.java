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

package com.bigdata.btree;

import com.bigdata.io.IByteArrayBuffer;

/**
 * Interface exposes more direct access to keys and values visited by an
 * {@link IEntryIterator}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITuple {

    /**
     * True iff the keys for the visited entries were requested when the
     * {@link Tuple} was initialized.
     */
    public boolean getKeysRequested();

    /**
     * True iff the values for the visited entries were requested when the
     * {@link Tuple} was initialized.
     */
    public boolean getValuesRequested();

    /**
     * The #of entries that have been visited so far and ZERO (0) until the
     * first entry has been visited.
     */
    public long getVisitCount();

    /**
     * Returns a copy of the current key.
     * <p>
     * Note: This causes a heap allocation. See {@link #getKeyBuffer()} to avoid
     * that allocation.
     * 
     * @throws UnsupportedOperationException
     *             if keys are not being materialized.
     */
    public byte[] getKey();

    /**
     * The buffer into which the keys are being copied.
     * 
     * @return The buffer.
     * 
     * @throws UnsupportedOperationException
     *             if keys are not being materialized.
     */
    public IByteArrayBuffer getKeyBuffer();
    
}
