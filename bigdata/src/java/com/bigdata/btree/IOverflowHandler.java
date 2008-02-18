/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
 * Created on Feb 12, 2008
 */

package com.bigdata.btree;

import java.io.Serializable;

import com.bigdata.rawstore.IRawStore;

/**
 * An interface that allows you to inspect index entries during an
 * {@link IndexSegmentBuilder} operation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IOverflowHandler extends Serializable {

    /**
     * Invoked for each index entry.
     * 
     * @param tuple
     *            The index entry.
     * @param target
     *            The target store on which you can write additional data.
     * 
     * @return The new value to be stored under the key in the generated
     *         {@link IndexSegment}.
     */
    public byte[] handle(ITuple tuple, IRawStore target);

    /**
     * Notified when overflow processing is done for a given source and
     * target.
     */
    public void close();
    
}
