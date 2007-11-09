/**

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
 * Created on Oct 27, 2006
 */

package com.bigdata.istore;

/**
 * A persistence store.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo Reify the notion of a segment?  Provide named roots for segments?
 */
public interface IStore {

    /**
     * Return an unisolated object manager.
     * 
     * @return
     * 
     * @todo Optional operation?  Drop completely?
     */
    public IOM getObjectManager();
    
    /**
     * Return a new object manager with transactional isolation.
     * 
     * @return
     */
    public ITx startTx();
    
    /**
     * Shutdown the store.
     */
    public void close();

    /**
     * True iff the store is open.
     */
    public boolean isOpen();
    
}
