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

/*
 * Created on Aug 10, 2005
 */
package com.bigdata.gom.om;

/**
 * This interface defines a protocol for native transactions. Native
 * transactions may be used to buffering updates and perform an implicit commit
 * when the transaction counter reaches zero. Operations outside of a
 * transaction context are NOT buffered and are immediately committed.
 * 
 * @author thompsonbry
 */

public interface INativeTransaction
{

    /**
     * Increments the native transaction counter.  When this
     * counter reaches zero, the transaction will be committed.<p>
     * 
     * @return The new value of the native transaction counter,
     * i.e., <em>after</em> it was incremented.
     */

    public int beginNativeTransaction();
    
    /**
     * The native transaction is committed when the counter reaches
     * zero.  This method accepts the value returned by {@link
     * #beginNativeTransaction()} and throws an exception if
     * <i>expectedCounter</i> is not equal to the internal counter.
     * This provides an eager search for un-matched native transactions.
     * 
     * @return The value of the counter after it has been decremented.
     * If the returned value is zero, then the native transaction was
     * committed.
     * 
     * @exception if the <i>expectedCounter</i> is not equal to the
     * internal counter <em>before</em> the latter is decremented.
     */
    public int commitNativeTransaction(int expectedCounter);

    /**
     * Rollback all changes since the last time the native
     * transaction counter was zero.
     */
    public void rollbackNativeTransaction();

    /**
     * The current value of the native transaction counter.<p>
     */
    public int getNativeTransactionCounter();

}
