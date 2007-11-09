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
 * Created on Feb 2, 2007
 */

package com.bigdata.btree;

import java.io.Serializable;

/**
 * A user-defined function that may be passed into an insert operation on a
 * btree in order to provide extensible local logic. User defined functions may
 * be used to create counters, assign timestamps, perform conditional inserts,
 * etc.
 * 
 * @todo the network api should support minimization of IO required to transfer
 *       UDFs.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface UserDefinedFunction extends Serializable {

    /**
     * Invoked if a key is found in the tree.
     * 
     * @param key
     *            The search key.
     * @param oldval
     *            The value currently stored under that key (MAY be null).
     * 
     * @return The object to insert under that key. Return <i>oldval</i> if the
     *         entry under that key SHOULD NOT be updated. Otherwise return the
     *         new value for that key. A return of <code>null</code> is legal
     *         and will set the value for that key to null unless it is already
     *         null, in which case the value will not be updated.
     */
    abstract public Object found(byte[] key, Object oldval);

    /**
     * Invoked if a key is not found in the tree.
     * 
     * @param key
     *            The search key.
     * 
     * @return The value to be inserted under that key or <code>null</code> if
     *         no value should be inserted.  In the special case when you need
     *         to insert the key with a null value, return {@link #INSERT_NULL}.
     */
    abstract public Object notFound(byte[] key);

    /**
     * The value returned by this method is the value returned to the
     * application as the result of the insert operation. Normally the
     * application will see the oldval. However, there are cases when you want
     * to return the current value, e.g., when implementing an auto-increment
     * counter.
     * <p>
     * Note: In order to implement this method such that you return the assigned
     * value rather than the <i>oldval</i> you generally need to set an
     * instance variable based on whether {@link #found(byte[], Object)} or
     * {@link #notFound(byte[])} was invoked. Failure to handle both conditions
     * will cause spurious return values based on the last insert operation.
     * <p>
     * Note: This method is invoked with <i>oldval:=null</code> if you return
     * <code>null</code> from {@link #notFound(byte[])}.
     * 
     * @param key
     *            The search key.
     * @param oldval
     *            The old value for the key.
     * 
     * @return The value to be returned to the application.
     */
    abstract public Object returnValue(byte[] key, Object oldval);
    
    /**
     * A special object that may be used returned from {@link #notFound(byte[])}
     * in order to force the insertion of a key with a <code>null</code>
     * value.
     */
    public static final Object INSERT_NULL = new Object();
    
}
