/**

Copyright (C) SYSTAP, LLC 2006-2010.  All rights reserved.

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
package com.bigdata.ha.msg;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class HAGlobalWriteLockRequest implements IHAGlobalWriteLockRequest,
        Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private final long lockWaitTimeout;
    private final TimeUnit lockWaitUnits;
    private final long lockHoldTimeout;
    private final TimeUnit lockHoldUnits;

    public HAGlobalWriteLockRequest(final long lockWaitTimeout,
            final TimeUnit lockWaitUnits, final long lockHoldTimeout,
            final TimeUnit lockHoldUnits) {

        if (lockWaitTimeout <= 0)
            throw new IllegalArgumentException();

        if (lockHoldTimeout <= 0)
            throw new IllegalArgumentException();

        if (lockWaitUnits == null)
            throw new IllegalArgumentException();

        if (lockHoldUnits == null)
            throw new IllegalArgumentException();

        this.lockWaitTimeout = lockWaitTimeout;
        this.lockHoldTimeout = lockHoldTimeout;
        this.lockWaitUnits = lockWaitUnits;
        this.lockHoldUnits = lockHoldUnits;

    }

    @Override
    public long getLockWaitTimeout() {
        return lockWaitTimeout;
    }

    @Override
    public TimeUnit getLockWaitUnits() {
        return lockWaitUnits;
    }

    @Override
    public long getLockHoldTimeout() {
        return lockHoldTimeout;
    }

    @Override
    public TimeUnit getLockHoldUnits() {
        return lockHoldUnits;
    }

}
