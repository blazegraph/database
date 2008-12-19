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
 * Created on Feb 16, 2007
 */

package com.bigdata.journal;

import java.io.IOException;
import java.rmi.Remote;

/**
 * A service for unique timestamps.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITimestampService extends Remote {

    /**
     * Return the next unique timestamp. Timestamps must be strictly increasing.
     * <p>
     * Note: This method MUST return strictly increasing values, even when it is
     * invoked by concurrent threads. While other implementations are possible
     * and may be more efficient, one way to insure thread safety is to
     * synchronize on some object such that the implementaiton exhibits a FIFO
     * behavior.
     * 
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @see TimestampServiceUtil#nextTimestamp(ITimestampService)
     */
    public long nextTimestamp() throws IOException;

}
