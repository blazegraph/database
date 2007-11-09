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
package com.bigdata.scaleup;

import com.bigdata.btree.IndexSegment;

/**
 * Interface for a scheduleable task that produces one or more
 * {@link IndexSegment}s, updates the {@link MetadataIndex} to reflect the
 * existence of the new {@link IndexSegment}s and notifies existing views
 * with a depedency on the source(s) that they must switch over to the new
 * {@link IndexSegment}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IPartitionTask extends
        java.util.concurrent.Callable<Object> {
    
    /**
     * Run the task.
     * 
     * @return No return semantics are defined.
     * 
     * @throws Exception
     *             The exception thrown by the task.
     */
    public Object call() throws Exception;

}