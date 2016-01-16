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
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;

/**
 * Interface allows an application to decide on the file name for the snapshot,
 * whether or not the snapshot will be compressed, etc.
 * 
 * @author bryan
 * @see <a href="http://trac.bigdata.com/ticket/1172"> Online backup for Journal
 *      </a>
 */
public interface ISnapshotFactory {

   /**
    * Return the {@link File} on which the snapshot will be written. A common
    * strategy is to label each snapshot with the commit counter from the
    * associated root block and place the snapshots within some directory known
    * to the application. E.g., "snapshots/snapshot-commitCounter.jnl". If the
    * snapshot is to be compressed, then the best practice is to have the file
    * name end with ".gz". See the {@link CommitCounterUtility} for helper
    * classes for dealing with commit counters as fixed width strings.
    * 
    * @param rbv
    *           The current root block. The snapshot will be consistent as of
    *           the commit point associated with this root block.
    * 
    * @return The file on which the snapshot will be written. If the file
    *         exists, then it must be empty. If the file is in a directory, the
    *         directory must exist and must be writable.
    * 
    * @throws IOException
    * 
    * @see CommitCounterUtility
    */
   File getSnapshotFile(final IRootBlockView rbv) throws IOException;

   /**
    * When <code>true</code> the generated snapshot will be compressed.
    */
   boolean getCompress();

}
