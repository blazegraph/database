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

package com.bigdata.ha;

import com.bigdata.journal.ForceEnum;
import com.bigdata.journal.IRootBlockView;

/**
 * Any Journal to be used in HA must implement the ISlave interface.  This is an intermediate level
 * interface covering both raw IO but also commit dependencies with RootBlock synchronization.
 * 
 * @author Martyn Cutcher
 *
 */
public interface ISlave {

	/**
	 * Ensures the slave journal is synchronized with the master.
	 *
	 * For new files all journals, if initialised from similar Properties will have created identical rootBlocks.
	 *  
	 * @param rootBlock - the rootBlock of the master journal
	 */
	public void checkRootBlock(IRootBlockView rootBlock);
	
	/**
	 * It is assumed that the data has already been validated by the call, the separate
	 * checksum can be used to validate the data remoting
	 * 
	 * @param address where data should be added
	 * @param data to be written
	 * @param chk the checksum to be appended
	 */
	public void addWrite(long newaddr, long oldaddr, byte[] data, long chk);

	/**
	 * It is assumed that when a rootblock is sent, this implies a commit, so the buffer strategy
	 * must flush any cached writes before updating the rootblock.
	 * 
	 * For the DiskWORM, a valid rootblock is all that is required for the Slave to be able to
	 * become a read/write Master.  See updateMetaAllocation for issues with DiskRW
	 */
	public void commitRootBlock(IRootBlockView rootBlock);
	/**
	 * For a RWStrategy the allocation block cannot be updated using addWrite, since the in-memory
	 * blocks must be updated to allow address decoding to support read-access.  For non RWStrategies
	 * this should never be requested.  The request to the RWStrategy would be generated as part of
	 * the master commit.
	 */
	public void updateAllocationBlock(int index, long addr, byte[] data);

	/**
	 * The meta allocation must be updated if the DiskRW journal is to be able to make writes.  If the Slave
	 * was to become the master it should probably re-open to the current commit, thus ensuring correct
	 * meta-allocation.  In thich case this interface would not be required.  However, it seems a little
	 * odd that in all other respects other than meta-allocation that the slave would be in-sync with the
	 * master. 
	 */
	public void updateMetaAllocation(long addr, byte[] data);

	/**
	 * The journal file must be extended.  For the RWStrategy this will also shift the current allocation blocks
	 * and update the root block.
	 */
	public void extendFile(long fsize);

	/**
	 * External request to reopen the Slave journal, for example to backoff to previous commit point after uncommitted writes
	 * had been sent.
	 */
	public void reopen();

	/**
	 * External shutdown request
	 */
	public void shutdown();
}
