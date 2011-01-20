/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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

package com.bigdata.rwstore.sector;

/**
 * The SectorManager defines the contract required to manage a set of
 * SectorAllocators.
 * 
 * The SectorManager is passed to the SectorAllocator constructors and they
 * will callback to manage their free list availability, and to trim the
 * allocated storage if required.
 * 
 * @author Martyn Cutcher
 *
 */
public interface ISectorManager {

	/**
	 * This request is made when the sectorAllocator no longer has a full set
	 * of block allocations available.
	 * 
	 * The allocator will issue this callback to help the SectorManager manage
	 * an effective freelist of available allocators.
	 * 
	 * @param sectorAllocator to be removed
	 */
	void removeFromFreeList(SectorAllocator sectorAllocator);

	/**
	 * When suficient alocations have been freed for recycling that a threshold
	 * of availability of reached for all block sizes, then the allocator
	 * calls back to the SectorManager to signal it is available to be returned
	 * to the free list.
	 * 
	 * @param sectorAllocator to be added
	 */
	void addToFreeList(SectorAllocator sectorAllocator);
	
	/**
	 * When a sector is first created, it will remain at the head of the free
	 * list until one of two conditions has been reached:
	 * 
	 * 1) The allocation has been saturated
	 * 2) The bit space has been filled
	 * 
	 * In the case of (2), then it is possible that significant allocation
	 * space cannot be utilised - which will happen if the average allocation
	 * is less than 1K.  In this situation, the sector can be trimmed and the
	 * space made available to the next sector.
	 * 
	 * trimSector will only be called in this condition - on the first occasion
	 * that the allocator is removed from the freeList.
	 * 
	 * @param trim - the amount by which the sector allocation can be reduced
	 */
	void trimSector(long trim, SectorAllocator sector);

}
