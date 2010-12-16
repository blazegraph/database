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

package com.bigdata.rwstore;

import java.io.File;

/**
 * The IStore interface provides persistent file-backed storage. It can be used
 * as a standalone utility, but has been primarily designed to support the
 * Generic Persistent Object model.
 */
public interface IStore {
	
//	/**************************************************************
//	 * called when used as a server, returns whether facility is enabled
//	 **/
//	public boolean preserveSessionData(); 

	/**
	 * Writes data on the store.
	 *
	 * @return the allocated address
	 **/
	public long alloc(byte buf[], int size, IAllocationContext context);

	/**
	 * Frees allocated storage
	 * 
	 * @param addr
	 *            the storage address to be freed
	 */
	public void free(long addr, int size);

//	/**************************************************************
//	 * Odd method needed by PSInputStream to fetch data of unknown
//	 *	size into a buffer
//	 *
//	 * <p>Both RWStore and WOStore store data in either explicit or
//	 *	implicit block sizes.</p>
//	 *
//	 * @param addr the address of the data in the IStore
//	 *				buf the buffer to store the data in
//	 *
//	 * @returns the size of the data copied
//	 **/
//	public int getDataSize(long addr, byte buf[]);

	/**
	 * Read data of a known size from the store.
	 * 
	 * @param l
	 *            the address of the data
	 * @param buf
	 *            the buffer of the size required!
	 */
	public void getData(long l, byte buf[]);
	
	/**************************************************************
	 * @param addr - the address
	 * @return the size of the slot associated
	 */
	public int getAssociatedSlotSize(int addr);
	
//	/**************************************************************
//	 * Given a physical address (byte offset on the store), return true
//	 * if that address could be managed by an allocated block.
//	 *
//	 * @param a the storage address to be tested
//	 **/	
//	public boolean verify(long a);

//	/**
//	 * The {@link RWStore} always generates negative address values.
//	 * 
//	 * @return whether the address given is a native IStore address
//	 */
//	public boolean isNativeAddress(long value);
	
//	/**
//	 * useful in debug situations
//	 *
//	 * @return store allocation and usage statistics
//	 */
//	public String getStats(boolean full);
	
	/**
	 * Close the file.
	 */
	public void close();
	
//	/**
//	 * Needed by PSOutputStream for BLOB buffer chaining.
//	 */
//	public int bufferChainOffset();

	/**
	 * Retrieves store file. Can be used to delete the store after the IStore
	 * has been released
	 * 
	 * @return the File object
	 */
	public File getStoreFile();

//	/**
//	 * Called by the PSOutputStream to register the header block of a blob. The
//	 * store must return a new address that is used to retrieve the blob header.
//	 * This double indirection is required to be able to manage the blobs, since
//	 * the blob header itself is of variable size and is handled by the standard
//	 * FixedAllocators in the RWStore.
//	 * 
//	 * @param addr
//	 *            The address of the header block of the blob.
//	 * 
//	 * @return The 
//	 */
//	public int registerBlob(int addr);

}
