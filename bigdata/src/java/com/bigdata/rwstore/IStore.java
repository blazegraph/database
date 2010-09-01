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

import com.bigdata.journal.IAllocationContext;

/************************************************************************************************
 * The IStore interface provides persistent file-backed storage.
 *  It can be used as a standalone utility, but has been primarily designed
 *	to support the Generic Persistent Object model.
 **/
public interface IStore {
    
//	/*********************************************************************
//	 * Provides a link to an object to carryout any additional data updates
//	 *	before the physical commit - used by the GPO object managers for example
//	 **/
//	public static interface ICommitCallback {
//	  public void commitCallback();
//	  public void commitComplete();
//	}
	
	public boolean isLongAddress();
	
//	/**************************************************************
//	 * Registers a commitCallback object.
//	 *
//	 * <p>This method may be called more than once, there maybe several
//	 *	such objects.</p>
//	 *
//	 * <p>It is used by the GPO object managers to allow them to store
//	 *	index information and other updated data after a commit
//	 *	cycle.</p>
//	 **/
//	public void setCommitCallback(ICommitCallback callback);
	
	/**************************************************************
	 * called when used as a server, returns whether facility is enabled
	 **/
	public boolean preserveSessionData(); 

//	/**************************************************************
//	 * the filestore may be explicitly limited
//	 *	- useful when testing, it is all too easy to fill a disk
//	 *
//	 * <p>the default is 1GB</p>
//	 *
//	 * @param size the new max filesize &gt;&gt; 8
//	 **/
//	public void setMaxFileSize(int size);
	
	/**************************************************************
	 * the lowest level interface should normally not be used directly.
	 *
	 * @return the allocated address
	 **/
	public long alloc(byte buf[], int size, IAllocationContext context);
	
	/**************************************************************
	 * frees allocated storage
	 *
	 * @param addr the storage address to be freed
	 **/
	public void free(long addr, int size);

//	/**************************************************************
//	 * Reallocates storage
//	 *
//	 * @param oldAddr is the existing address to be freed
//	 * @return a stream to write to the store
//	 **/
//	public PSOutputStream realloc(long oldAddr, int size);
//	
//	public PSInputStream getData(long value);

	/**************************************************************
	 * Odd method needed by PSInputStream to fetch data of unknown
	 *	size into a buffer
	 *
	 * <p>Both RWStore and WOStore store data in either explicit or
	 *	implicit block sizes.</p>
	 *
	 * @param addr the address of the data in the IStore
	 *				buf the buffer to store the data in
	 *
	 * &returns the size of the data copied
	 **/
	public int getDataSize(long addr, byte buf[]);

//	/**************************************************************
//	 * if the caller can be sure of the size, then a more efficient allocation can be made,
//	 * but the corresponding getData call must also be made with an explicit size.
//	 *
//	 * <p>this should not generally be used - but specific objects can exploit this 
//	 *	interface for storing special purpose fixed size structures.</p>
//	 *
//	 * <p>Note that the Write Once Store will not automatically preserve historical
//	 *	address information if explicit buffers are used.</p>
//	 **/	
//	public long realloc(long oldaddr, int oldsze, byte buf[]);
	
	/**************************************************************
	 * Used to retrieve data of a known size, typically after having
	 *	been allocated using fixed size reallocation.
	 *
	 * @param l the address of the data
	 * @param buf the buffer of the size required!
	 **/	
	public void getData(long l, byte buf[]);
	
	/**************************************************************
	 * a debug method that verifies a storage address as active
	 *
	 * @param a the storage address to be tested
	 **/	
	public boolean verify(long a);

	/***************************************************************************************
	 * this supports the core functionality of a WormStore, other stores should return
	 *	zero, indicating no previous versions available
	 **/
	public long getPreviousAddress(long addr);

	/***************************************************************************************
	 * @return whether the address given is a native IStore address
	 **/
	public boolean isNativeAddress(long value);

//	/***************************************************************************************
//	 * the root address enables the store to be self contained!
//	 *	Along with the allocation information to manage the data, the store by default
//	 *	can store and provide a root address to data needed to initialize the system.
//	 *
//	 * @param addr the address to be stored as "root"
//	 **/
//	public void setRootAddr(long addr);
//	
//	/***************************************************************************************
//	 * @return the root address previously set
//	 **/
//	public long getRootAddr();
	
//	/***************************************************************************************
//	 * A utility equivalent to : store.getData(store.getRootAddr());
//	 *
//	 * @return an InputStream for any data stored at the root address
//	 **/
//	public PSInputStream getRoot();
	
//	/***************************************************************************************
//	 * clears all data from the store.
//	 **/
//	public void clear();


//	/***************************************************************************************
//	 * increments the current nested transaction level
//	 **/
//	public void startTransaction();
//	
//	/***************************************************************************************
//	 * decrements the current nested transaction level, if the value is reduced to zero then
//	 *	a physical commit is carried out, if the level is already zero, a runtime exception
//	 *	is thrown.
//	 **/
//	public void commitTransaction();
//	
//	/***************************************************************************************
//	 * if the transaction level is greater than one, all modifcations are undone, and the
//	 *	transaction level set to zero.
//	 **/
//	public void rollbackTransaction();

	/***************************************************************************************
	 * does what it says
	 **/
	public String getVersionString();
	
	/***************************************************************************************
	 * useful in debug situations
	 *
	 * @return store allocation and usage statistics
	 **/
	public String getStats(boolean full);
	
	public void close();
	
	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public int bufferChainOffset();
	
	public void absoluteWriteLong(long addr, int threshold, long value);

	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public void absoluteWriteInt(int addr, int offset, int value);
	
	/***************************************************************************************
	 * Needed to free Blob chains.
	 **/
	public int absoluteReadInt(int addr, int offset);
	
	/***************************************************************************************
	 * Needed to free Blob chains.
	 **/
	public int absoluteReadLong(long addr, int offset);
	
//	/***************************************************************************************
//	 * copies the store to a new file, this is not necessarily a byte for byte copy
//	 *	since the store could write only consolidated data - particulalry relevant for the
//	 *	Write Once store.
//	 *
//	 * @param filename specifies the file to be copied to.
//	 **/
//	public void backup(String filename) throws FileNotFoundException, IOException;
//
//	/***************************************************************************************
//	 * copies the store to a new file, this is not necessarily a byte for byte copy
//	 *	since the store could write only consolidated data - particulalry relevant for the
//	 *	Write Once store.
//	 *
//	 * @param outstr specifies stream to be copied to.
//	 **/
//	public void backup(OutputStream outstr) throws IOException;
//
//	/***************************************************************************************
//	 * useful in deployed web services to be able to restore a previously backed-up
//	 *	store.  Can also be useful to copy databases, for example, when running
//	 *	a test system that can be simply restored to a backup extracted from a live system.
//	 *
//	 * @param instr specifies stream to be restored from.
//	 **/
//	public void restore(InputStream instr) throws IOException;

  /*********************************************************************************************
   * Retrieves store file.
   * Can be used to delete the store after the IStore has been released
   * @return the File object
   **/
	public File getStoreFile();

	public void absoluteWriteAddress(long addr, int threshold, long addr2);

	public int getAddressSize();

	/**
	 * Called by the PSOutputStream to register the header bloc of a blob.  The store
	 * must return a new address that is used to retrieve the blob header. This double
	 * indirection is required to be able to manage the blobs, since the blob header
	 * itself is of variable size and is handled by the standard FixedAllocators in the
	 * RWStore.  For a WORM implementation the address of the blob header can be returned
	 * directly
	 * 
	 * @param addr
	 * @return
	 */
	public int registerBlob(int addr);
}
