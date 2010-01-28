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

// Storage class
//
// Author: Martyn Cutcher
//
// Provides an interface to allocatinf storage within a disk file.
//
// Essentially provides a DiskMalloc interface.
//
// In addition to the DiskMalloc/ReAlloc mechanism, a single root address
//	can be associated.
//	This can be used when opening an existing storage file to retrieve some
//	management object - such as an object manager!
//
// The allocator also support atomic update via a simple transaction mechanism.
//
// Updates are normally committed imediately, but by using startTransaction and
//	commitTransaction, the previous state of the store is retained until
//	the moment of commitment.
//
// It would also be possible to add some journalling/version mechanism, where
//	snapshots of the allocation maps are retained for sometime.  For a store
//	which was only added to this would not be an unreasonable overhead and
//	would support the rolling back of the database weekly or monthly if
//	required.
//
// The input/output mechanism uses ByteArray Input and Output Streams.
//
// One difference between the disk realloc and in memory realloc is that the disk
//	realloc will always return a new address and mark the old address as ready to be
//	freed.
//
// Only the first four bytes of the first sector contain data.  They contain the address
//	of the current header.
//
// A set of allocation headers are used to control storage allocation.  However they must
//	also be allocated storage.  For this purpose, a meta allocationHdrf is used.  The
//	allocation headers themselves are allocated space at the end of the file.  When the
//	allocation header storage overlaps with the normal data storage, there is a file
//	reallocation.  This entails the moving of the header data to the end of the expanded
//	file.
//
// The meta-allocation header is a fixed size, and defines the maximum number of
//	allocation blocks - 64 * 4 * 8 = 2048.  Each allocation block can on average store
//	2048 allocation sectors (with the exception of blob allocators), giving a maximum
//	number of objects of 4M.
//
// Allocation is reduced to sets of allocator objects which have a start address and a
//	bitmap of allocated storage maps.
//
// Searching thousands of allocation blocks to find storage is not efficient, but by
//	utilising roving pointers and sorting blocks with free space available this can be
//	made most efficient.
//
// In order to provide optimum use of bitmaps, this implementation will NOT use the
//	BitSet class.
//
// Using the meta-allocation bits, it is straightforward to load ALL the allocation
//  headers. A total of (say) 100 allocation headers might provide upto 4000 allocations
//  each -> 400 000 objects, while 1000 headers -> 4m objects and 2000 -> 8m objects.
//
// The allocators are split into a set of FixedAllocators and then BlobAllocation.  The
//  FixedAllocators will allocate from 128 to 32K objects, with
//  a minimum block allocation of 64K, and a minimum bit number per block of 32.
//
// Where possible lists and roving pointers will be used to minimise searching of the
//  potentially large structures.
//
// Since the memory is allocated on (at least) a 128 byte boundary, there is some leeway
//  on storing the address.  Added to the address is the shift required to make to the
//  "standard" 128 byte block, e.g. blocksize = 128 << (addr % 8)
//
// NB Useful method on RandomAccessFile.setLength(newLength)
//
// When session data is preserved two things must happen - the allocators must not
//	reallocate data that has been freed in this session.  That should be it.
/////////////////////////////////////////////////////////////////////////////////////////
package com.bigdata.rwstore;

import java.util.*;
import java.io.*;
import java.nio.channels.FileChannel;

/**
 * Storage class
 * 
 * Author: Martyn Cutcher
 * 
 *  Provides an interface to allocating storage within a disk file.
 * 
 *  Essentially provides a DiskMalloc interface.
 * 
 *  In addition to the DiskMalloc/ReAlloc mechanism, a single root address
 * 	can be associated.
 * 	This can be used when opening an existing storage file to retrieve some
 * 	management object - such as an object manager!
 * 
 *  The allocator also support atomic update via a simple transaction mechanism.
 * 
 *  Updates are normally committed imediately, but by using startTransaction and
 * 	commitTransaction, the previous state of the store is retained until
 * 	the moment of commitment.
 * 
 *  It would also be possible to add some journalling/version mechanism, where
 * 	snapshots of the allocation maps are retained for sometime.  For a store
 * 	which was only added to this would not be an unreasonable overhead and
 * 	would support the rolling back of the database weekly or monthly if
 * 	required.
 * 
 *  The input/output mechanism uses ByteArray Input and Output Streams.
 * 
 *  One difference between the disk realloc and in memory realloc is that the disk
 * 	realloc will always return a new address and mark the old address as ready to be
 * 	freed.
 * 
 *  Only the first four bytes of the first sector contain data.  They contain the address
 * 	of the current header.
 * 
 *  A set of allocation headers are used to control storage allocation.  However they must
 * 	also be allocated storage.  For this purpose, a meta allocationHdrf is used.  The
 * 	allocation headers themselves are allocated space at the end of the file.  When the
 * 	allocation header storage overlaps with the normal data storage, there is a file
 * 	reallocation.  This entails the moving of the header data to the end of the expanded
 * 	file.
 * 
 *  The meta-allocation header is a fixed size, and defines the maximum number of
 * 	allocation blocks - 64 * 4 * 8 = 2048.  Each allocation block can on average store
 * 	2048 allocation sectors (with the exception of blob allocators), giving a maximum
 * 	number of objects of 4M.
 * 
 *  Allocation is reduced to sets of allocator objects which have a start address and a
 * 	bitmap of allocated storage maps.
 * 
 *  Searching thousands of allocation blocks to find storage is not efficient, but by
 * 	utilising roving pointers and sorting blocks with free space available this can be
 * 	made most efficient.
 * 
 *  In order to provide optimum use of bitmaps, this implementation will NOT use the
 * 	BitSet class.
 * 
 *  Using the meta-allocation bits, it is straightforward to load ALL the allocation
 *   headers. A total of (say) 100 allocation headers might provide upto 4000 allocations
 *   each -> 400 000 objects, while 1000 headers -> 4m objects and 2000 -> 8m objects.
 * 
 *  The allocators are split into a set of FixedAllocators and then BlobAllocation.  The
 *   FixedAllocators will allocate from 128 to 32K objects, with
 *   a minimum block allocation of 64K, and a minimum bit number per block of 32.
 * 
 *  Where possible lists and roving pointers will be used to minimise searching of the
 *   potentially large structures.
 * 
 *  Since the memory is allocated on (at least) a 128 byte boundary, there is some leeway
 *   on storing the address.  Added to the address is the shift required to make to the
 *  "standard" 128 byte block, e.g. blocksize = 128 << (addr % 8)
 * 
 *  NB Useful method on RandomAccessFile.setLength(newLength)
 * 
 *  When session data is preserved two things must happen - the allocators must not
 * 	reallocate data that has been freed in this session, or more clearly can only
 *  free data that has been allocated in this session.  That should be it.
 * 
 */

public class RWStore implements IStore {
	protected static java.util.logging.Logger cat = java.util.logging.Logger
			.getLogger(RWStore.class.getName());

	static final int MAX_FIXED_ALLOC = 8 * 1024;
	static final int MIN_FIXED_ALLOC = 64;

	ICommitCallback m_commitCallback;

	public void setCommitCallback(ICommitCallback callback) {
		m_commitCallback = callback;
	}

	// ///////////////////////////////////////////////////////////////////////////////////////
	// RWStore Data
	// ///////////////////////////////////////////////////////////////////////////////////////

	protected File m_fd;
	protected RandomAccessFile m_file;
	protected int m_transactionCount;
	protected boolean m_committing;

	protected boolean m_preserveSession = false;
	protected boolean m_readOnly;

	// lists of total alloc blocks
	ArrayList m_allocs;

	// lists of free alloc blocks
	ArrayList m_freeFixed[];

	// lists of blocks requiring commitment
	ArrayList m_commitList;

	WriteBlock m_writes;

	static final int cMinHdrSize = 2048; // FIXME 2048

	private void baseInit() {
		m_commitCallback = null;

		m_headerSize = cMinHdrSize;
		m_metaBitsSize = cDefaultMetaBitsSize;

		m_metaBits = new int[m_metaBitsSize];
		m_metaTransientBits = new int[m_metaBitsSize];

		m_maxFileSize = 2 * 1024 * 1024; // 1gb max (mult by 128)!!

		m_commitList = new ArrayList();
		m_allocs = new ArrayList();

		int numFixed = 8;

		m_freeFixed = new ArrayList[numFixed];

		for (int i = 0; i < numFixed; i++) {
			m_freeFixed[i] = new ArrayList();
		}

		m_transactionCount = 0;
		m_committing = false;
	}

	protected String m_filename;
	protected LockFile m_lockFile;

	// Constructor
	public RWStore(String file, boolean readOnly) {
		this(new File(file), readOnly);
	}

	public RWStore(File fd, boolean readOnly) {
		fd = fd.getAbsoluteFile();
		m_fd = fd;
		m_readOnly = readOnly;
		m_filename = fd.getAbsolutePath();

		if (Config.isLockFileNeeded() && !readOnly) {
			m_lockFile = LockFile.create(m_filename + ".lock");

			if (m_lockFile == null) {
				throw new RuntimeException("Store file already open");
			}
		}

		baseInit();

		try {
			if (!fd.exists()) {
				File pfile = fd.getParentFile();
				if (pfile != null) {
					pfile.mkdirs();
				}

				// Should this be more "rwd" ???
				m_file = new RandomAccessFile(fd, "rw");
				m_writes = new WriteBlock(m_file);

				// initialise the file

				// m_fileSize = 1024 * 1024;
				m_fileSize = -120; // 1Mb
				// m_fileSize = -4; // 32K
				m_metaStartAddr = m_fileSize;
				m_nextAllocation = -1; // keep on a 8K boundary (needed for
										// BlobAllocation)
				m_file.setLength(convertAddr(m_fileSize));

				startTransaction();
				commitTransaction();

				if (readOnly) {
					m_file = new RandomAccessFile(fd, "r");
				}

			} else {
				m_file = new RandomAccessFile(fd, (readOnly ? "r" : "rw"));
				m_writes = new WriteBlock(m_file);

				readFileSpec();
				readHdr();
				readAllocationBlocks();
			}
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to initialize store", e);
		}
	}

	public void close() {
		try {
			m_file.close();
			if (m_lockFile != null) {
				m_lockFile.clear();
			}
		} catch (IOException e) {
			// ..oooh err... only trying to help
		}
	}

	/*********************************************************************
	 * make sure resource is closed!
	 **/
	protected void finalize() {
		close();
	}

	protected void readAllocationBlocks() throws IOException {

		long addr = convertAddr(m_fileSize);
		long metaStart = convertAddr(m_metaStartAddr);
		int i = 0;

		TreeSet blocks = new TreeSet();

		while (addr > metaStart) {
			addr -= 1024;
			if (tstBit(m_metaBits, i++)) {
				byte buf[] = new byte[1024];

				m_file.seek(addr);

				m_file.readFully(buf);
				DataInputStream strBuf = new DataInputStream(
						new ByteArrayInputStream(buf));

				int allocSize = strBuf.readInt();
				Allocator allocator = new FixedAllocator(allocSize,
						m_preserveSession, m_writes);

				allocator.read(strBuf);

				int index = 0;
				int fixedSize = MIN_FIXED_ALLOC;
				for (; fixedSize < allocSize; fixedSize *= 2, index++)
					;

				blocks.add(allocator);

				allocator.setFreeList(m_freeFixed[index]);

				allocator.setDiskAddr(addr);
			}
		}

		m_allocs.addAll(blocks);
		for (int index = 0; index < m_allocs.size(); index++) {
			((Allocator) m_allocs.get(index)).setIndex(index);
		}
	}

	// Root interface
	public long getRootAddr() {
		return m_rootAddr;
	}

	// Root interface
	public PSInputStream getRoot() {
		try {
			return getData(m_rootAddr);
		} catch (Exception e) {
			throw new StorageTerminalError("Unable to read root data", e);
		}
	}

	public void setRootAddr(long rootAddr) {
		m_rootAddr = (int) rootAddr;
	}

	// Limits
	public void setMaxFileSize(int maxFileSize) {
		m_maxFileSize = maxFileSize;
	}

	public long getMaxFileSize() {
		long maxSize = m_maxFileSize;
		return maxSize << 8;
	}

	// Allocators
	public PSInputStream getData(long addr) {
		synchronized (this) {
			m_writes.flush();

			if (addr == 0) {
				return null;
			}

			int size = addr2Size((int) addr);

			PSInputStream instr = PSInputStream.getNew(this, size);

			try {
				m_file.seek(physicalAddress((int) addr));
				m_file.readFully(instr.getBuffer(), 0, size);
			} catch (IOException e) {
				throw new StorageTerminalError("Unable to read data", e);
			}

			return instr;
		}
	}

	// Allocators
	public PSInputStream getData(int addr, int size) {
		synchronized (this) {
			m_writes.flush();

			if (addr == 0) {
				return null;
			}

			PSInputStream instr = PSInputStream.getNew(this, size);

			try {
				m_file.seek(physicalAddress(addr));
				m_file.readFully(instr.getBuffer(), 0, size);
			} catch (IOException e) {
				throw new StorageTerminalError("Unable to read data", e);
			}

			return instr;
		}
	}

	public void getData(long addr, byte buf[]) {
		synchronized (this) {
			m_writes.flush();

			if (addr == 0) {
				return;
			}

			try {
				m_file.seek(physicalAddress((int) addr));
				m_file.readFully(buf, 0, buf.length);
			} catch (IOException e) {
				throw new StorageTerminalError("Unable to read data", e);
			}
		}
	}

	public int getDataSize(long addr, byte buf[]) {
		synchronized (this) {
			m_writes.flush();

			if (addr == 0) {
				return 0;
			}

			try {
				int size = addr2Size((int) addr);
				m_file.seek(physicalAddress((int) addr));
				m_file.readFully(buf, 0, size);

				return size;
			} catch (IOException e) {
				throw new StorageTerminalError("Unable to read data", e);
			}
		}
	}

	/***************************************************************************************
	 * this supports the core functionality of a WormStore, other stores should
	 * return zero, indicating no previous versions available
	 **/
	public long getPreviousAddress(long laddr) {
		return 0;
	}

	/**
	 * free
	 * 
	 * Biggest problem is finding the block. Sorted blocks helps this
	 * enormously, aint with the block type encoding in the address.
	 * 
	 * The encoding is made by adding to the address. 0 is added for a minimum
	 * fixed block and then 1, 2, 3 etc for the increasing fixed sizes. 15 is
	 * added for a blob.
	 */
	public void free(long laddr) {
		int addr = (int) laddr;

		switch (addr) {
		case 0:
		case -1:
		case -2:
			return;
		}

		while (addr != 0) {
			Allocator alloc = getBlockByAddress(addr);
			alloc.free(addr);

			if (!m_commitList.contains(alloc)) {
				m_commitList.add(alloc);
			}

			// Is it a potential blob block? If so then
			// read last 4 bytes to get next address
			if (alloc.getBlockSize() == 8192) {
				addr = absoluteReadInt(addr, 8188);
			} else {
				addr = 0;
			}
		}
	}

	/**
	 * alloc
	 * 
	 * Just need to work out whether it should be a blob or one of several fixed
	 * allocations
	 * 
	 * The free list <b>must</b> be realiable. Remember that m_freeBlobs is an
	 * ArrayList of free blob allocators while m_freeFixed is an <b>array</b> of
	 * ArrayList(s)
	 * 
	 * Note that the blob allocator is not guaranteed to return an allocation
	 * block since it may only contain free entries that are too small for the
	 * request.
	 */
	boolean allocCurrent = false;

	public int alloc(int size) {
		if (allocCurrent) {
			throw new Error("Nested allocation .. WHY!");
		}

		allocCurrent = true;

		ArrayList list;
		Allocator allocator = null;
		int i = 0;
		int addr = 0;

		int cmp = MIN_FIXED_ALLOC;
		while (size > cmp) {
			i++;
			cmp *= 2;
		}

		list = m_freeFixed[i];
		if (list.size() == 0) {
			allocator = new FixedAllocator(cmp, m_preserveSession, m_writes);
			allocator.setFreeList(list);
			allocator.setIndex(m_allocs.size());

			addr = allocator.alloc(this, size);

			m_allocs.add(allocator);
		} else {
			allocator = (Allocator) list.get(0);
			addr = allocator.alloc(this, size);
		}

		if (!m_commitList.contains(allocator)) {
			m_commitList.add(allocator);
		}

		if (!allocator.hasFree()) {
			list.remove(allocator);
		}

		allocCurrent = false;

		return addr;
	}

	/****************************************************************************
	 * The base realloc method that returns a stream for writing to rather than
	 * handle the reallocation immediately.
	 **/
	public PSOutputStream realloc(long oldAddr) {
		free(oldAddr);

		return PSOutputStream.getNew(this);
	}

	/****************************************************************************
	 * Called by PSOutputStream to make to actual allocation.
	 **/
	public long alloc(byte buf[], int size) {

		if (size > 4096 && size < 8188) { // frig for blob link blocks
			size = 8192;

			buf[8188] = 0;
			buf[8189] = 0;
			buf[8190] = 0;
			buf[8191] = 0;
		}

		int newAddr = alloc(size);

		m_writes.addWrite(physicalAddress(newAddr), buf, size);

		return newAddr;
	}

	/****************************************************************************
	 * Fixed buffer size reallocation
	 **/
	public long realloc(long oldAddr, byte buf[]) {
		free(oldAddr);

		return alloc(buf, buf.length);
	}

	synchronized public void startTransaction() {
		if (m_committing) {
			return;
		}

		m_transactionCount++;
	}

	synchronized public void commitTransaction() {
		if (m_committing) {
			return;
		}

		if (--m_transactionCount <= 0) {
			commitChanges();

			m_transactionCount = 0;
		}
	}

	public int getTransactionCount() {
		return m_transactionCount;
	}

	// --------------------------------------------------------------------------------------------
	// rollbackTransaction
	//
	// clear write buffer
	// read in last committed header
	synchronized public void rollbackTransaction() {
		if (m_transactionCount > 0 || m_readOnly) { // hack for resync
			baseInit();

			try {
				m_writes.clear();

				readFileSpec();
				readHdr();
				readAllocationBlocks();
			} catch (Exception e) {
				throw new StorageTerminalError(
						"Unable to rollback transaction", e);
			}
		}
	}

	synchronized public boolean isActiveTransaction() {
		return m_transactionCount > 0;
	}

	private void readHdr() throws IOException {
		byte buf[] = new byte[m_headerSize];

		m_file.seek(m_curHdrAddr);
		m_file.readFully(buf);

		DataInputStream str = new DataInputStream(new ByteArrayInputStream(buf));
		m_nextAllocation = str.readInt();
		m_rootAddr = str.readInt();

		m_maxFileSize = str.readInt();

		if (m_vers >= 3.0f) {
			m_metaBitsSize = str.readInt();

			// System.out.println("MetabitsSize: " + m_metaBitsSize);
		} else {
			m_metaBitsSize = 128; // fixed size, pre 3.0
		}

		m_metaBits = new int[m_metaBitsSize];

		for (int i = 0; i < m_metaBitsSize; i++) {
			m_metaBits[i] = str.readInt();
		}
		m_metaTransientBits = (int[]) m_metaBits.clone();
	}

	private int calcHdrSize() {
		return (m_metaBitsSize * 4) + 20;
	}

	private int calcHdrAllocSize() {
		int req = calcHdrSize();

		if (req <= cMinHdrSize) { // 2 * 2048 can be stored in first 8k
			req = cMinHdrSize;
		} else { // thereafter must be stored in mod 8K globally allocated block
			req = 8192 + ((req / 8192) * 8192);
		}

		// return convertFromAddr(req);

		return req;
	}

	private void writeHdr() throws IOException {
		// System.out.println("Writing Header: " + calcHdrSize());

		byte buf[] = new byte[calcHdrSize()];
		FixedOutputStream str = new FixedOutputStream(buf);

		str.writeInt(m_nextAllocation);
		str.writeInt(m_rootAddr);
		str.writeInt(m_maxFileSize);

		str.writeInt(m_metaBitsSize);

		for (int i = 0; i < m_metaBitsSize; i++) {
			str.writeInt(m_metaBits[i]);
		}

		str.flush();

		m_writes.addWrite(m_curHdrAddr, buf, buf.length);
	}

	static final float s_version = 3.0f;

	protected void writeFileSpec() {
		try {
			m_file.seek(0);
			m_file.writeLong(m_curHdrAddr);
			m_file.writeLong(m_altHdr1);
			m_file.writeLong(m_altHdr2);
			m_file.writeInt(m_fileSize);
			m_file.writeInt(m_metaStartAddr);
			m_file.writeFloat(s_version);
			m_file.writeInt(m_headerSize);
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to write file spec", e);
		}
	}

	float m_vers = 0.0f;

	protected void readFileSpec() {
		try {
			m_file.seek(0);
			m_curHdrAddr = m_file.readLong();

			m_altHdr1 = m_file.readLong();
			m_altHdr2 = m_file.readLong();

			m_fileSize = m_file.readInt();
			m_metaStartAddr = m_file.readInt();

			m_vers = m_file.readFloat();

			if (m_vers != s_version) {
				String msg = "Incorrect store version : " + m_vers
						+ " expects : " + s_version;

				throw new IOException(msg);
			} else {
				m_headerSize = m_file.readInt();
			}

		} catch (IOException e) {
			throw new StorageTerminalError("Unable to read file spec", e);
		}
	}

	public String getVersionString() {
		return "RWStore " + s_version;
	}

	public void commitChanges() {
		try {
			// Commit Callback?
			synchronized (this) {
				m_committing = true;

				if (m_commitCallback != null) {
					m_commitCallback.commitCallback();
				}

				// Reserve meta allocation - is this necessary? Not sure anymore

				if (m_altHdr1 == 0 || m_altHdr2 == 0) {
					throw new Error("Meta Allocation Error");
				}

				m_curHdrAddr = m_curHdrAddr == m_altHdr1 ? m_altHdr2
						: m_altHdr1;

				// save allocation headers
				Iterator iter = m_commitList.iterator();
				while (iter.hasNext()) {
					Allocator allocator = (Allocator) iter.next();

					metaFree(allocator.getDiskAddr());
					allocator.setDiskAddr(metaAlloc());

					byte[] buf = allocator.write();
					m_writes.addWrite(allocator.getDiskAddr(), buf, buf.length);
				}
				m_commitList.clear();

				// Write Header
				// Write out fileSize, meta_StartAddr, metaAllocBits
				writeHdr();

				m_writes.flush();

				// Now write at address 0 the address of the header and we're
				// done
				writeFileSpec();

				m_metaTransientBits = (int[]) m_metaBits.clone();

				if (m_commitCallback != null) {
					m_commitCallback.commitComplete();
				}

				m_file.getChannel().force(false);
			}
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to commit transaction", e);
		} finally {
			m_committing = false;
		}
	}

	// Header Data
	private long m_curHdrAddr = 0;
	private long m_altHdr1 = 2048;
	private long m_altHdr2 = 4096;
	private int m_rootAddr;

	private int m_fileSize;
	private int m_nextAllocation;
	private int m_maxFileSize;

	private int m_headerSize = 2048;

	// Meta Allocator
	private static int cDefaultMetaBitsSize = 16; // DEBUG FIX ME
	// private static int cDefaultMetaBitsSize = 128;
	private int m_metaBits[];
	private int m_metaBitsSize = cDefaultMetaBitsSize;
	private int m_metaTransientBits[];
	private int m_metaStartAddr;

	protected int allocBlock(int size) {
		// minimum 1
		if (size <= 0) {
			System.out.println("allocBlock fixing zero or less request: "
					+ size);

			size = 1;
		}

		int allocAddr = m_nextAllocation;
		m_nextAllocation -= size;

		while (convertAddr(m_nextAllocation) >= convertAddr(m_metaStartAddr)) {
			extendFile();
		}

		return allocAddr;
	}

	/**
	 * meta allocation/free
	 * 
	 * Allocates persistant store for allocation blocks.
	 * 
	 * grows data from the top to the file, e.g. bit 0 is 1024 from end-of-file.
	 * 
	 * If metaStart <= nextAllocation, then the file must be extended. All the
	 * allocation blocks are moved to the new end of file area, and the
	 * metaStartAddress is incremented by the same delta value.
	 * 
	 * NB the metaStart calculation uses an address rounded to 8k, so on
	 * extension the new metaStart may be up to 8K less than the true start
	 * address.
	 */
	long metaAlloc() {
		long lmetaStart = convertAddr(m_metaStartAddr);
		long lnextAlloc = convertAddr(m_nextAllocation);

		if (lmetaStart <= lnextAlloc) {
			throw new Error("Allocation ScrewUP!!!");
		}

		int bit = fndBit(m_metaTransientBits, m_metaBitsSize);

		if (bit < 0) {
			// reallocate metaBits and recalculate m_headerSize
			// extend m_metaBits by 16 ints!
			int nsize = m_metaBitsSize + 16;

			// System.out.println("New MetaBits: " + nsize);

			int[] nbits = new int[nsize];
			int[] ntransients = new int[nsize];

			for (int i = 0; i < m_metaBitsSize; i++) {
				nbits[i] = m_metaBits[i];
				ntransients[i] = m_metaTransientBits[i];
			}
			for (int i = m_metaBitsSize; i < nsize; i++) {
				nbits[i] = 0;
				ntransients[i] = 0;
			}

			m_metaBitsSize = nsize;
			m_metaBits = nbits;
			m_metaTransientBits = ntransients;

			// update header size
			int nhdrsize = calcHdrAllocSize();

			// System.out.println("Calc Header Size: " + nhdrsize +
			// ", Current: " + m_headerSize);

			if (nhdrsize != m_headerSize) {
				// System.out.println("New Header Size: " + nhdrsize);

				// header is not in re-allocatable memory
				int allocSize = convertFromAddr(nhdrsize);

				int naddr = allocBlock(allocSize);
				// System.out.println("allocBlock(" + allocSize + ") -> " +
				// naddr + " offset: " + getOffset(naddr));

				m_altHdr1 = convertAddr(naddr); // allocated on 8K boundary -
												// max addressable is 8K * 2G =
												// 16TB
				m_altHdr2 = convertAddr(allocBlock(allocSize));

				m_curHdrAddr = m_altHdr1;

				m_headerSize = nhdrsize;
			}

			// now get new allocation!
			bit = fndBit(m_metaTransientBits, m_metaBitsSize);
		}

		setBit(m_metaTransientBits, bit);
		setBit(m_metaBits, bit);

		long addr = metaBit2Addr(bit);

		if (addr < lmetaStart) {

			m_metaStartAddr = convertFromAddr(addr);

			lmetaStart = convertAddr(m_metaStartAddr);
		}

		if (lmetaStart <= lnextAlloc) {
			long oldAddr = addr;
			long oldMetaStart = lmetaStart;

			extendFile();

			addr = metaBit2Addr(bit);
		}

		return addr;
	}

	void metaFree(long addr) {
		if (addr != 0) {
			int bit = addr2metaBit(addr);

			if (tstBit(m_metaBits, bit)) {
				clrBit(m_metaBits, bit);
			} else {
				clrBit(m_metaTransientBits, bit);
			}
		}
	}

	long metaBit2Addr(int bit) {
		return convertAddr(m_fileSize) - (1024 * (bit + 1));
	}

	int addr2metaBit(long addr) {
		long val = ((convertAddr(m_fileSize) - addr) / 1024) - 1;

		return (int) val;
	}

	/*
	 * clear
	 * 
	 * reset the file size and
	 */
	public void clear() {
		try {
			baseInit();

			m_fileSize = -4;
			m_metaStartAddr = m_fileSize;
			m_nextAllocation = -1; // keep on a 8K boundary (8K minimum
									// allocation)
			m_file.setLength(convertAddr(m_fileSize));

			m_curHdrAddr = 0;
			m_altHdr1 = 0;
			m_altHdr2 = 0;
			m_rootAddr = 0;

			startTransaction();
			commitTransaction();
		} catch (Exception e) {
			throw new StorageTerminalError("Unable to clear store", e);
		}
	}

	public static long convertAddr(int addr) {
		long laddr = addr;

		if (laddr < 0) {
			return (-laddr) << 13;
		} else {
			return laddr & 0xFFFFFFF0;
		}
	}

	public int convertFromAddr(long addr) {
		return (int) -(addr >> 13);
	}

	/**
	 * extendFile will extend by 10% and round up to be a multiple of 16k
	 * 
	 * The allocation blocks must also be moved. Note that it would be a bad
	 * idea if these were moved with an overlapping copy!
	 * 
	 * After moving the physical memory the in-memory allocation blocks must
	 * then be updated with their new posiiton.
	 * 
	 * Note that since version 3.0 the size of the metaBits is variable. This
	 * must be taken into consideration when moving data. - Has the location
	 * changed as a result of the "reallocation". If this is incorrect then the
	 * wrong commit blocks will be copied, resulting in a corrupt data file.
	 * 
	 * There are two approaches to this problem. The first is only to copy the
	 * known committed (written) allocation blocks - but this cannot be implied
	 * by "zero'd" bits since this can indicate that memory has been cleared.
	 */
	private void extendFile() {
		int adjust = 0;

		m_writes.flush();

		try {
			long fromAddr = convertAddr(m_fileSize);
			long oldMetaStart = convertAddr(m_metaStartAddr);

			// adjust = -2
			adjust = -120 + (m_nextAllocation - m_metaStartAddr)
					+ (m_fileSize - m_metaStartAddr) + (m_fileSize / 10);

			StringBuffer dbg = new StringBuffer("Extend file, adjust: "
					+ adjust);
			dbg.append("m_nextAllocation: " + m_nextAllocation);
			dbg.append(", m_metaStartAddr: " + m_metaStartAddr);
			dbg.append(", m_fileSize: " + m_fileSize);

			// System.out.println(dbg.toString()); // DEBUG!!

			if (adjust > 0) {
				throw new Error("Extend file is NEGATIVE!!");
			}

			m_fileSize += adjust;
			m_metaStartAddr += adjust;

			if (getMaxFileSize() < m_fileSize) {
				// whoops!! How to exit more gracefully?
				throw new Error("System greater than maximum size");
			}

			// System.out.println("Extending file from: " + fromAddr + " to " +
			// convertAddr(m_fileSize));

			m_file.setLength(convertAddr(m_fileSize));

			long toAddr = convertAddr(m_fileSize);
			byte buf[] = new byte[1024];
			while (fromAddr > oldMetaStart) {
				fromAddr -= 1024;
				toAddr -= 1024;
				m_file.seek(fromAddr);
				m_file.readFully(buf);
				m_file.seek(toAddr);
				m_file.write(buf);
			}
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to extend the file", e);
		}

		// adjust alloc offsets
		ArrayList list = m_allocs;
		Iterator iter = m_allocs.iterator();
		while (iter.hasNext()) {
			Allocator alloc = (Allocator) iter.next();
			if (alloc.getDiskAddr() > 0) {
				alloc.setDiskAddr(alloc.getDiskAddr() + convertAddr(adjust));
			}
		}

		// ensure file structure is right
		writeFileSpec();
	}

	static void setBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		bits[(int) index] |= 1 << bit;
	}

	static boolean tstBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		return (bits[(int) index] & 1 << bit) != 0;
	}

	static void clrBit(int[] bits, int bitnum) {
		int index = bitnum / 32;
		int bit = bitnum % 32;

		int val = bits[index];

		val &= ~(1 << bit);

		bits[index] = val;
	}

	static int fndBit(int[] bits, int size) {
		for (int i = 0; i < size; i++) {
			if (bits[i] != 0xFFFFFFFF) {
				for (int k = 0; k < 32; k++) {
					if ((bits[i] & (1 << k)) == 0) {
						return (i * 32) + k;
					}
				}
			}
		}

		return -1;
	}

	// --------------------------------------------------------------------------------------
	private String allocListStats(ArrayList list) {
		String stats = "";
		Iterator iter = list.iterator();
		while (iter.hasNext()) {
			stats = stats + ((Allocator) iter.next()).getStats();
		}

		return stats;
	}

	protected static int s_allocation = 0;

	public String getStats(boolean full) {
		s_allocation = 0;

		String stats = "FileSize : " + m_fileSize + " allocated : "
				+ m_nextAllocation + " meta data : "
				+ (m_fileSize - m_metaStartAddr) + "\r\n";

		if (full) {
			stats = stats + allocListStats(m_allocs);

			stats = stats + "Allocated : " + s_allocation;
		}

		return stats;
	}

	public ArrayList getStorageBlockAddresses() {
		ArrayList addrs = new ArrayList();

		Iterator allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			Allocator alloc = (Allocator) allocs.next();
			alloc.addAddresses(addrs);
		}

		return addrs;
	}

	// --------------------------------------------------------------------------------------

	public boolean verify(long laddr) {
		int addr = (int) laddr;

		if (addr == 0) {
			return false;
		}

		return getBlockByAddress(addr) != null;
	}

	/*****************************************************************************
	 * Address transformation
	 * 
	 * latched2Physical
	 **/
	public long physicalAddress(int addr) {
		if (addr > 0) {
			return addr & 0xFFFFFFE0;
		} else {
			long laddr = getBlock(addr).getPhysicalAddress(getOffset(addr));

			return laddr;
		}
	}

	/********************************************************************************
	 * handle dual address format, if addr is positive then it is the physical
	 * address, so the Allocators must be searched.
	 **/
	private Allocator getBlockByAddress(int addr) {
		if (addr < 0) {
			return getBlock(addr);
		}

		Iterator allocs = m_allocs.iterator();

		Allocator alloc = null;
		while (allocs.hasNext()) {
			alloc = (Allocator) allocs.next();

			if (alloc.addressInRange(addr)) {
				break;
			}
			alloc = null;
		}

		return alloc;
	}

	private Allocator getBlock(int addr) {
		int index = (-addr) >>> 16;

		return (Allocator) m_allocs.get(index);
	}

	private int getOffset(int addr) {
		return (-addr) & 0xFFFF;
	}

	public int addr2Size(int addr) {
		if (addr > 0) {
			int size = 0;

			int index = ((int) addr) % 16;

			if (index == 15) { // blob
				throw new Error(
						"FIX ME : legacy BLOB code being accessed somehow");
			} else {
				size = MIN_FIXED_ALLOC;

				for (int muls = 0; muls < index; muls++) {
					size *= 2;
				}
			}

			return size;
		} else {
			return getBlock(addr).getPhysicalSize(getOffset(addr));
		}
	}

	public boolean isNativeAddress(long addr) {
		return addr <= 0;
	}

	/*******************************************************************************
	 * called when used as a server, returns whether facility is enabled, this
	 * is the whole point of the wormStore - so the answer is true
	 **/
	public boolean preserveSessionData() {
		m_preserveSession = true;

		Iterator allocs = m_allocs.iterator();
		while (allocs.hasNext()) {
			((Allocator) allocs.next()).preserveSessionData();
		}
		return true;
	}

	/*******************************************************************************
	 * called by allocation blocks to determine whether they can re-allocate
	 * data within this session.
	 **/
	protected boolean isSessionPreserved() {
		return m_preserveSession;
	}

	/*********************************************************************
	 * create backup file, copy data to it, and close it.
	 **/
	synchronized public void backup(String filename)
			throws FileNotFoundException, IOException {
		File destFile = new File(filename);
		destFile.createNewFile();

		RandomAccessFile dest = new RandomAccessFile(destFile, "rw");

		int bufSize = 64 * 1024;
		byte[] buf = new byte[bufSize];

		m_file.seek(0);

		int rdSize = bufSize;
		while (rdSize == bufSize) {
			rdSize = m_file.read(buf);
			if (rdSize > 0) {
				dest.write(buf, 0, rdSize);
			}
		}

		dest.close();
	}

	/*********************************************************************
	 * copy storefile to output stream.
	 **/
	synchronized public void backup(OutputStream outstr) throws IOException {
		int bufSize = 64 * 1024;
		byte[] buf = new byte[bufSize];

		m_file.seek(0);

		int rdSize = bufSize;
		while (rdSize == bufSize) {
			rdSize = m_file.read(buf);
			if (rdSize > 0) {
				outstr.write(buf, 0, rdSize);
			}
		}
	}

	synchronized public void restore(InputStream instr) throws IOException {
		int bufSize = 64 * 1024;
		byte[] buf = new byte[bufSize];

		m_file.seek(0);

		int rdSize = bufSize;
		while (rdSize == bufSize) {
			rdSize = instr.read(buf);
			if (rdSize > 0) {
				m_file.write(buf, 0, rdSize);
			}
		}
	}

	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public void absoluteWriteInt(int addr, int offset, int value) {
		try {
			// must check write cache!!, or the write may be overwritten - just
			// flush for now
			m_writes.flush();

			m_file.seek(physicalAddress(addr) + offset);
			m_file.writeInt(value);
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to write integer", e);
		}
	}

	/***************************************************************************************
	 * Needed to free Blob chains.
	 **/
	public int absoluteReadInt(int addr, int offset) {
		try {
			m_file.seek(physicalAddress(addr) + offset);
			return m_file.readInt();
		} catch (IOException e) {
			throw new StorageTerminalError("Unable to write integer", e);
		}
	}

	/***************************************************************************************
	 * Needed by PSOutputStream for BLOB buffer chaining.
	 **/
	public int bufferChainOffset() {
		return 8188; // 8192 - 4
	}

	/*********************************************************************************************
	 * Retrieves files associated with the persistent environment of this Store.
	 * This supports content management w/o the need to store the files as BLOB
	 * objects in the database.
	 * 
	 * @return the File object for the filename provided
	 **/
	public File getFile(String filename) {
		String dirname = m_fd.getName().replaceAll("[.]", "_") + "_files";

		File filedir = new File(m_fd.getParent(), dirname);
		filedir.mkdir();

		File thefile = new File(filedir, filename);

		return thefile;
	}

	public File getStoreFile() {
		return m_fd;
	}

	public boolean isLongAddress() {
		// always ints
		return false;
	}

	public int absoluteReadLong(long addr, int offset) {
		// TODO Auto-generated method stub
		return 0;
	}

	public void absoluteWriteLong(long addr, int threshold, long value) {
		// TODO Auto-generated method stub
	}

	public void absoluteWriteAddress(long addr, int threshold, long addr2) {
		absoluteWriteInt((int) addr, threshold, (int) addr2);
	}

	public int getAddressSize() {
		// TODO Auto-generated method stub
		return 4;
	}

	// DiskStrategy Support
	public RandomAccessFile getRandomAccessFile() {
		return m_file;
	}
	public FileChannel getChannel() {
		return m_file.getChannel();
	}
}

/*****************************************************************
 * 8 Python test stuff
 * 
 * from cutthecrap.gpo.client import OMClient from cutthecrap.gpo import GPOMap
 * import sys;
 * 
 * client = OMClient("cutthecrap.oms.ObjectManager", "D:/testdb/newrw.rw"); om =
 * client.getObjectManager();
 * 
 * om.registerClassifier("parent", "name");
 * 
 * p = GPOMap(om);
 * 
 * def addLoads(i, t): while t > 0: om.startNativeTransaction(); j = i; while j
 * > 0: g = GPOMap(om); g.set("name", str(i) + "_gpo"); g.set("parent", p); j =
 * j -1; om.commitNativeTransaction(); t = t - 1;
 * 
 * 
 * # This seems to fail! addLoads(1000, 1000); addLoads(100000, 100);
 **/
