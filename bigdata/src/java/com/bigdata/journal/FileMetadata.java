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
package com.bigdata.journal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.Banner;
import com.bigdata.config.Configuration;
import com.bigdata.config.IValidator;
import com.bigdata.config.IntegerRangeValidator;
import com.bigdata.config.LongRangeValidator;
import com.bigdata.io.DirectBufferPool;
import com.bigdata.io.FileChannelUtility;
import com.bigdata.io.IReopenChannel;
import com.bigdata.io.writecache.WriteCacheService;
import com.bigdata.quorum.Quorum;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.util.ChecksumUtility;

/**
 * Helper object used when opening or creating journal file in any of the
 * file-based modes.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FileMetadata {

	/**
	 * Logger.
	 */
	protected static final Logger log = Logger.getLogger(FileMetadata.class);
	protected static final boolean INFO = log.isInfoEnabled();
	protected static final boolean DEBUG = log.isDebugEnabled();

	static final int SIZE_MAGIC = Bytes.SIZEOF_INT;
	static final int SIZE_VERSION = Bytes.SIZEOF_INT;
	static final int SIZEOF_ROOT_BLOCK = RootBlockView.SIZEOF_ROOT_BLOCK;

	/**
	 * Magic value for journal (the root blocks have their own magic value).
	 */
	final int MAGIC = 0xe6b4c275;

	/**
	 * Journal version number (version 1). @todo What relation is there between
	 * this and version info in the root blocks?
	 */
	final int VERSION1 = 0x1;

	/**
	 * The file that was opened.
	 */
	final File file;

	/**
	 * The mode used to open the file.
	 */
	final String fileMode;

	/**
	 * The buffer mode used to open that file.
	 */
	final BufferMode bufferMode;

    /**
     * The interface for IO performed on that file.
     * <p>
     * Note: this field is only safe for use during the initial file
     * create/open. It is not safe to use once a file has been closed, whether
     * directly or by an interrupt during an IO operation.
     */
	RandomAccessFile raf;

	/**
	 * The 32-bit magic value at offset 0L in the file.
	 * 
	 * @see #MAGIC
	 */
	final int magic;

	/**
	 * The 32-bit version number at offset 4 in the file.
	 */
	final int version;

	/**
	 * The extent of the file in bytes.
	 */
	final long extent;

	/**
	 * The extent of the user data space (everything after the root blocks).
	 */
	final long userExtent;

	/**
	 * The #of bits out of a 64-bit long integer that are used to encode the
	 * byte offset as an unsigned integer. The remaining bits are used to encode
	 * the byte count (aka record length) as an unsigned integer.
	 */
	final int offsetBits;

	// /**
	// * The #of records to be buffered in an optional read cache -or- ZERO(0)
	// to
	// * disable the read cache.
	// */
	// final int readCacheCapacity;
	//    
	// /**
	// * The maximum size of a record that will be allowed into the read cache.
	// */
	// final int readCacheMaxRecordSize;

	/**
	 * When <code>true</code> the write cache will be enabled.
	 */
	final boolean writeCacheEnabled;

	/**
	 * The #of write cache buffers to be used if the write cache is enabled.
	 */
	public final int writeCacheBufferCount;

	/**
	 * <code>true</code> iff record level checksums are enabled. These are used
	 * beginning with {@link RootBlockView#VERSION2} and for the
	 * {@link RWStrategy} from {@link RootBlockView#VERSION1}.
	 */
	final boolean useChecksums;

	/**
	 * The next offset at which a record would be written on the store. The
	 * offset is relative to the start of the user data space. Offset zero(0)
	 * addresses the first byte after the root blocks.
	 */
	final long nextOffset;

	/**
	 * True iff the file was opened in a read-only mode.
	 */
	final boolean readOnly;

	/**
	 * The timestamp from the createTime field in the root block.
	 */
	final long createTime;

	/**
	 * The timestamp from the closeTime field in the root block.
	 */
	final long closeTime;

	/**
	 * Offset of the first root block in the file.
	 */
	static final int OFFSET_ROOT_BLOCK0 = SIZE_MAGIC + SIZE_VERSION;

	/**
	 * Offset of the second root block in the file.
	 */
	static final int OFFSET_ROOT_BLOCK1 = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 1);

	/**
	 * The size of the journal header, including MAGIC, version, and both root
	 * blocks. This is used as an offset when computing the index of a record in
	 * the journal.
	 */
	static final int headerSize0 = SIZE_MAGIC + SIZE_VERSION + (SIZEOF_ROOT_BLOCK * 2);

	/**
	 * Depending on the mode, this will be either a direct buffer, a mapped
	 * buffer or [null] if no buffer is being used.
	 */
	final ByteBuffer buffer;

	/**
	 * True iff the file was pre-existing.
	 */
	final boolean exists;

//    /**
//     * The object used by the journal to compute the checksums of its root
//     * blocks (this object is NOT thread-safe so there is one instance per
//     * journal).
//     */
//    final private ChecksumUtility checker = new ChecksumUtility();

    /**
	 * The 1st root block.
	 */
	protected IRootBlockView rootBlock0;

	/**
	 * The 2nd root block.
	 */
	protected IRootBlockView rootBlock1;

	/**
	 * The current root block. For a new file, this is "rootBlock0". For an
	 * existing file it is based on an examination of both root blocks.
	 */
	final IRootBlockView rootBlock;
	
	/**
	 * Properties used to initialize this FileMetadata object
	 */
	final Properties properties;

	/**
	 * This constructor handles the case where the file does not exist or exists
	 * but is empty (including files created by the temporary file creation
	 * mechanism).
	 * 
	 * @param file
	 *            The name of the file to be opened.
	 * @param bufferMode
	 *            The {@link BufferMode}.
	 * @param useDirectBuffers
	 *            true if a buffer should be allocated using
	 *            {@link ByteBuffer#allocateDirect(int)} rather than
	 *            {@link ByteBuffer#allocate(int)}. This has no effect for the
	 *            {@link BufferMode#Disk} and {@link BufferMode#Mapped} modes.
	 * @param initialExtent
	 *            The initial extent of the journal. The size of the journal is
	 *            automatically increased up to the <i>maximumExtent</i> on an
	 *            as necessary basis.
	 * @param maximumExtent
	 *            The maximum extent of the journal before it will
	 *            {@link Journal#overflow()}.
	 * @param create
	 *            When true, the file is created if it does not exist (this is
	 *            ignored for {@link BufferMode#Temporary} files since they are
	 *            created lazily if at all).
	 * @param isEmptyFile
	 *            This flag must be set when the temporary file mechanism is
	 *            used to create a new temporary file otherwise an empty file is
	 *            treated as an error since it does not contain valid root
	 *            blocks.
	 * @param deleteOnExit
	 *            When set, a <em>new</em> file will be marked for deletion when
	 *            the VM exits. This may be used as part of a temporary store
	 *            strategy.
	 * @param readOnly
	 *            When true, the file is opened in a read-only mode and it is an
	 *            error if the file does not exist.
	 * @param forceWrites
	 *            When true, the file is opened in "rwd" mode and individual IOs
	 *            are forced to disk. This option SHOULD be false since we only
	 *            need to write through to disk on commit, not on each IO.
	 * @param offsetBits
	 *            The #of bits out of a 64-bit long integer that are used to
	 *            encode the byte offset as an unsigned integer. The remaining
	 *            bits are used to encode the byte count (aka record length) as
	 *            an unsigned integer. This value is <em>ignored</em> if the
	 *            journal is being reopened, in which case the real offset bits
	 *            is read from the root block of the journal.
	 * @param writeCacheEnabled
	 *            When <code>true</code>, the {@link DiskOnlyStrategy} will
	 *            allocate a direct {@link ByteBuffer} from the
	 *            {@link DirectBufferPool} to service as a write cache.
	 * @param writeCacheBufferCount
	 *            The #of buffers to allocate for the {@link WriteCacheService}.
	 * @param validateChecksum
	 *            When <code>true</code>, the checksum stored in the root blocks
	 *            of an existing file will be validated when the file is opened.
	 *            See {@link Options#VALIDATE_CHECKSUM}.
	 * @param createTime
	 *            The create time to be assigned to the root block iff a new
	 *            file is created.
	 * @param quorumToken
	 *            The current quorum token or {@link Quorum#NO_QUORUM} if the
	 *            caller is not met with any {@link Quorum}.
	 * @param alternateRootBlock
	 *            When <code>true</code> the prior root block will be used. This
	 *            option may be used when a commit record is valid but the data
	 *            associated with the commit point is invalid. There are two
	 *            root blocks. Normally the one which has been most recently
	 *            written will be loaded on restart. When this option is
	 *            specified, the older of the two root blocks will be loaded
	 *            instead. <strong>If you use this option and then do a commit
	 *            then the more recent of the root blocks will be lost and any
	 *            data associated with that commit point will be lost as
	 *            well!</strong>
	 * 
	 * @throws RuntimeException
	 *             if there is a problem preparing the file for use by the
	 *             journal.
	 */
	FileMetadata(final File file, final BufferMode bufferMode,
			final boolean useDirectBuffers, final long initialExtent,
			final long maximumExtent, final boolean create,
			final boolean isEmptyFile, boolean deleteOnExit,
			final boolean readOnly, final ForceEnum forceWrites,
			final int offsetBits, final boolean writeCacheEnabled,
			final int writeCacheBufferCount, final boolean validateChecksum,
			final long createTime, final long quorumToken,
			final boolean alternateRootBlock,
			final Properties properties) throws RuntimeException {

		if (file == null)
			throw new IllegalArgumentException();

		if (bufferMode == null)
			throw new IllegalArgumentException();

		if (bufferMode == BufferMode.Transient) {

			// This mode is not a valid option in this context.
			throw new IllegalArgumentException();

		}

        if (file.exists() && file.length() != 0)
            throw new IllegalArgumentException("File exists and is not empty: "
                    + file.getAbsolutePath());

        if (readOnly && create) {

			throw new IllegalArgumentException("'" + Options.CREATE + "' may not be used with '" + Options.READ_ONLY
					+ "'");

		}

		if (readOnly && forceWrites != ForceEnum.No) {

			throw new IllegalArgumentException("'" + Options.FORCE_WRITES + "'='" + forceWrites
					+ "' may not be used with '" + Options.READ_ONLY + "'");

		}

		// check the argument. the value is only used if we are creating a new
		// journal.
		WormAddressManager.assertOffsetBits(offsetBits);

		this.bufferMode = bufferMode;

		// this.offsetBits = offsetBits;

		// this.readCacheCapacity = readCacheCapacity;
		//        
		// this.readCacheMaxRecordSize = readCacheMaxRecordSize;

		this.writeCacheEnabled = writeCacheEnabled;

		this.writeCacheBufferCount = writeCacheBufferCount;

		this.fileMode = (readOnly ? "r" : forceWrites.asFileMode());

		this.readOnly = readOnly;

		this.exists = false;

		this.properties = properties;

		// true for a temporary file
		final boolean temporary = bufferMode.equals(BufferMode.Temporary);

		if (temporary) {

			// override for temporary files.
			deleteOnExit = true;

		}

		this.file = file;

		if (exists && !temporary) {

			if (INFO)
				log.info("Opening existing file: " + file.getAbsoluteFile());

		} else {

			if (readOnly) {

				throw new RuntimeException("File does not exist and '" + Options.READ_ONLY + "' was specified: "
						+ file.getAbsoluteFile());

			}

			if (!create && !isEmptyFile) {

				throw new RuntimeException("File does not exist and '" + Options.CREATE + "' was not specified: "
						+ file.getAbsoluteFile());

			}

			/*
			 * Note: a temporary file that does not exist is created _lazily_.
			 * See below and DiskOnlyStrategy.
			 */
			if (INFO)
				log.info("Backing file: exists=" + exists + ", temporary=" + temporary + ", create=" + create
						+ ", readOnly=" + readOnly + ", file=" + file.getAbsoluteFile());

		}

		try {

			/*
			 * Open/create the file (temporary files are not opened/created
			 * eagerly).
			 */
			// this.raf = temporary ? null : FileLockUtility.openFile(file,
			// fileMode,
			// bufferMode != BufferMode.Mapped);
			if (!temporary) {

				/*
				 * Open / create and obtain shared/exclusive lock if possible.
				 * Sets [raf] as a side-effect.
				 */
				opener.reopenChannel();

			}

//			if (exists && !temporary) {
//
//				/*
//				 * The file already exists (but not for temporary files).
//				 * 
//				 * Note: this next line will throw IOException if there is a
//				 * file lock contention.
//				 * 
//				 * Note: [raf] was initialized by [opener.reopenChannel()]
//				 * above!
//				 */
//				this.extent = raf.length();
//
//				this.userExtent = extent - headerSize0;
//
//				if (this.extent <= headerSize0) {
//
//					/*
//					 * By throwing an exception for files that are not large
//					 * enough to contain the MAGIC, VERSION, and both root
//					 * blocks we avoid IO errors when trying to read those data
//					 * and are able to reject files based on whether they have
//					 * bad magic, version, or root blocks.
//					 */
//
//					throw new RuntimeException("File too small to contain a valid journal: " + file.getAbsoluteFile());
//
//				}
//
//				// if( bufferMode != BufferMode.Disk ) {
//				if (bufferMode.isFullyBuffered()) {
//
//					/*
//					 * Verify that we can address this many bytes with this
//					 * strategy. The strategies that rely on an in-memory buffer
//					 * are all limited to the #of bytes that can be addressed by
//					 * an int32.
//					 */
//
//					AbstractBufferStrategy.assertNonDiskExtent(userExtent);
//
//				}
//
//				/*
//				 * Note: The code to read the MAGIC, VERSION, and root blocks is
//				 * shared by DumpJournal (code is copy by value) and in part by
//				 * the rollback() method on AbstractJournal.
//				 */
//
//				/*
//				 * Read the MAGIC and VERSION.
//				 */
//				raf.seek(0L);
//				try {
//					/*
//					 * Note: this next line will throw IOException if there is a
//					 * file lock contention.
//					 */
//					magic = raf.readInt();
//				} catch (IOException ex) {
//					throw new RuntimeException("Can not read magic. Is file locked by another process?", ex);
//				}
//				if (magic != MAGIC)
//					throw new RuntimeException("Bad journal magic: expected=" + MAGIC + ", actual=" + magic);
//				version = raf.readInt();
//				if (version != VERSION1)
//					throw new RuntimeException("Bad journal version: expected=" + VERSION1 + ", actual=" + version);
//
//				/*
//				 * Check root blocks (magic, timestamps), choose root block,
//				 * read constants (slotSize, segmentId).
//				 */
//				{
//
//					final ChecksumUtility checker = validateChecksum ? ChecksumUtility.threadChk
//							.get()
//							: null;
//
//					// final FileChannel channel = raf.getChannel();
//					final ByteBuffer tmp0 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
//					final ByteBuffer tmp1 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
//					FileChannelUtility.readAll(opener, tmp0, OFFSET_ROOT_BLOCK0);
//					FileChannelUtility.readAll(opener, tmp1, OFFSET_ROOT_BLOCK1);
//					tmp0.position(0); // resets the position.
//					tmp1.position(0);
//					try {
//						rootBlock0 = new RootBlockView(true, tmp0, checker);
//					} catch (RootBlockException ex) {
//						log.warn("Bad root block zero: " + ex);
//					}
//					try {
//						rootBlock1 = new RootBlockView(false, tmp1, checker);
//					} catch (RootBlockException ex) {
//						log.warn("Bad root block one: " + ex);
//					}
//					if (rootBlock0 == null && rootBlock1 == null) {
//						throw new RuntimeException(
//								"Both root blocks are bad - journal is not usable: "
//										+ file);
//					}
//					if (alternateRootBlock)
//						log.warn("Using alternate root block");
//					/*
//					 * Choose the root block based on the commit counter.
//					 * 
//					 * Note: The commit counters MAY be equal. This will happen
//					 * if we rollback the journal and override the current root
//					 * block with the alternate root block.
//					 */
//					final long cc0 = rootBlock0.getCommitCounter();
//					final long cc1 = rootBlock1.getCommitCounter();
//					this.rootBlock = (cc0 > cc1 ? (alternateRootBlock ? rootBlock1
//							: rootBlock0)
//							: (alternateRootBlock ? rootBlock0 : rootBlock1));
//				}
//
//				// use the offset bits from the root block.
//				this.offsetBits = rootBlock.getOffsetBits();
//
//				/*
//				 * The offset into the user extent at which the next record will
//				 * be written.
//				 */
//				this.nextOffset = rootBlock.getNextOffset();
//
//				this.createTime = rootBlock.getCreateTime();
//
//				this.closeTime = rootBlock.getCloseTime();
//
//				if (closeTime != 0L && !readOnly) {
//
//					throw new RuntimeException("Journal is closed for writes: closedTime=" + closeTime);
//
//				}
//
//				switch (bufferMode) {
//				case Direct: {
//					// Allocate the buffer buffer.
//					buffer = (useDirectBuffers ? ByteBuffer.allocateDirect((int) userExtent) : ByteBuffer
//							.allocate((int) userExtent));
//					// Setup to read data from file into the buffer.
//					if (nextOffset > Integer.MAX_VALUE) {
//						throw new RuntimeException("This file is too large for a buffered mode: use " + BufferMode.Disk);
//					}
//					buffer.limit((int) nextOffset);
//					buffer.position(0);
//					if (nextOffset > 0) {
//						// Read the file image into the direct buffer.
//						FileChannelUtility.readAll(opener, buffer, headerSize0);
//					}
//					break;
//				}
//				case Mapped: {
//					// Map the file.
//					boolean loadMappedFile = false; // @todo expose as property.
//					buffer = opener.reopenChannel().map(FileChannel.MapMode.READ_WRITE, headerSize0, extent);
//					if (loadMappedFile) {
//						/*
//						 * Load the image into mapped memory. Generally, I would
//						 * think that you are better off NOT loading the image.
//						 * When you want the image in memory, use the Direct
//						 * mode instead. It should be MUCH faster and has better
//						 * control over the amount and timing of the IO.
//						 */
//						((MappedByteBuffer) buffer).load();
//					}
//					break;
//				}
//				case Disk:
//					buffer = null;
//					break;
//				case DiskWORM:
//					buffer = null;
//					break;
//				case DiskRW:
//					buffer = null;
//					break;
//				default:
//					throw new AssertionError();
//				}
//
//				/*
//				 * Note: there should be no processing required on restart since
//				 * the intention of transactions that did not commit will not be
//				 * visible.
//				 */
//
//			} else {

			/*
			 * Create a new journal.
			 */

			if (deleteOnExit) {

				// Mark the file for deletion on exit.
				try {
					file.deleteOnExit();
				} catch (NullPointerException ex) {
					/*
					 * Ignore NPE caused by a known Sun bug.
					 * 
					 * See http://bugs.sun.com/view_bug.do?bug_id=6526376
					 */
				}

			}

			/*
			 * Set the initial extent.
			 * 
			 * Note: since a mapped file CAN NOT be extended, we pre-extend
			 * it to its maximum extent here.
			 */

			this.extent = (bufferMode == BufferMode.Mapped ? maximumExtent : initialExtent);

			this.userExtent = extent - headerSize0;

			// if (bufferMode != BufferMode.Disk
			// && bufferMode != BufferMode.Temporary ) {
			if (bufferMode.isFullyBuffered()) {

				/*
				 * Verify that we can address this many bytes with this
				 * strategy. The strategies that rely on an in-memory buffer
				 * are all limited to the #of bytes that can be addressed by
				 * an int32.
				 */

				AbstractBufferStrategy.assertNonDiskExtent(userExtent);

			}

			/*
			 * The offset at which the first record will be written. This is
			 * zero(0) since the buffer offset (0) is the first byte after
			 * the root blocks.
			 */
			nextOffset = 0;

			magic = MAGIC;

			version = VERSION1;

			if (!temporary) {

				/*
				 * Extend the file. We do this eagerly in an attempt to
				 * convince the OS to place the data into a contiguous
				 * region on the disk.
				 */
				raf.setLength(extent);

				/*
				 * Write the MAGIC and version on the file.
				 */
				raf.seek(0);
				raf.writeInt(MAGIC);
				raf.writeInt(VERSION1);

			}

			/*
			 * Generate the root blocks. They are for all practical purposes
			 * identical (in fact, their timestamps will be distinct). The
			 * root block are then written into their locations in the file.
			 */
			{

                final ChecksumUtility checker = ChecksumUtility.threadChk.get();

				// use the caller's value for offsetBits.
				this.offsetBits = offsetBits;
				final long commitCounter = 0L;
				final long firstCommitTime = 0L;
				final long lastCommitTime = 0L;
				final long commitRecordAddr = 0L;
				final long commitRecordIndexAddr = 0L;
				final UUID uuid = UUID.randomUUID(); // journal's UUID.
				final StoreTypeEnum stenum = bufferMode.getStoreType();
				if (createTime == 0L) {
					throw new IllegalArgumentException(
							"Create time may not be zero.");
				}
				this.createTime = createTime;
				this.closeTime = 0L;
				final IRootBlockView rootBlock0 = new RootBlockView(true,
						offsetBits, nextOffset, firstCommitTime,
						lastCommitTime, commitCounter, commitRecordAddr,
						commitRecordIndexAddr, uuid, quorumToken,//
						0L, 0L, stenum, createTime, closeTime, checker);
				final IRootBlockView rootBlock1 = new RootBlockView(false,
						offsetBits, nextOffset, firstCommitTime,
						lastCommitTime, commitCounter, commitRecordAddr,
						commitRecordIndexAddr, uuid, quorumToken,//
						0L, 0L, stenum, createTime, closeTime, checker);

				if (!temporary) {

					// FileChannel channel = raf.getChannel();

					FileChannelUtility.writeAll(opener, rootBlock0
							.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK0);

					FileChannelUtility.writeAll(opener, rootBlock1
							.asReadOnlyBuffer(), OFFSET_ROOT_BLOCK1);

					/*
					 * Force the changes to disk. We also force the file
					 * metadata to disk since we just changed the file size and
					 * we do not want to loose track of that.
					 */

					opener.reopenChannel().force(true);

				}

				this.rootBlock = rootBlock0;

			}
			
			switch (bufferMode) {
			case Direct:
				/*
				 * Allocate the buffer.
				 * 
				 * Note that we do not read in any data since no user data
				 * has been written and the root blocks are not cached in
				 * the buffer to avoid possible overwrites.
				 */
				buffer = (useDirectBuffers ? ByteBuffer.allocateDirect((int) userExtent) : ByteBuffer
						.allocate((int) userExtent));
				break;
			case Mapped:
				/*
				 * Map the file starting from the first byte of the user
				 * space and continuing through the entire user extent.
				 */
				if (INFO)
					log.info("Mapping file=" + file);
				buffer = opener.reopenChannel().map(FileChannel.MapMode.READ_WRITE, headerSize0, userExtent);
				break;
			case DiskRW:
				buffer = null;
				break;
			case Disk:
				buffer = null;
				break;
			case DiskWORM:
				buffer = null;
				break;
			case Temporary:
				buffer = null;
				break;
			default:
				throw new AssertionError();
			}

//			}

			this.useChecksums = useChecksums(rootBlock);

		} catch (IOException ex) {

			throw new RuntimeException("file=" + file, ex);

		}

	}

	/**
	 * This constructor handles cases where the file exists and is non-empty.
	 * 
	 * @param file
	 *            The name of the file to be opened.
	 * @param useDirectBuffers
	 *            true if a buffer should be allocated using
	 *            {@link ByteBuffer#allocateDirect(int)} rather than
	 *            {@link ByteBuffer#allocate(int)}. This has no effect for the
	 *            {@link BufferMode#Disk} and {@link BufferMode#Mapped} modes.
	 * @param readOnly
	 *            When true, the file is opened in a read-only mode and it is an
	 *            error if the file does not exist.
	 * @param forceWrites
	 *            When true, the file is opened in "rwd" mode and individual IOs
	 *            are forced to disk. This option SHOULD be false since we only
	 *            need to write through to disk on commit, not on each IO.
	 * @param writeCacheEnabled
	 *            When <code>true</code>, the {@link DiskOnlyStrategy} will
	 *            allocate a direct {@link ByteBuffer} from the
	 *            {@link DirectBufferPool} to service as a write cache.
	 * @param writeCacheBufferCount
	 *            The #of buffers to allocate for the {@link WriteCacheService}.
	 * @param validateChecksum
	 *            When <code>true</code>, the checksum stored in the root blocks
	 *            of an existing file will be validated when the file is opened.
	 *            See {@link Options#VALIDATE_CHECKSUM}.
	 * @param alternateRootBlock
	 *            When <code>true</code> the prior root block will be used. This
	 *            option may be used when a commit record is valid but the data
	 *            associated with the commit point is invalid. There are two
	 *            root blocks. Normally the one which has been most recently
	 *            written will be loaded on restart. When this option is
	 *            specified, the older of the two root blocks will be loaded
	 *            instead. <strong>If you use this option and then do a commit
	 *            then the more recent of the root blocks will be lost and any
	 *            data associated with that commit point will be lost as
	 *            well!</strong>
	 * 
	 * @throws RuntimeException
	 *             if there is a problem preparing the file for use by the
	 *             journal.
	 */
	FileMetadata(final File file, final boolean useDirectBuffers,
			final boolean readOnly, final ForceEnum forceWrites,
			final boolean writeCacheEnabled, final int writeCacheBufferCount,
			final boolean validateChecksum, final boolean alternateRootBlock,
			final Properties properties)
			throws RuntimeException {

		if (file == null)
			throw new IllegalArgumentException();

		if (!file.exists() || file.length() == 0)
			throw new IllegalArgumentException(
					"File does not exist or is empty: "
							+ file.getAbsolutePath());

		if (readOnly && forceWrites != ForceEnum.No) {

			throw new IllegalArgumentException("'" + Options.FORCE_WRITES + "'='" + forceWrites
					+ "' may not be used with '" + Options.READ_ONLY + "'");

		}

		this.writeCacheEnabled = writeCacheEnabled;

		this.writeCacheBufferCount = writeCacheBufferCount;

		this.fileMode = (readOnly ? "r" : forceWrites.asFileMode());
				
		this.readOnly = readOnly;

		this.exists = file.exists();

		this.file = file;
		
		this.properties = properties;

		try {

			/*
			 * Open / create and obtain shared/exclusive lock if possible. Sets
			 * [raf] as a side-effect.
			 */
			opener.reopenChannel();

			/*
			 * The file already exists (but not for temporary files).
			 * 
			 * Note: this next line will throw IOException if there is a file
			 * lock contention.
			 * 
			 * Note: [raf] was initialized by [opener.reopenChannel()] above!
			 */
			this.extent = raf.length();

			this.userExtent = extent - headerSize0;

			if (this.extent <= headerSize0) {

				/*
				 * By throwing an exception for files that are not large enough
				 * to contain the MAGIC, VERSION, and both root blocks we avoid
				 * IO errors when trying to read those data and are able to
				 * reject files based on whether they have bad magic, version,
				 * or root blocks.
				 */

				throw new RuntimeException(
						"File too small to contain a valid journal: "
								+ file.getAbsoluteFile());

			}

			/*
			 * Note: The code to read the MAGIC, VERSION, and root blocks is
			 * shared by DumpJournal (code is copy by value) and in part by the
			 * rollback() method on AbstractJournal.
			 */

			/*
			 * Read the MAGIC and VERSION.
			 */
			raf.seek(0L);
			try {
				/*
				 * Note: this next line will throw IOException if there is a
				 * file lock contention.
				 */
				magic = raf.readInt();
			} catch (IOException ex) {
				throw new RuntimeException("Can not read magic. Is file locked by another process?", ex);
			}
			if (magic != MAGIC)
				throw new RuntimeException("Bad journal magic: expected=" + MAGIC + ", actual=" + magic);
			version = raf.readInt();
			if (version != VERSION1)
				throw new RuntimeException("Bad journal version: expected=" + VERSION1 + ", actual=" + version);

			/*
			 * Check root blocks (magic, timestamps), choose root block, read
			 * constants (slotSize, segmentId).
			 */
			{
				
				final ChecksumUtility checker = validateChecksum ? ChecksumUtility.threadChk
						.get()
						: null;

				// final FileChannel channel = raf.getChannel();
				final ByteBuffer tmp0 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
				final ByteBuffer tmp1 = ByteBuffer.allocate(RootBlockView.SIZEOF_ROOT_BLOCK);
				FileChannelUtility.readAll(opener, tmp0, OFFSET_ROOT_BLOCK0);
				FileChannelUtility.readAll(opener, tmp1, OFFSET_ROOT_BLOCK1);
				tmp0.position(0); // resets the position.
				tmp1.position(0);
				try {
					rootBlock0 = new RootBlockView(true, tmp0, checker);
				} catch (RootBlockException ex) {
					log.warn("Bad root block zero: " + ex);
				}
				try {
					rootBlock1 = new RootBlockView(false, tmp1, checker);
				} catch (RootBlockException ex) {
					log.warn("Bad root block one: " + ex);
				}
				if (rootBlock0 == null && rootBlock1 == null) {
					throw new RuntimeException(
							"Both root blocks are bad - journal is not usable: "
									+ file);
				}
				if (alternateRootBlock)
					log.warn("Using alternate root block");
				/*
				 * Choose the root block based on the commit counter.
				 * 
				 * Note: The commit counters MAY be equal. This will happen if
				 * we rollback the journal and override the current root block
				 * with the alternate root block.
				 */
				final long cc0 = rootBlock0.getCommitCounter();
				final long cc1 = rootBlock1.getCommitCounter();
				this.rootBlock = (cc0 > cc1 ? (alternateRootBlock ? rootBlock1
						: rootBlock0) : (alternateRootBlock ? rootBlock0
						: rootBlock1));

			}

			this.bufferMode = BufferMode.getDefaultBufferMode(rootBlock.getStoreType());

			if (bufferMode.isFullyBuffered()) {

				/*
				 * Verify that we can address this many bytes with this
				 * strategy. The strategies that rely on an in-memory buffer are
				 * all limited to the #of bytes that can be addressed by an
				 * int32.
				 */

				AbstractBufferStrategy.assertNonDiskExtent(userExtent);

			}

			// use the offset bits from the root block.
			this.offsetBits = rootBlock.getOffsetBits();

			/*
			 * The offset into the user extent at which the next record will be
			 * written.
			 */
			this.nextOffset = rootBlock.getNextOffset();

			this.createTime = rootBlock.getCreateTime();

			this.closeTime = rootBlock.getCloseTime();

			if (closeTime != 0L && !readOnly) {

				throw new RuntimeException("Journal is closed for writes: closedTime=" + closeTime);

			}

			switch (bufferMode) {
			case Direct: {
				// Allocate the buffer buffer.
				buffer = (useDirectBuffers ? ByteBuffer.allocateDirect((int) userExtent) : ByteBuffer
						.allocate((int) userExtent));
				// Setup to read data from file into the buffer.
				if (nextOffset > Integer.MAX_VALUE) {
					throw new RuntimeException("This file is too large for a buffered mode: use " + BufferMode.Disk);
				}
				buffer.limit((int) nextOffset);
				buffer.position(0);
				if (nextOffset > 0) {
					// Read the file image into the direct buffer.
					FileChannelUtility.readAll(opener, buffer, headerSize0);
				}
				break;
			}
			case Mapped: {
				// Map the file.
				boolean loadMappedFile = false; // @todo expose as property.
				buffer = opener.reopenChannel().map(FileChannel.MapMode.READ_WRITE, headerSize0, extent);
				if (loadMappedFile) {
					/*
					 * Load the image into mapped memory. Generally, I would
					 * think that you are better off NOT loading the image. When
					 * you want the image in memory, use the Direct mode
					 * instead. It should be MUCH faster and has better control
					 * over the amount and timing of the IO.
					 */
					((MappedByteBuffer) buffer).load();
				}
				break;
			}
			case Disk:
				buffer = null;
				break;
			case DiskWORM:
				buffer = null;
				break;
			case DiskRW:
				buffer = null;
				break;
			default:
				throw new AssertionError();
			}

			/*
			 * Note: there should be no processing required on restart since the
			 * intention of transactions that did not commit will not be
			 * visible.
			 */

			this.useChecksums = useChecksums(rootBlock);

		} catch (IOException ex) {

			throw new RuntimeException("file=" + file, ex);

		}

	}

	/**
	 * Used to re-open the {@link FileChannel} in this class.
	 */
	private final IReopenChannel<FileChannel> opener = new IReopenChannel<FileChannel>() {

		public String toString() {

			return file.toString();

		}

		public FileChannel reopenChannel() throws IOException {

			return FileMetadata.this.reopenChannel();

		}

	};

	/**
	 * This method transparently re-opens the channel for the backing file.
	 * 
	 * @throws IllegalStateException
	 *             if the store is closed.
	 * 
	 * @throws IOException
	 *             if the backing file can not be locked.
	 */
	final synchronized private FileChannel reopenChannel() throws IOException {

		if (raf != null && raf.getChannel().isOpen()) {

			/*
			 * The channel is still open. If you are allowing concurrent reads
			 * on the channel, then this could indicate that two readers each
			 * found the channel closed and that one was able to re-open the
			 * channel before the other such that the channel was open again by
			 * the time the 2nd reader got here.
			 */

			return raf.getChannel();

		}

		// open the file.
		this.raf = new RandomAccessFile(file, fileMode);

		if (INFO)
			log.info("(Re-)opened file: " + file);

		if (bufferMode != BufferMode.Mapped) {

			try {

				/*
				 * Request a shared file lock.
				 */

				final boolean readOnly = "r".equals(fileMode);

				if (raf.getChannel().tryLock(0, Long.MAX_VALUE, readOnly/* shared */) == null) {

					/*
					 * Note: A null return indicates that someone else holds the
					 * lock. This can happen if the platform does not support
					 * shared locks or if someone requested an exclusive file
					 * lock.
					 */

					try {
						raf.close();
					} catch (Throwable t) {
						// ignore.
					}

					throw new IOException("File already locked? file=" + file);

				}

			} catch (IOException ex) {

				/*
				 * Note: This is true of NFS volumes. This is Ok and should be
				 * ignored. However the backing file is not protected against
				 * accidental deletes or overwrites.
				 */

				if (INFO)
					log.info("FileLock not supported: file=" + file, ex);

			} catch (OverlappingFileLockException ex) {

				if (INFO)
					log.info("FileLock problem: file=" + file, ex);
				
				throw new RuntimeException("FileLock Overlap", ex);

			}
		}

		return raf.getChannel();

	}

	/**
	 * Record level checksums are enabled for the RW store since
	 * {@link RootBlockView#VERSION1} and for the WORM store since
	 * {@link RootBlockView#VERSION2}.
	 * 
	 * @param rootBlock
	 *            A root block.
	 * 
	 * @return Whether or not record level checksums are enabled.
	 */
	static private boolean useChecksums(final IRootBlockView rootBlock) {

		return rootBlock.getVersion() >= RootBlockView.VERSION2
				|| (rootBlock.getVersion() == 1 && rootBlock.getStoreType() == StoreTypeEnum.RW);

	}

	public void writeRootBlock(final IRootBlockView rootBlock,
			final ForceEnum forceOnCommit) throws IOException {

		if (rootBlock == null)
			throw new IllegalArgumentException();

		final ByteBuffer data = rootBlock.asReadOnlyBuffer();

		final long pos = rootBlock.isRootBlock0() ? OFFSET_ROOT_BLOCK0 : OFFSET_ROOT_BLOCK1;

		final FileChannel channel = raf.getChannel();

		channel.write(data, pos);

		if (forceOnCommit != ForceEnum.No) {

			channel.force(forceOnCommit == ForceEnum.ForceMetadata);

		}

		if (log.isTraceEnabled())
			log.trace("Writing ROOTBLOCK with commitCounter: " + rootBlock.getCommitCounter()
					+ ", commitRecordIndexAddr: " + rootBlock.getCommitRecordIndexAddr()
					+ ", commitRecordAddr: " + rootBlock.getCommitRecordAddr());
	}

	/**
	 * Prepare a journal file for use by an {@link IBufferStrategy}. The file
	 * will be created if necessary as permitted and instructed by the specified
	 * {@link Options}. The root blocks will be created or read from the file
	 * and verified. A variety of interesting properties are set on the returned
	 * {@link FileMetadata} object.
	 */
	static public FileMetadata createInstance(final Properties properties,
			final boolean isScaleout, final long quorumToken) {

		Banner.banner();
		
		final BufferMode bufferMode = BufferMode.valueOf(getProperty(
				properties, Options.BUFFER_MODE, Options.DEFAULT_BUFFER_MODE));
		
        boolean create = Boolean.parseBoolean(Options.DEFAULT_CREATE);
        
        boolean isEmptyFile = false;

		final boolean createTempFile = Boolean.parseBoolean(getProperty(
				properties, Options.CREATE_TEMP_FILE,
				Options.DEFAULT_CREATE_TEMP_FILE));

		final File tmpDir = new File(getProperty(properties, Options.TMP_DIR,
				System.getProperty("java.io.tmpdir")));

        if (createTempFile) {

			create = false;

			isEmptyFile = true;

			if (!tmpDir.exists()) {

				if (!tmpDir.mkdirs()) {

					throw new RuntimeException("Could not create directory: "
							+ tmpDir.getAbsolutePath());

				}

			}

			if (!tmpDir.isDirectory()) {

				throw new RuntimeException("Not a directory: "
						+ tmpDir.getAbsolutePath());

			}

		}
        
        // The backing file.
        final File file;
        {

			// The name of the backing file iff explicitly specified.
			String fname = getProperty(properties, Options.FILE, null);

			if (createTempFile) {

				if (fname != null) {

					throw new RuntimeException("Can not use option '"
							+ Options.CREATE_TEMP_FILE + "' with option '"
							+ Options.FILE + "'");

				}

				try {

					fname = File.createTempFile("bigdata-" + bufferMode + "-",
							Options.JNL, tmpDir).toString();

				} catch (IOException ex) {

					throw new RuntimeException(ex);

				}

			} else if (fname == null) {

				throw new RuntimeException("Required property: '"
						+ Options.FILE + "'");

			}

			file = new File(fname);
		
        }

		if (file.exists() && file.length() == 0) {

			/*
			 * The file exists but is empty. This is what happens if you use
			 * File.createTempFile(...) to generate the name of the file to be
			 * opened. As a special exception, the journal will be initialized
			 * on the empty file.
			 * 
			 * Note: this will also be the case if a new file is opened but
			 * never written to.
			 */

			isEmptyFile = true;

		} else {

			/*
			 * Make sure that the parent directory (if any) exists.
			 */

			final File parent = file.getParentFile();

			if (parent != null && !parent.exists()) {

				// create the parent directory.

				if (!parent.mkdirs()) {

					throw new RuntimeException(
							"Could not create parent directory: " + parent);

				}

            }
            
        }
        
		final boolean useDirectBuffers = Boolean.parseBoolean(getProperty(
				properties, Options.USE_DIRECT_BUFFERS,
				Options.DEFAULT_USE_DIRECT_BUFFERS));

		final boolean readOnly = Boolean.parseBoolean(getProperty(properties,
				Options.READ_ONLY, Options.DEFAULT_READ_ONLY));

		final ForceEnum forceWrites = ForceEnum.parse(getProperty(properties,
				Options.FORCE_WRITES, Options.DEFAULT_FORCE_WRITES));

		final boolean deleteOnExit = Boolean.parseBoolean(getProperty(
				properties, Options.DELETE_ON_EXIT,
				Options.DEFAULT_DELETE_ON_EXIT));

		final boolean writeCacheEnabled = Boolean.parseBoolean(getProperty(
				properties, Options.WRITE_CACHE_ENABLED,
				Options.DEFAULT_WRITE_CACHE_ENABLED));

		final int writeCacheBufferCount = Integer.parseInt(getProperty(
				properties, Options.WRITE_CACHE_BUFFER_COUNT,
				Options.DEFAULT_WRITE_CACHE_BUFFER_COUNT));

		final boolean validateChecksum = Boolean.parseBoolean(getProperty(
				properties, Options.VALIDATE_CHECKSUM,
				Options.DEFAULT_VALIDATE_CHECKSUM));

		final boolean alternateRootBlock = Boolean.parseBoolean(getProperty(
				properties, Options.ALTERNATE_ROOT_BLOCK, "false"));

        if (alternateRootBlock && !readOnly) {

            log.warn("*** Using the alternate root block: "
                    + "Data will be lost on the next commit! ***");

        }
        
		if (file.exists() && !isEmptyFile) {

			/*
			 * Code path when the file exists and is non-empty.
			 */
			
			return new FileMetadata(file, useDirectBuffers, readOnly,
					forceWrites, writeCacheEnabled, writeCacheBufferCount,
					validateChecksum, alternateRootBlock, properties);

		} else {

			/*
			 * Code path with the file does not exist or exists but is empty.
			 * 
			 * Note: Among other things, this code path covers the create of an
			 * AbstractJournal when the caller has supplied a filename obtained
			 * from the temporary file creation mechanisms.
			 */

			final long initialExtent = getProperty(properties,
					Options.INITIAL_EXTENT, Options.DEFAULT_INITIAL_EXTENT,
					new LongRangeValidator(Options.minimumInitialExtent,
							Long.MAX_VALUE));

			final long maximumExtent = getProperty(properties,
					Options.MAXIMUM_EXTENT, Options.DEFAULT_MAXIMUM_EXTENT,
					new LongRangeValidator(initialExtent, Long.MAX_VALUE));

			/*
			 * Note: The caller SHOULD specify an explicit [createTime] when
			 * its value is critical. The default assigned here does NOT
			 * attempt to use a clock that is consistent with the commit
			 * protocol or even a clock that assigns unique timestamps.
			 */
			final long createTime = Long.parseLong(getProperty(properties,
					Options.CREATE_TIME, "" + System.currentTimeMillis()));

			final int offsetBits = getProperty(
					properties,
					Options.OFFSET_BITS,
					Integer
							.toString((!isScaleout ? WormAddressManager.SCALE_UP_OFFSET_BITS
									: WormAddressManager.SCALE_OUT_OFFSET_BITS)),
					new IntegerRangeValidator(
							WormAddressManager.MIN_OFFSET_BITS,
							WormAddressManager.MAX_OFFSET_BITS));

			return new FileMetadata(file, bufferMode, useDirectBuffers,
					initialExtent, maximumExtent, create, isEmptyFile,
					deleteOnExit, readOnly, forceWrites, offsetBits,
					writeCacheEnabled, writeCacheBufferCount, validateChecksum,
					createTime, quorumToken, alternateRootBlock, properties);
		
		}

	}

	static protected String getProperty(final Properties properties,
			final String name, final String defaultValue) {

		return Configuration.getProperty(null/* indexManager */, properties,
				""/* no namespace */, name, defaultValue);

	}

	static protected <E> E getProperty(final Properties properties,
			final String name, final String defaultValue,
			IValidator<E> validator) {

		return Configuration.getProperty(null, properties, ""/* no namespace */,
				name, defaultValue, validator);

	}
	
	/**
	 * retrieve the property value from the properties object used to
	 * initialize this object.
	 */
	public String getProperty(final String name, final String defaultValue) {
		return getProperty(properties, name, defaultValue);
	}

}
