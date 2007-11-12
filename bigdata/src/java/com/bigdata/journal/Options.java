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
import java.nio.channels.FileChannel;
import java.util.Properties;

import com.bigdata.btree.IndexSegment;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Options for the {@link Journal}. Options are specified as property values to
 * the {@link Journal#Journal(Properties)} constructor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface Options {

    /**
     * <code>file</code> - The name of the file. If the file not found and
     * {@link #CREATE} is true, then a new journal will be created. The filename
     * extension <code>.jnl</code> is recommended.
     */
    public static final String FILE = "file";

    /**
     * <code>bufferMode</code> - One of "Transient", "Direct", "Mapped", or
     * "Disk" (Disk). See {@link BufferMode} for more information about each
     * mode.
     * 
     * @see BufferMode#Transient
     * @see BufferMode#Direct
     * @see BufferMode#Mapped
     * @see BufferMode#Disk
     * 
     * @see #DEFAULT_BUFFER_MODE
     */
    public static final String BUFFER_MODE = "bufferMode";

    /**
     * <code>useDirectBuffers</code> - A boolean property whose value controls
     * whether a direct (native) or heap-based {@link ByteBuffer} will be
     * allocated by the selected {@link BufferMode}. Note that this only
     * applies to fully buffered modes, e.g., {@link BufferMode#Transient} or
     * {@link BufferMode#Direct}. This parameter has no effect for the
     * memory-mapped or disk-only buffer modes.
     */
    public static final String USE_DIRECT_BUFFERS = "useDirectBuffers";
    
    /**
     * <code>writeCacheCapacity</code> - An integer property whose value
     * controls the size of the write cache (in bytes) used by the selected
     * {@link BufferMode} (default <code>10</code>). A value of ZERO (0) will
     * disable the write cache.
     * <p>
     * Note: This value is ignored by some {@link BufferMode}s.
     * 
     * @see #DEFAULT_WRITE_CACHE_CAPACITY
     */
    public static final String WRITE_CACHE_CAPACITY = "writeCacheCapacity";
    
    /**
     * <code>initialExtent</code> - The initial extent of the journal (bytes).
     * When the journal is backed by a file, this is the initial length of that
     * file. The initial user extent is typically slightly smaller as the head
     * of the file contains some metadata outside of the user space (the root
     * blocks). The initial extent will be transparently extended as necessary
     * when the user space becomes full. When using a partitioned index strategy
     * the initial extent and the maximum extent should be the same so that the
     * cost of extending the journal buffer may be avoided as much as possible.
     * 
     * @see #DEFAULT_INITIAL_EXTENT
     */
    public static final String INITIAL_EXTENT = "initialExtent";
    
    /**
     * <code>maximumExtent</code> - The maximum extent of the journal (bytes).
     * The journal will {@link Journal#overflow()} once it approaches this limit
     * during a {@link Journal#commit()}. The default implementation ignores
     * overflow events. A scale up or scale out implementation uses this event
     * as a trigger to evict data from btrees that absorb writes on the journal
     * into partitioned {@link IndexSegment}s.
     * 
     * @see #DEFAULT_MAXIMUM_EXTENT
     */
    public static final String MAXIMUM_EXTENT = "maximumExtent";

    /**
     * <code>offsetBits</code> - The #of bits in a 64-bit long integer
     * identifier that are used to encode the byte offset of a record in the
     * store as an unsigned integer. The default is
     * {@link WormAddressManager#DEFAULT_OFFSET_BITS}, which allows store files
     * up to ~4T in length and records up to (but not including) 4M in length.
     * 
     * @see WormAddressManager
     * @see WormAddressManager#DEFAULT_OFFSET_BITS
     */
    public static final String OFFSET_BITS = "offsetBits";
    
    /**
     * <code>validateChecksum</code> - An optional boolean property (default
     * is <code>true</code>). When true, the checksum stored in the root
     * blocks of an existing store will be validated when the store file is
     * opened. This property may be set to false if you are engaged in a
     * desparate attempt to use a bad root block or to patch a bad root block
     * using a hex editor.
     */
    public static final String VALIDATE_CHECKSUM = "validateChecksum";
    
    /**
     * <code>create</code> - An optional boolean property (default is
     * <code>true</code>). When true and the named file is not found, a new
     * journal will be created.
     */
    public static final String CREATE = "create";
        
    /**
     * <code>readOnly</code> - When true, the journal must pre-exist and
     * will be read-only (optional, default is <code>false</code>).
     */
    public static final String READ_ONLY = "readOnly";
    
    /**
     * <code>forceWrites</code> - A trinary property {no, force,
     * forceMetadata} that determines whether the journal will be opened in a
     * mode that writes through synchronously to stable storage (default
     * <code>No</code>). This option does NOT effect the stability of the
     * data on disk but may be used to tweak the file system buffering (forcing
     * writes is generally MUCH slower and is turned off by default).
     * 
     * @see #FORCE_ON_COMMIT, which controls the stability of the data on disk.
     * @see #DEFAULT_FORCE_WRITES
     * @see ForceEnum
     */
    public static final String FORCE_WRITES = "forceWrites";

    /**
     * <code>forceOnCommit</code> - A trinary property {no, force,
     * forceMetadata} that determines whether the journal will be forced to
     * stable storage on a commit (default <code>forceMetadata</code>).
     * <dl>
     * <dt>No</dt>
     * <dd>This option is useful when the journal is replicated so that we can
     * always failover to another server having the same data. Unless the file
     * is replicated or transient, this mode can lead to lost data if there is a
     * hardware or software failure.</dd>
     * <dt>Force</dt>
     * <dd>Force the journal contents, but not the file metadata, to stable
     * storage. The precise semantics of this option are dependent on the OS and
     * hardware platform (some hardware platforms may guarentee that the file
     * metadata is stable using a battery powered disk cache). If there is a
     * crash, the information lost can range from the last modified timestamp on
     * the file to the file length (which can make it impossible to re-open the
     * journal until the file length is corrected with a file system repair
     * utility), to the file allocation structure in the file system (which is
     * always a serious loss, but is normally repairable with a file system
     * repair utility). </dd>
     * <dt>ForceMetadata</dt>
     * <dd>Force the journal contents and the file metadata to stable storage.
     * This option is the most secure. </dd>
     * </dl>
     * Based on some reading online, it appears that not forcing metadata can
     * mean anything from having the lastModified timestamp on the file be
     * incorrect to having the wrong file length in the directory structure, to
     * having the file allocation nodes not be coherent. This appears to be OS
     * and hardware specific (some hardware buffers the power for the disk
     * system so that such writes always complete).
     * 
     * @see #DOUBLE_SYNC
     * @see IBufferStrategy#force(boolean)
     * @see FileChannel#force(boolean)
     * @see ForceEnum
     */
    public static final String FORCE_ON_COMMIT = "forceOnCommit";

    /**
     * <code>doubleSync</code> - This boolean option causes application data
     * to be forced to stable storage <em>before</em> we update the root
     * blocks. This option seeks to guarentee that the application data is
     * stable on the disk before the atomic commit. Some operating systems
     * and/or file systems may otherwise choose an ordered write or otherwise
     * process the writes in a different order. This could have the consequence
     * that the root blocks are laid down on the disk before the application
     * data. In this situation a hard failure during the write could result in
     * the loss of application data since the updated root blocks represent the
     * atomic commit point but not all application data was successfully made
     * stable on disk.
     */
    public static final String DOUBLE_SYNC = "doubleSync";

    /**
     * <code>createTempFile</code> - This boolean option causes a new file to
     * be created using the {@link File#createTempFile(String, String, File)}
     * temporary file mechanism.
     * <p>
     * Note: If {@link #DELETE_ON_EXIT} is also specified, then the temporary
     * file will be {@link File#deleteOnExit() marked for deletion} when the JVM
     * exits.
     * <p>
     * Note: This option is often used when preparing a journal for a unit test.
     * <p>
     * Note: The default temporary directory is used unless it is overriden by
     * the {@link #TMP_DIR} option.
     * <p>
     * Note: In order to re-open a journal that was created with this option you
     * MUST (a) set this option to <code>false</code> and (b) set
     * {@link #FILE} to the value returned by {@link AbstractJournal#getFile()}.
     */
    public final static String CREATE_TEMP_FILE = "createTempFile";
    
    /**
     * <code>deleteOnClose</code> - This boolean option causes the journal
     * file to be deleted when the journal is closed (default <em>false</em>).
     * This option is used by the some test suites (those that do not test
     * restart safety) to keep down the disk burden of the tests and MUST NOT be
     * used with restart-safe data.
     */
    public final static String DELETE_ON_CLOSE = "deleteOnClose";

    /**
     * <code>deleteOnExit</code> - This boolean option causes the journal file
     * to be deleted when the VM exits (default <em>false</em>). This option
     * is used by the test suites to keep down the disk burden of the tests
     * and MUST NOT be used with restart-safe data.
     */
    public final static String DELETE_ON_EXIT = "deleteOnExit";
    
    /**
     * <code>tmp.dir</code> - The property whose value is the name of the
     * directory in which temporary files will be created. When not specified
     * the default is governed by the value of the System property named
     * <code>java.io.tmpdir</code>. There are several kinds of temporary
     * files that can be created, including temporary journals, intermediate
     * files from an index merge process, etc.
     */
    public static final String TMP_DIR = "tmp.dir";

    /**
     * The default for the {@link #BUFFER_MODE}.
     */
    public final static BufferMode DEFAULT_BUFFER_MODE = BufferMode.Disk;
    
    /**
     * The default for {@link #USE_DIRECT_BUFFERS}.
     */
    public final static boolean DEFAULT_USE_DIRECT_BUFFERS = false;

    /**
     * The default for {@link #WRITE_CACHE_CAPACITY} (10M).
     */
    public final static int DEFAULT_WRITE_CACHE_CAPACITY = 10 * Bytes.megabyte32;
    
    /**
     * The default initial extent for a new journal.
     */
    public final static long DEFAULT_INITIAL_EXTENT = 10 * Bytes.megabyte;
    
    /**
     * The default maximum extent for a new journal before a commit triggers an
     * overflow event.
     */
    public final static long DEFAULT_MAXIMUM_EXTENT = 200 * Bytes.megabyte;
    
    /**
     * The default #of bits used to encode the byte offset of a record in the
     * store as an unsigned integer.
     * 
     * @see WormAddressManager#DEFAULT_OFFSET_BITS
     */
    public final static int DEFAULT_OFFSET_BITS = WormAddressManager.DEFAULT_OFFSET_BITS;
    
    /**
     * The default for the {@link #CREATE} option.
     */
    public final static boolean DEFAULT_CREATE = true;

    /**
     * The default for the {@link #READ_ONLY} option.
     */
    public final static boolean DEFAULT_READ_ONLY = false;
    
    /**
     * The default for the {@link #FORCE_WRITES} option (writes are not forced).
     */
    public final static ForceEnum DEFAULT_FORCE_WRITES = ForceEnum.No;
    
    /**
     * The default for the {@link #FORCE_ON_COMMIT} option (file data and
     * metadata are forced).
     */
    public final static ForceEnum DEFAULT_FORCE_ON_COMMIT = ForceEnum.Force;
    
    /**
     * The default for the {@link #DOUBLE_SYNC} option (application data is NOT
     * forced to disk before we write the root blocks).
     */
    public final static boolean DEFAULT_DOUBLE_SYNC = false;
    
    /**
     * The default for the {@link #DELETE_ON_CLOSE} option.
     */
    public final static boolean DEFAULT_DELETE_ON_CLOSE = false;

    /**
     * The default for the {@link #DELETE_ON_EXIT} option.
     */
    public final static boolean DEFAULT_DELETE_ON_EXIT = false;

    /**
     * The default for the {@link #CREATE_TEMP_FILE} option.
     */
    public final static boolean DEFAULT_CREATE_TEMP_FILE = false;

    /**
     * The recommened extension for journal files.
     */
    public static final String JNL = ".jnl";
    
}
