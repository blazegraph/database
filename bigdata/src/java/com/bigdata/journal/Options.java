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
import java.io.FileDescriptor;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Properties;

import com.bigdata.btree.BTree;
import com.bigdata.btree.Checkpoint;
import com.bigdata.btree.IndexSegment;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;
import com.bigdata.resources.ResourceManager;

/**
 * Options for the {@link Journal}. Options are specified as property values to
 * the {@link Journal#Journal(Properties)} constructor.
 * 
 * <h2>Write Cache</h2>
 * <p>
 * Some operating systems, file systems, disk controllers, and disk drives may
 * re-order writes, processing them in a different order from the sequence in
 * which they are issued by the application, and may return from
 * {@link FileChannel#force(boolean)} or {@link FileDescriptor#sync()} before
 * the data are on stable media. This can have the consequence that the root
 * blocks are laid down on the disk before the application data. In this
 * situation a hard failure during (or after!) the write could result in the
 * loss of application data since the root blocks representing the atomic commit
 * point MIGHT be updated before the application data was successfully made
 * stable on disk. Further, {@link IAtomicStore#commit()} MAY return before the
 * data are stable on disk.
 * </p>
 * <p>
 * Depending on your needs there are several steps which you can take to
 * increase data security, including backups, media replication, etc. In terms
 * of the {@link Journal} itself, you can use {@link #FORCE_WRITES} to request
 * synchronous writes or disable the write cache on your platform. While
 * {@link #FORCE_WRITES} is trivially specified as a configuration option,
 * please note that the behavior of {@link #FORCE_WRITES} can vary from platform
 * to platform and that disabling the write cache on your platform may give you
 * a greater assurance that writes are neither being reordered nor returning
 * before the data are on stable storage. Also note that some high end disk
 * systems have battery backup for the on-disk write cache, in which case you
 * may choose to leave the write cache on the disk enabled.
 * </p>
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
    String FILE = "file";

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
    String BUFFER_MODE = "bufferMode";

    /**
     * The capacity of the LRU cache for canonicalizing mapping maintained by
     * the {@link AbstractJournal} for index {@link Checkpoint}s. The capacity
     * of this cache indirectly controls how many <strong>historical</strong>
     * indices the journal will hold open. The main reason for keeping a
     * historical index open is to reuse its buffers, including its node and
     * leaf cache, if another request arrives "soon" for that historical commit
     * point of the index.
     * <p>
     * The effect of this parameter is indirect owning to the semantics of weak
     * references and the control of the JVM over when they are cleared. Once an
     * index becomes weakly reachable, the JVM will eventually GC the index
     * object, thereby effectively closing it (or at least releasing all
     * resources associated with that index). Since indices which are strongly
     * reachable are never "closed" this provides our guarentee that indices are
     * never closed if they are in use.
     * 
     * @see #DEFAULT_HISTORICAL_INDEX_CACHE_CAPACITY
     * 
     * @see #LIVE_INDEX_CACHE_CAPACITY, which governs the retention of clean
     *      "live" indices.
     * 
     * @see AbstractJournal#getIndex(long)
     */
    String HISTORICAL_INDEX_CACHE_CAPACITY = "historicalIndexCacheCapacity";
    
    /**
     * The capacity of the LRU cache for the "live" {@link Name2Addr} object
     * (default 20). A special {@link Name2Addr} instance is maintained for the
     * "live" index objects, gives the most recent {@link Checkpoint} address
     * from which the "live" version of the index may be loaded, and provides a
     * canonicalizing cache for the "live" index objects by name.
     * <p>
     * The capacity of this cache for this special {@link Name2Addr} instance
     * indirectly controls how many <strong>clean</strong> "live" indices the
     * journal will hold open. A "live" index is only clean when it has just
     * been loaded from the store or when it has just been committed, so this
     * really effects how long an index which has been committed will be keep
     * open. The main reason for keeping the index open after a commit is to
     * reuse its buffers, including its node and leaf cache, if another request
     * arrives "soon" for an operation on that index.
     * <p>
     * The effect of this parameter is indirect owning to the semantics of weak
     * references and the control of the JVM over when they are cleared. Once an
     * index becomes weakly reachable, the JVM will eventually GC the index
     * object, thereby effectively closing it (or at least releasing all
     * resources associated with that index). Dirty indices will be held open
     * until the next commit/abort regardless since a hard reference is placed
     * on the commit list.
     * 
     * @see #DEFAULT_LIVE_INDEX_CACHE_CAPACITY
     * 
     * @see #HISTORICAL_INDEX_CACHE_CAPACITY
     * 
     * @see Name2Addr
     */
    String LIVE_INDEX_CACHE_CAPACITY = "liveIndexCacheCapacity";
    
    /**
     * <code>useDirectBuffers</code> - A boolean property whose value controls
     * whether a direct (native) or heap-based {@link ByteBuffer} will be
     * allocated by the selected {@link BufferMode}. Note that this only
     * applies to fully buffered modes, e.g., {@link BufferMode#Transient} or
     * {@link BufferMode#Direct}. This parameter has no effect for the
     * memory-mapped or disk-only buffer modes.
     */
    String USE_DIRECT_BUFFERS = "useDirectBuffers";
    
    /**
     * <code>writeCacheCapacity</code> - An integer property whose value
     * controls the size of the write cache (in bytes) used by the selected
     * {@link BufferMode} (default <code>10485760</code> bytes). A value of
     * ZERO (0) will disable the write cache.
     * <p>
     * Note: This value is ignored by some {@link BufferMode}s.
     * 
     * @see #DEFAULT_WRITE_CACHE_CAPACITY
     */
    String WRITE_CACHE_CAPACITY = "writeCacheCapacity";
    
    /**
     * <code>initialExtent</code> - The initial extent of the journal (bytes).
     * When the journal is backed by a file, this is the initial length of that
     * file. The initial user extent is typically slightly smaller as the head
     * of the file contains some metadata outside of the user space (the root
     * blocks). The initial extent will be transparently extended as necessary
     * when the user space becomes full.
     * <p>
     * Note: When using a partitioned index strategy the initial extent and the
     * maximum extent should be the same so that the cost of extending the
     * journal may be avoided as much as possible.
     * 
     * @see #DEFAULT_INITIAL_EXTENT
     */
    String INITIAL_EXTENT = "initialExtent";
    
    long minimumInitialExtent = Bytes.megabyte;
    
    int minimumWriteCacheCapacity = Bytes.megabyte32;
    
    /**
     * <code>maximumExtent</code> - The maximum extent of the journal (bytes).
     * The journal will {@link IResourceManager#overflow()} once it approaches
     * this limit.
     * 
     * @see #DEFAULT_MAXIMUM_EXTENT
     */
    String MAXIMUM_EXTENT = "maximumExtent";

    /**
     * <code>offsetBits</code> - The #of bits in a 64-bit long integer
     * identifier that are used to encode the byte offset of a record in the
     * store as an unsigned integer (default is {@value #DEFAULT_OFFSET_BITS}).
     * <p>
     * The default supports scale-out deployments (distributed federation). If
     * you are using a scale-up deployment (single data service or journal) then
     * you SHOULD explicitly specify a different value in order to allow larger
     * files.
     * 
     * @see #DEFAULT_OFFSET_BITS
     * @see WormAddressManager#SCALE_UP_OFFSET_BITS
     * @see WormAddressManager#SCALE_OUT_OFFSET_BITS
     */
    String OFFSET_BITS = "offsetBits";
    
    /**
     * <code>validateChecksum</code> - An optional boolean property (default
     * is <code>true</code>). When true, the checksum stored in the root
     * blocks of an existing store will be validated when the store file is
     * opened. This property may be set to false if you are engaged in a
     * desparate attempt to use a bad root block or to patch a bad root block
     * using a hex editor.
     * 
     * @see #DEFAULT_VALIDATE_CHECKSUM
     */
    String VALIDATE_CHECKSUM = "validateChecksum";
    
    /**
     * <code>create</code> - An optional boolean property (default is
     * <code>true</code>). When true and the named file is not found, a new
     * journal will be created.
     */
    String CREATE = "create";

    /**
     * <code>createTime</code> - An optional property allowed iff a new
     * journal is being created whose value is the timestamp to be written in
     * the {@link IRootBlockView#getCreateTime()} field of the root block. This
     * is used by the {@link ResourceManager} to place consistent createTime
     * timestamps on its managed journal resources.
     */
    String CREATE_TIME = "createTime";
    
    /**
     * <code>readOnly</code> - When true, the journal must pre-exist and
     * will be read-only (optional, default is <code>false</code>).
     */
    String READ_ONLY = "readOnly";
    
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
    String FORCE_WRITES = "forceWrites";

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
    String FORCE_ON_COMMIT = "forceOnCommit";

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
     * 
     * @deprecated This option does NOT provide a sufficient guarentee when a
     *             write cache is in use by the operating system or the disk if
     *             the layered write caches return before all data is safely on
     *             disk (or in a battery powered cache). In order to protect
     *             against this you MUST disable the write cache layers in the
     *             operating system and the disk drive such that
     *             {@link FileChannel#force(boolean)} will not return until the
     *             data are in fact on stable storage. If you disable the OS and
     *             disk write cache then you do NOT need to specify this option
     *             since writes will be ordered and all data will be on disk
     *             before we update the commit blocks.
     */
    String DOUBLE_SYNC = "doubleSync";

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
    String CREATE_TEMP_FILE = "createTempFile";
    
    /**
     * <code>deleteOnClose</code> - This boolean option causes the journal
     * file to be deleted when the journal is closed (default <em>false</em>).
     * This option is used by the some test suites (those that do not test
     * restart safety) to keep down the disk burden of the tests and MUST NOT be
     * used with restart-safe data.
     */
    String DELETE_ON_CLOSE = "deleteOnClose";

    /**
     * <code>deleteOnExit</code> - This boolean option causes the journal file
     * to be deleted when the VM exits (default <em>false</em>). This option
     * is used by the test suites to keep down the disk burden of the tests
     * and MUST NOT be used with restart-safe data.
     */
    String DELETE_ON_EXIT = "deleteOnExit";
    
    /**
     * <code>tmp.dir</code> - The property whose value is the name of the
     * directory in which temporary files will be created. When not specified
     * the default is governed by the value of the System property named
     * <code>java.io.tmpdir</code>. There are several kinds of temporary
     * files that can be created, including temporary journals, intermediate
     * files from an index merge process, etc.
     */
    String TMP_DIR = "tmp.dir";

    /**
     * <code>branchingFactor</code> - The branching factor for indices
     * created with {@link IIndexManager#registerIndex(String)}.
     * 
     * @see #DEFAULT_BRANCHING_FACTOR
     */
    String BRANCHING_FACTOR = "branchingFactor";
    
    /**
     * The default for the {@link #BUFFER_MODE}.
     */
    BufferMode DEFAULT_BUFFER_MODE = BufferMode.Disk;
    
    /**
     * The default for {@link #USE_DIRECT_BUFFERS}.
     */
    boolean DEFAULT_USE_DIRECT_BUFFERS = false;

    /**
     * The default for {@link #WRITE_CACHE_CAPACITY} (10M).
     */
    int DEFAULT_WRITE_CACHE_CAPACITY = 10 * Bytes.megabyte32;
    
    /**
     * The default initial extent for a new journal.
     * 
     * @see #INITIAL_EXTENT
     */
    long DEFAULT_INITIAL_EXTENT = 10 * Bytes.megabyte;
    
    /**
     * The default maximum extent for a new journal before a commit triggers an
     * overflow event (500M).
     * 
     * @see #MAXIMUM_EXTENT
     */
    long DEFAULT_MAXIMUM_EXTENT = 500 * Bytes.megabyte;
    
    /**
     * The default #of bits ({@value WormAddressManager#SCALE_OUT_OFFSET_BITS})
     * used to encode the byte offset of a record in the store as an unsigned
     * integer.
     * 
     * @see #OFFSET_BITS
     */
    String DEFAULT_OFFSET_BITS = ""+WormAddressManager.SCALE_OUT_OFFSET_BITS;

    boolean DEFAULT_VALIDATE_CHECKSUM = true;
    
    /**
     * The default for the {@link #CREATE} option.
     */
    boolean DEFAULT_CREATE = true;

    /**
     * The default for the {@link #READ_ONLY} option.
     */
    boolean DEFAULT_READ_ONLY = false;
    
    /**
     * The default for the {@link #FORCE_WRITES} option (writes are not forced).
     * 
     * @todo consider changing this default so that we normally request
     *       synchronous writes.
     */
    ForceEnum DEFAULT_FORCE_WRITES = ForceEnum.No;
    
    /**
     * The default for the {@link #FORCE_ON_COMMIT} option (file data and
     * metadata are forced).
     */
    ForceEnum DEFAULT_FORCE_ON_COMMIT = ForceEnum.ForceMetadata;
    
    /**
     * The default for the {@link #DOUBLE_SYNC} option (application data is NOT
     * forced to disk before we write the root blocks).
     */
    boolean DEFAULT_DOUBLE_SYNC = false;
    
    /**
     * The default for the {@link #DELETE_ON_CLOSE} option.
     */
    boolean DEFAULT_DELETE_ON_CLOSE = false;

    /**
     * The default for the {@link #DELETE_ON_EXIT} option.
     */
    boolean DEFAULT_DELETE_ON_EXIT = false;

    /**
     * The default for the {@link #CREATE_TEMP_FILE} option.
     */
    boolean DEFAULT_CREATE_TEMP_FILE = false;

    /**
     * The default for the {@link #BRANCHING_FACTOR} option.
     */
    String DEFAULT_BRANCHING_FACTOR = ""+BTree.DEFAULT_BRANCHING_FACTOR;

    /**
     * The default for the {@link #HISTORICAL_INDEX_CACHE_CAPACITY} option.
     */
    String DEFAULT_HISTORICAL_INDEX_CACHE_CAPACITY = "20";

    /**
     * The default for the {@link #LIVE_INDEX_CACHE_CAPACITY} option.
     */
    String DEFAULT_LIVE_INDEX_CACHE_CAPACITY = "20";
    
    /**
     * The recommended extension for journal files.
     */
    String JNL = ".jnl";
    
    /**
     * The recommended extension for {@link IndexSegment} files.
     */
    String SEG = ".seg";
    
}
