/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.journal;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.Properties;

import com.bigdata.objndx.IndexSegment;
import com.bigdata.rawstore.Bytes;

/**
 * Options for the {@link Journal}. Options are specified as property values to
 * the {@link Journal#Journal(Properties)} constructor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Options {

    /**
     * <code>file</code> - The name of the file. If the file not found and
     * {@link #CREATE} is true, then a new journal will be created. The filename
     * extension <code>.jnl</code> is recommended.
     */
    public static final String FILE = "file";

    /**
     * <code>bufferMode</code> - One of "Transient", "Direct", "Mapped",
     * or "Disk". See {@link BufferMode} for more information about each
     * mode.
     * 
     * @see BufferMode#Transient
     * @see BufferMode#Direct
     * @see BufferMode#Mapped
     * @see BufferMode#Disk
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
     * <code>segment</code> - The unique int32 segment identifier (required
     * unless this is a {@link BufferMode#Transient} journal). Segment
     * identifiers are assigned by a bigdata federation (scale out solution).
     * When using the journal as part of an embedded or scale up database you
     * may safely assign an arbitrary segment identifier, e.g., zero(0).
     */
    public static final String SEGMENT = "segment";
    
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
     * data on disk but may be used to tweak the file system buffering.
     * 
     * @see #FORCE_ON_COMMIT, which controls the stability of the data on disk.
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
     * be created using the
     * {@link File#createTempFile(String, String, File) temporary file mechanism}.
     * If {@link #DELETE_ON_EXIT} is also specified, then the temporary file
     * will be {@link File#deleteOnExit() marked for deletion} when the JVM
     * exits. This option is often used when preparing a journal for a unit
     * test. The default temporary directory is used unless it is overriden by
     * the {@link #TMP_DIR} option.
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
     * The default for {@link #USE_DIRECT_BUFFERS}.
     */
    public final static boolean DEFAULT_USE_DIRECT_BUFFERS = false;
    
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
