package com.bigdata.journal;

import java.nio.channels.FileChannel;
import java.util.Properties;

/**
 * Options for the {@link Journal}. Options are specified as property
 * values to the {@link Journal#Journal(Properties)} constructor.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Options {

    /**
     * <code>file</code> - The name of the file. If the file not found and
     * {@link #CREATE} is true, then a new journal will be created.
     */
    public static final String FILE = "file";

    /**
     * <code>bufferMode</code> - One of "transient", "direct", "mapped",
     * or "disk". See {@link BufferMode} for more information about each
     * mode.
     * 
     * @see BufferMode#Transient
     * @see BufferMode#Direct
     * @see BufferMode#Mapped
     * @see BufferMode#Disk
     */
    public static final String BUFFER_MODE = "bufferMode";
    
    /**
     * <code>objectIndexSize</code> - The size of a node in the object
     * index (aka branching factor). A larger node size correlates with an
     * object index with less height, and height correlates with access
     * time. Access time is a concern when the journal is not fully
     * buffered. The value must be even and is normally a power of two,
     * e.g., 64, 128, 256, etc.
     */
    public static final String OBJECT_INDEX_SIZE = "objectIndexSize";

    /**
     * <code>initialExtent</code> - The initial extent of the journal
     * (bytes). The initial file size is computed by subtracting off the
     * space required by the root blocks and dividing by the slot size.
     */
    public static final String INITIAL_EXTENT = "initialExtent";
    
    /**
     * <code>segment</code> - The unique segment identifier (required
     * unless this is a {@link BufferMode#Transient} journal). Segment
     * identifiers are assigned by a bigdata federation. When using the
     * journal as part of an embedded database you may safely assign an
     * arbitrary segment identifier.
     */
    public static final String SEGMENT = "segment";
    
    /**
     * <code>create</code> - An optional boolean property (default is
     * <code>true</code>). When true and the named file is not found, a new
     * journal will be created.
     * 
     * @todo Write tests for this feature.
     */
    public static final String CREATE = "create";
    
    /**
     * <p>
     * <code>slotSize</code> - The slot size in bytes. The data space of
     * the journal is divided into equal sized slots. All objects written on
     * the journal are written onto one or more slots. The journal is
     * relatively efficient about its use of slots. The main overhead when
     * slots are two small is the slot allocation index (a bitmap). When
     * slots are too large, there is a tendency for wasted space. Note that
     * the journal will reclaim slots as the versions written on those slots
     * become available for GC.
     * </p>
     * <p>
     * If the journal is being used to absorb writes and provide concurrency
     * control for small objects, then the slot size should be relatively
     * small, e.g., 48, 64, 96, or 128 bytes depending on the application.
     * If an object is either over or under the slot size, then there will
     * be waste. When the object is under the slot size, the waste is the
     * space remaining in the slot. When the object is over the slot size,
     * the waste is the space remaining in the last slot allocated to that
     * object. If objects tend to be larger, then you can increase the slot
     * size.
     * </p>
     */
    public static final String SLOT_SIZE = "slotSize";
    
    /**
     * <code>readOnly</code> - When true, the journal must pre-exist and
     * will be read-only (optional, default is <code>false</code>).
     * 
     * @todo Write tests for this feature.
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
     * <dt>no</dt>
     * <dd>This option is useful when the journal is replicated so that we can
     * always failover to another server having the same data. Unless the file
     * is replicated or transient, this mode can lead to lost data if there is a
     * hardware or software failure.</dd>
     * <dt>force</dt>
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
     * <dt>forceMetadata</dt>
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
     * @see IBufferStrategy#force(boolean)
     * @see FileChannel#force(boolean)
     * @see ForceEnum
     */
    public static final String FORCE_ON_COMMIT = "forceOnCommit";
    
    /**
     * <code>conflictResolver</code> - The name of a class that implements
     * the {@link IConflictResolver} interface (optional). When specified,
     * the class MUST define a public constructor with the signature
     * <code><i>class</i>( Journal journal )</code>. There is NO
     * default. Resolution of write-write conflicts is enabled iff a
     * conflict resolution class is declared with this parameter. If a value
     * is not provided, the a write-write conflict will result in the
     * rollback of a transaction.
     */
    public static final String CONFLICT_RESOLVER = "conflictResolver";
    
    /**
     * <code>deleteOnClose</code> - This optional boolean option causes
     * the journal file to be deleted when the journal is closed (default
     * <em>false</em>). This option is used by the test suites to keep
     * down the disk burden of the tests and MUST NOT be used with live
     * data.
     */
    public final static String DELETE_ON_CLOSE = "deleteOnClose";
    
    /**
     * The default initial extent for a new journal.
     */
    public final static long DEFAULT_INITIAL_EXTENT = 10 * Bytes.megabyte;
    
    /**
     * The minimum node size (aka branching factor) for the object index.
     */
    public final static int MIN_OBJECT_INDEX_SIZE = 16;
    
    /**
     * The maximum node size (aka branching factor) for the object index.
     */
    public final static int MAX_OBJECT_INDEX_SIZE = 2048;
    
    /**
     * The default node size (aka branching factor) for the object index.
     */
    public final static int DEFAULT_OBJECT_INDEX_SIZE = 256;
    
    /**
     * The minimum permitted slot size.
     */
    public final static int MIN_SLOT_SIZE = 32;
    
    /**
     * The default slot size.
     */
    public final static int DEFAULT_SLOT_SIZE = 64;

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
    public final static ForceEnum DEFAULT_FORCE_ON_COMMIT = ForceEnum.ForceMetadata;
    
    /**
     * The default for the {@link #DELETE_ON_CLOSE} option.
     */
    public final static boolean DEFAULT_DELETE_ON_CLOSE = false;

}
