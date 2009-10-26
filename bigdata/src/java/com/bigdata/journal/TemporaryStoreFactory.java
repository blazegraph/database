/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

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
/*
 * Created on Sep 3, 2008
 */

package com.bigdata.journal;

import java.io.File;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.cache.ConcurrentWeakValueCache;
import com.bigdata.rawstore.Bytes;
import com.bigdata.rawstore.WormAddressManager;

/**
 * Helper class for {@link IIndexStore#getTempStore()}. This class is very light
 * weight.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class TemporaryStoreFactory {

    protected static final transient Logger log = Logger
            .getLogger(TemporaryStoreFactory.class);
    
    /**
     * Configuration options for the {@link TemporaryStoreFactory}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {

        /**
         * The directory within which the {@link TemporaryStore}s will be
         * created.  The default is whichever directory is specified by
         * the Java system property <code>java.io.tmpdir</code>.
         */
        String TMP_DIR = TemporaryStoreFactory.class.getName() + ".tmpDir";

        /**
         * The Java system property whose value is the default directory for the
         * {@link TemporaryStore}s created by this factory (
         * <code>java.io.tmpdir</code>).
         */
        String JAVA_TMP_DIR_PROPERTY = "java.io.tmpdir";

        /**
         * The #of bits in a 64-bit long integer identifier that are used to encode
         * the byte offset of a record in the store as an unsigned integer. The
         * default is {@link WormAddressManager#SCALE_UP_OFFSET_BITS}.
         * 
         * @see WormAddressManager#SCALE_UP_OFFSET_BITS
         * @see WormAddressManager#SCALE_OUT_OFFSET_BITS
         */
        String OFFSET_BITS = TemporaryStoreFactory.class.getName()+".offsetBits";

        String DEFAULT_OFFSET_BITS = ""+WormAddressManager.SCALE_UP_OFFSET_BITS;
        
        /**
         * The maximum extent of the existing {@link TemporaryStore} before
         * {@link Journal#getTempStore()} will return a new
         * {@link TemporaryStore} instance (default
         * {@value #DEFAULT_TEMPORARY_STORE_MAX_EXTENT}).
         * <p>
         * {@link TemporaryStore}s are reused in order to keep down the #of file
         * handles and the latency to create a new {@link TemporaryStore} for
         * operations which make heavy use of temporary data. Old
         * {@link TemporaryStore} instances will be reclaimed (and deleted on
         * the disk) once they are no longer strongly referenced.
         * <p>
         * This option DOES NOT place an absolute limit on the maximum extent of
         * a {@link TemporaryStore} instance. A {@link TemporaryStore} will
         * continue to grow in side as long as a process continues to write on
         * the {@link TemporaryStore}.
         */
        String MAX_EXTENT = TemporaryStoreFactory.class.getName()
                + ".maxExtent";

        /**
         * The default maximum extent ({@value #DEFAULT_MAX_EXTENT}). A new
         * {@link TemporaryStore} will be created by {@link #getTempStore()}
         * when the extent of the current {@link TemporaryStore} reaches this
         * value. However, the temporary store will continue to grow as long as
         * there are execution contexts which retain a reference to that
         * instance.
         * <p>
         * Note: Each file system has its own limits on the maximum size of a
         * file. FAT16 limits the maximum file size to only 2G. FAT32 supports
         * 4G files. NTFS and most un*x file systems support 16G+ files. A safe
         * point for allocating a new temporary store for new requests is
         * therefore LT the smallest maximum file size supported by any of the
         * common file systems.
         * <p>
         * A temporary store that reaches the maximum size allowed for the file
         * system will fail when a request is made to extend that file. How that
         * effects processing depends of course on the purpose to which the
         * temporary store was being applied. E.g., to buffer a transaction, to
         * perform truth maintenance, etc.
         * 
         * @todo If we had more visibility into the file system for a given
         *       logical disk then we could apply that information to
         *       dynamically set the cutover point for the temporary store based
         *       on the backing file system. Unfortunately, {@link File} does
         *       not provide us with that information.
         */
        String DEFAULT_MAX_EXTENT = "" + (1 * Bytes.gigabyte);

    }

    /**
     * The current {@link TemporaryStore}. This is initially <code>null</code>.
     * If there are no strong references to the current {@link TemporaryStore}
     * then this reference can be cleared by GC and the store will be
     * automatically closed and its backing file deleted on the disk.
     */
    private WeakReference<TemporaryStore> ref = null;

    /** The directory within which the temporary files will be created. */
    private final File tmpDir;

    /**
     * The offset bits for the {@link TemporaryStore} instances.
     * 
     * @see WormAddressManager
     */
    private final int offsetBits;

    /**
     * The maximum extent of the current {@link TemporaryStore} before a new
     * instance will be returned by {@link #getTempStore()}.
     */
    private final long maxExtent;

    /**
     * Constructor uses the Java system properties to configure the factory.
     * The {@link Options} may be used to override the defaults if specified
     * in the environment or on the JVM command line.
     */
    public TemporaryStoreFactory() {

        this(
        //
                new File(System.getProperty(Options.TMP_DIR, System
                        .getProperty(Options.JAVA_TMP_DIR_PROPERTY))),
                //
                Integer.valueOf(System.getProperty(Options.OFFSET_BITS,
                        Options.DEFAULT_OFFSET_BITS)),
                //
                Long.valueOf(System.getProperty(Options.MAX_EXTENT,
                        Options.DEFAULT_MAX_EXTENT))
        //
        );

    }

    /**
     * Constructor uses the caller's properties object to configure the factory.
     * 
     * @param properties
     *            Properties used to configure the factory.
     */
    public TemporaryStoreFactory(final Properties properties) {

        this(
        //
                new File(properties.getProperty(Options.TMP_DIR, System
                        .getProperty(Options.JAVA_TMP_DIR_PROPERTY))),
                //
                Integer.valueOf(properties.getProperty(Options.OFFSET_BITS,
                        Options.DEFAULT_OFFSET_BITS)),
                //
                Long.valueOf(properties.getProperty(Options.MAX_EXTENT,
                        Options.DEFAULT_MAX_EXTENT))
        //
        );

    }
    
    /**
     * Constructor uses the caller's values to configure the factory.
     * 
     * @param tmpDir
     *            The directory within which the {@link TemporaryStore} files
     *            will be created.
     * @param offsetBits
     *            This value governs how many records can exist within the
     *            {@link TemporaryStore} and the maximum size of those records.
     *            A good default value is
     *            {@link WormAddressManager#SCALE_UP_OFFSET_BITS}.
     * @param maxExtent
     *            The maximum extent of the current {@link TemporaryStore}
     *            before {@link #getTempStore()} will return a new
     *            {@link TemporaryStore}.
     * 
     * @throws IllegalArgumentException
     *             if <i>maxExtent</i> is negative (zero is allowed and will
     *             cause each request to return a distinct
     *             {@link TemporaryStore}).
     */
    public TemporaryStoreFactory(final File tmpDir, final int offsetBits,
            final long maxExtent) {

        if (tmpDir == null)
            throw new IllegalArgumentException();
        
        WormAddressManager.assertOffsetBits(offsetBits);
        
        if (maxExtent < 0L)
            throw new IllegalArgumentException();
        
        this.tmpDir = tmpDir;
        
        this.offsetBits = offsetBits;

        this.maxExtent = maxExtent;

        if (log.isInfoEnabled()) {

            log.info(Options.TMP_DIR + "=" + tmpDir);
            
            log.info(Options.OFFSET_BITS + "=" + offsetBits);
            
            log.info(Options.MAX_EXTENT + "=" + maxExtent);
            
        }
        
    }

    /**
     * Return a {@link TemporaryStore}. If there is no existing
     * {@link TemporaryStore} then a new instance is returned. If there is an
     * existing {@link TemporaryStore} and its extent is greater then the
     * configured maximum extent then a new {@link TemporaryStore} will be
     * created and returned. Otherwise the existing instance is returned.
     */
    synchronized public TemporaryStore getTempStore() {

        TemporaryStore t = ref == null ? null : ref.get();

        if (t == null || t.getBufferStrategy().getExtent() > maxExtent) {

            // create an empty backing file in the specified directory.
            final File file = TemporaryRawStore.getTempFile(tmpDir);
            
            // Create a temporary store using that backing file.
            t = new TemporaryStore(offsetBits, file);

            // put into the weak value cache.
            stores.put(t.getUUID(), t);
            
            // return weak reference.
            ref = new WeakReference<TemporaryStore>(t);

        }

        return t;
    }

    /**
     * Weak value cache of the open temporary stores. Temporary stores are
     * automatically cleared from this cache after they have become only weakly
     * reachable. Temporary stores are closed by a finalizer, and are deleted
     * when closed. It is possible that a temporary store will not be finalized
     * if the store is shutdown and that {@link File#deleteOnExit()} will not be
     * called, in which case you may have dead temporary stores lying around.
     */
    private ConcurrentWeakValueCache<UUID, TemporaryStore> stores = new ConcurrentWeakValueCache<UUID, TemporaryStore>(
            0);

    /**
     * Close all open temporary stores allocated by this factory.
     */
    synchronized public void closeAll() {
        
        final Iterator<Map.Entry<UUID,WeakReference<TemporaryStore>>> itr = stores.entryIterator();
        
        while(itr.hasNext()) {
            
            final Map.Entry<UUID,WeakReference<TemporaryStore>> entry = itr.next();
            
            final TemporaryStore store = entry.getValue().get();
            
            if (store == null) {

                // The weak reference has been cleared.
                continue;
                
            }

            // close the temporary store (it will be deleted synchronously).
            if(store.isOpen()) {

                store.close();
            
            }
            
        }
        
    }
    
}
