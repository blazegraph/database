package com.bigdata.resources;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.bigdata.journal.IResourceManager;
import com.bigdata.resources.ResourceManager.Options;

/**
 * The default implementation accepts directories under the configured
 * {@link IResourceManager#getDataDir()} and files with either
 * {@link com.bigdata.journal.Options#JNL} or
 * {@link com.bigdata.journal.Options#SEG} file extensions.
 * <p> *
 * <P>
 * If you define additional files that are stored within the
 * {@link ResourceManager#getDataDir()} then you SHOULD subclass
 * {@link ResourceFileFilter} to recognize those files and override
 * {@link ResourceManager#newFileFilter()} method to return your
 * {@link ResourceFileFilter} subclass.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ResourceFileFilter implements FileFilter {

    /**
     * 
     */
    private final StoreFileManager resourceManager;

    protected static final Logger log = Logger.getLogger(ResourceFileFilter.class);
    
    /**
     * @param resourceManager
     */
    public ResourceFileFilter(StoreFileManager resourceManager) {

        if (resourceManager == null)
            throw new IllegalArgumentException();
        
        this.resourceManager = resourceManager;
        
    }

    /**
     * Override this method to extend the filter.
     * 
     * @param f
     *            A file passed to {@link #accept(File)}
     *            
     * @return <code>true</code> iff the file should be accepted by the
     *         filter.
     */
    protected boolean accept2(File f) {

        return false;

    }

    final public boolean accept(File f) {

        if (f.isDirectory()) {

            //                // Either f iff it is a directory or the directory containing f.
            //                final File dir = f.isDirectory() ? f : f.getParentFile();

            final File dir = f;

            // get the canonical form of the directory.
            final String fc;
            try {

                fc = dir.getCanonicalPath();

            } catch (IOException ex) {

                throw new RuntimeException(ex);

            }

            if (!fc.startsWith(resourceManager.getDataDir().getPath())) {

                throw new RuntimeException("File not in data directory: file="
                        + f + ", dataDir=" + resourceManager.dataDir);

            }

            // directory inside of the data directory.
            return true;

        }

        if (f.getName().endsWith(Options.JNL)) {

            // journal file.
            return true;

        }

        if (f.getName().endsWith(Options.SEG)) {

            // index segment file.
            return true;

        }

        if (accept2(f)) {

            // accepted by subclass.
            return true;

        }

        log.warn("Unknown file: " + f);

        return false;

    }

}
