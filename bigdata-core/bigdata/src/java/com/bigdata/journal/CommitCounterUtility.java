/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Formatter;

import org.apache.log4j.Logger;

import com.bigdata.ha.halog.IHALogReader;

/**
 * Utility class for operations on files that are named using a commit counter.
 * <p>
 * The commit counter based files are arranged in a heirarchial directory
 * structure with 3 digits per directory and 7 directory levels. These levels
 * are labeled with depths <code>[0..6]</code>. The root directory is at depth
 * ZERO (0). Each directory contains up to <code>1000</code> children. The
 * children in the non-leaf directories are subdirectories labeled
 * <code>0..999</code>. The leaf directories are at depth SIX (6). Leaf
 * directories contain files. Each file in a leaf directory is labeled with a
 * <code>21</code> digit base name and some purpose specific file extension.
 * Each such file has data for the specific commit point encoded by the basename
 * of the file.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class CommitCounterUtility {

    private static final Logger log = Logger
            .getLogger(CommitCounterUtility.class);
    
    /**
     * The number of base-10 digits per directory level. This allows children
     * having labels <code>000...999</code>. Thus there are <code>1000</code>
     * children per directory.
     */
    private static final int DIGITS_PER_DIR = 3;
    
    /**  The number of files per directory. */
    private static final int FILES_PER_DIR = 1000;
    
    /** The depth of the root directory. */
    private static final int ROOT_DIR_DEPTH = 0;
    
    /** The depth of a leaf directory. */
    private static final int LEAF_DIR_DEPTH = 6;
    
    /**
     * The #of digits (21) in the base file name for a commit counter as
     * formatted by {@link #getCommitCounterStr(long)}.
     * <p>
     * Note: 21 := (leafDirDepth+1) * digitsPerDir
     */
    private static final int BASENAME_DIGITS = 21;

    /**
     * The {@link Formatter} string that is used to generate the base name of
     * the files in the leaf directories. This string represents the commit
     * counter value with leading zeros. The leading zeros are relied upon to
     * impose an ordering over the base names of the files using a sort.
     */
    private static final String FORMAT_STR = "%0" + BASENAME_DIGITS + "d";
    
    /**
     * The #of digits (21) in the base file name for a commit counter as
     * formatted by {@link #getCommitCounterStr(long)}.
     * <p>
     * Note: 21 := (leafDirDepth+1) * digitsPerDir
     */
    public static int getBasenameDigits() {

        return BASENAME_DIGITS;

    }

    /**
     * The number of base-10 digits per directory level (
     * {@value #DIGITS_PER_DIR}). This allows children having labels
     * <code>000...999</code>. Thus there are <code>1000</code> children per
     * directory.
     */
    public static int getDigitsPerDirectory() {

        return DIGITS_PER_DIR;

    }

    /**
     * The number of files per directory ({@value #FILES_PER_DIR}).
     */
    public static int getFilesPerDirectory() {

        return FILES_PER_DIR;
        
    }
    
    /**
     * The depth of the root directory ({@value #ROOT_DIR_DEPTH}).
     */
    public static int getRootDirectoryDepth() {

        return ROOT_DIR_DEPTH;
        
    }
    
    /**
     * The depth of a leaf directory ({@value #LEAF_DIR_DEPTH}).
     */
    public static int getLeafDirectoryDepth() {

        return LEAF_DIR_DEPTH;
        
    }
    
    /**
     * Return the name of the {@link File} associated with the commitCounter.
     * 
     * @param dir
     *            The directory spanning all such files.
     * @param commitCounter
     *            The commit counter for the current root block on the journal.
     * @param ext
     *            The filename extension.
     *            
     * @return The name of the corresponding snapshot file.
     */
    public static File getCommitCounterFile(final File dir,
            final long commitCounter, final String ext) {

        /*
         * Format the name of the file.
         * 
         * Note: The commit counter in the file name should be zero filled to 20
         * digits so we have the files in lexical order in the file system (for
         * convenience). [I have changed this to 21 digits since that can be
         * broken up into groups of three per below.]
         * 
         * Note: The files are placed into a recursive directory structure with
         * 1000 files per directory. This is done by taking the lexical form of
         * the file name and then partitioning it into groups of THREE (3)
         * digits.
         */
        final String basename = getCommitCounterStr(commitCounter);

        /*
         * Now figure out the recursive directory name.
         */
        File t = dir;
        
        for (int i = 0; i < (BASENAME_DIGITS - DIGITS_PER_DIR); i += DIGITS_PER_DIR) {

            t = new File(t, basename.substring(i, i + DIGITS_PER_DIR));

        }

        final File file = new File(t, basename + ext);

        return file;
        
    }

    /**
     * Format the commit counter with leading zeros such that it will be
     * lexically ordered in the file system.
     * 
     * @param commitCounter
     *            The commit counter.
     *            
     * @return The basename of the file consisting of the foramtted commit
     *         counter with the appropriate leading zeros.
     */
    public static String getCommitCounterStr(final long commitCounter) {

        final StringBuilder sb = new StringBuilder(BASENAME_DIGITS);

        final Formatter f = new Formatter(sb);

        f.format(FORMAT_STR, commitCounter);
        f.flush();
        f.close();

        final String basename = sb.toString();

        return basename;

    }

    /**
     * Parse out the commitCounter from the file name.
     * 
     * @param name
     *            The file name
     * @param ext
     *            The expected file extension.
     * 
     * @return The commit counter from the file name.
     * 
     * @throws IllegalArgumentException
     *             if either argument is <code>null</code>
     * @throws NumberFormatException
     *             if the file name can not be interpreted as a commit counter.
     */
    public static long parseCommitCounterFile(final String name,
            final String ext) throws NumberFormatException {

        if (name == null)
            throw new IllegalArgumentException();

        if (ext == null)
            throw new IllegalArgumentException();

        // Strip off the filename extension.
        final int len = name.length() - ext.length();

        final String fileBaseName = name.substring(0, len);

        // Closing commitCounter for snapshot file.
        final long commitCounter = Long.parseLong(fileBaseName);

        return commitCounter;
        
    }

    /**
     * Return the basename of the file (strip off the extension).
     * 
     * @param name
     *            The file name.
     * @param ext
     *            The extension.
     *            
     * @return The base name of the file without the extension.
     */
    public static String getBaseName(final String name, final String ext) {

        final String basename = name.substring(0, name.length() - ext.length());

        return basename;

    }
    
    /**
     * Recursively removes any files and subdirectories and then removes the
     * file (or directory) itself. Only files recognized by
     * {@link #getFileFilter()} will be deleted.
     * <p>
     * Note: A dedicated version of this method exists here to thrown an
     * {@link IOException} if we can not delete a file. This is deliberate. It
     * is thrown to prevent a REBUILD from proceeding unless we can clear out
     * the old snapshot and HALog files.
     * 
     * @param errorIfDeleteFails
     *            When <code>true</code> and {@link IOException} is thrown if a
     *            file matching the filter or an empty directory matching the
     *            filter can not be removed. When <code>false</code>, that event
     *            is logged @ WARN instead.
     * @param f
     *            A file or directory.
     * @param fileFilter
     *            A filter matching the files and directories to be visited and
     *            removed. If directories are matched, then they will be removed
     *            iff they are empty. A depth first visitation is used, so the
     *            files and sub-directories will be cleared before we attempt to
     *            remove the parent directory.
     * @throws IOException
     *             if any file or non-empty directory can not be deleted (iff
     *             <i>errorIfDeleteFails</i> is <code>true</code>).
     */
    public static void recursiveDelete(final boolean errorIfDeleteFails,
            final File f, final FileFilter fileFilter) throws IOException {

        if (f.isDirectory()) {

            final File[] children = f.listFiles(fileFilter);

            for (int i = 0; i < children.length; i++) {

                recursiveDelete(errorIfDeleteFails, children[i], fileFilter);

            }

        }

        if (!f.exists())
            return;

        if (log.isInfoEnabled())
            log.info("Removing: " + f);

        final boolean deleted = f.delete();

        if (!deleted) {
        
            if (f.isDirectory() && f.list().length != 0) {

                // Ignore non-empty directory.
                return;
                
            }
            
            final String msg = "Could not remove file: " + f;
            
            if (errorIfDeleteFails) {
            
                // Complete if we can not delete a file.
                throw new IOException(msg);

            } else {
                
                log.warn(msg);

            }
        
        }

    }

    /**
     * Find and return the {@link File} associated with the greatest commit
     * counter. This uses a reverse order search to locate the most recent file
     * very efficiently.
     * 
     * @param f
     *            The root of the directory structure for the snapshot or HALog
     *            files.
     * @param fileFilter
     *            Either the {@link SnapshotManager#SNAPSHOT_FILTER} or the
     *            {@link IHALogReader#HALOG_FILTER}.
     * 
     * @return The file from the directory structure associated with the
     *         greatest commit counter.
     * 
     * @throws IOException
     */
    public static File findGreatestCommitCounter(final File f,
            final FileFilter fileFilter) throws IOException {

        if (f == null)
            throw new IllegalArgumentException();

        if (fileFilter == null)
            throw new IllegalArgumentException();
        
        if (f.isDirectory()) {

            final File[] files = f.listFiles(fileFilter);

            /*
             * Sort into (reverse) lexical order to force visitation in
             * (reverse) lexical order.
             * 
             * Note: This should work under any OS. Files will be either
             * directory names (3 digits) or filenames (21 digits plus the file
             * extension). Thus the comparison centers numerically on the digits
             * that encode either part of a commit counter (subdirectory) or an
             * entire commit counter (HALog file).
             */
            Arrays.sort(files,ReverseFileComparator.INSTANCE);

            for (int i = 0; i < files.length; i++) {

                final File tmp = findGreatestCommitCounter(files[i], fileFilter);

                if (tmp != null) {

                    // Done.
                    return tmp;

                }

            }

        } else if (fileFilter.accept(f)) {

            // Match
            return f;

        }

        // No match.
        return null;

   }

    /**
     * Impose a reverse sort on files.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
     *         Thompson</a>
     */
    private static class ReverseFileComparator implements Comparator<File> {

        @Override
        public int compare(final File o1, final File o2) {

            return o2.compareTo(o1);

        }

        /** Impose a reverse sort on files. */
        private static final Comparator<File> INSTANCE = new ReverseFileComparator();

    }
    
}
