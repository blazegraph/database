package com.bigdata.service.mapReduce;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Iterator;

import cutthecrap.utils.striterators.Expander;
import cutthecrap.utils.striterators.SingleValueIterator;
import cutthecrap.utils.striterators.Striterator;

/**
 * Processes files in a named directory of a (network) file system.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo specify binary vs character data (using an encoding guesser).
 */
public class FileSystemMapSource implements IMapSource {

    private File dir;

    private FileFilter filter;

    /**
     * 
     * @param dir
     *            The top level directory.
     * @param filter
     *            The filter for files to be processed. Note: You MUST
     *            return <code>true</code> when the file is a directory if
     *            you want to recursively process subdirectories.
     */
    public FileSystemMapSource(File dir, FileFilter filter) {

        if (dir == null)
            throw new IllegalArgumentException();

        if (!dir.exists())
            throw new IllegalArgumentException("Does not exist: " + dir);

        if (!dir.isDirectory())
            throw new IllegalArgumentException("Not a directory: " + dir);

        this.dir = dir;

        this.filter = filter;

    }

    public Iterator<Object> getSources() {

        return getSources(dir);

    }

    protected Iterator<Object> getSources(File dir) {

        File[] files = (filter == null ? dir.listFiles() : dir
                .listFiles(filter));

        return new Striterator(Arrays.asList(files).iterator())
                .addFilter(new Expander() {

                    private static final long serialVersionUID = -6221565889774152076L;

                    protected Iterator expand(Object arg0) {

                        File file = (File) arg0;

                        if (file.isDirectory()) {

                            return getSources(file);

                        } else {

                            return new SingleValueIterator(file);

                        }

                    }

                });

    }

}