package com.bigdata.service.mapReduce;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.UUID;

/**
 * Abstract base class for {@link IMapTask}s accepting a filename as the
 * "key" and the file contents as the "value".
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class AbstractFileInputMapTask extends
        AbstractMapTask {

    protected AbstractFileInputMapTask(UUID uuid, Object source, int nreduce,
            IHashFunction hashFunction) {

        super(uuid, source, nreduce, hashFunction);

    }
    
    /**
     * Used to read from the {@link #getSource()} when it is a {@link File}.
     * 
     * @param file
     *            The data source.
     *            
     * @throws Exception
     */
    final public void input(File file) throws Exception {

        MapReduceMaster.log.info("Start file: " + file);

        final InputStream is = new BufferedInputStream(new FileInputStream(
                file));

        try {

            input(file, is);

            MapReduceMaster.log.info("Done file : " + file + ", ntuples="
                            + getTupleCount());

        } finally {

            try {

                is.close();

            } catch (Throwable t) {

                MapReduceMaster.log.warn("Problem closing input stream: " + file, t);

            }

        }

    }

    abstract protected void input(File input, InputStream is)
            throws Exception;

}
