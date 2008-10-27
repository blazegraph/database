package com.bigdata.counters;

import java.io.InputStream;

import org.apache.log4j.Logger;

/**
 * A {@link Runnable} that reads the output of an {@link ActiveProcess}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public abstract class AbstractProcessReader implements Runnable {
    
    static protected final Logger log = Logger
            .getLogger(AbstractProcessReader.class);

    /**
     * True iff the {@link #log} level is DEBUG or less.
     */
    final protected static boolean DEBUG = log.isInfoEnabled();

    /**
     * True iff the {@link #log} level is INFO or less.
     */
    final protected static boolean INFO = log.isDebugEnabled();

    /**
     * The {@link InputStream} from which the output of the process will be
     * read.
     */
    protected InputStream is;

    /**
     * Saves a reference to the {@link InputStream}.
     * 
     * @param is
     *            The input stream from which the output of the process will
     *            be read.
     */
    public void start(InputStream is) {

        if(INFO) 
            log.info("");

        if (is == null)
            throw new IllegalArgumentException();

        this.is = is;

    }

}