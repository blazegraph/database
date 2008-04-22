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
 * Created on Mar 26, 2008
 */

package com.bigdata.counters;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.log4j.Logger;

import com.bigdata.util.concurrent.DaemonThreadFactory;

/**
 * Command manages the execution and termination of a native process and an
 * object reading the output of that process.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ActiveProcess {
    
    static protected final Logger log = Logger.getLogger(ActiveProcess.class);

    /**
     * Used to read {@link #is} and aggregate the performance data reported
     * by the {@link #process}.
     */
    protected final ExecutorService readService = Executors
            .newSingleThreadExecutor(DaemonThreadFactory
                    .defaultThreadFactory());
    
    protected Process process = null;

    protected InputStream is = null;

    protected volatile Future readerFuture;

    /**
     * 
     * @param command
     *            The command to be executed. See
     *            {@link ProcessBuilder#command(List)}.
     * @param collector
     */
    public ActiveProcess(List<String> command,
            AbstractProcessCollector collector) {

        if (command == null)
            throw new IllegalArgumentException();

        if (command.isEmpty())
            throw new IllegalArgumentException();

        if (collector == null)
            throw new IllegalArgumentException();

        // log the command that will be run.
        {
            
            StringBuilder sb = new StringBuilder();

            for (String s : command) {

                sb.append( s +" ");

            }

            log.info("command:\n" + sb);

        }
                    
        try {

            ProcessBuilder tmp = new ProcessBuilder(command);

            collector.setEnvironment(tmp.environment());
            
            process = tmp.start();

        } catch (IOException e) {

            throw new RuntimeException(e);

        }

        /*
         * Note: Process is running but the reader on the process output has
         * not been started yet!
         */
        
    }

    /**
     * Attaches the reader to the process, which was started by the ctor and
     * is already running.
     * 
     * @param processReader
     *            The reader.
     */
    public void start(AbstractProcessReader processReader) {
        
        log.info("");

        if (processReader == null)
            throw new IllegalArgumentException();

        if (readerFuture != null)
            throw new IllegalStateException();

        is = process.getInputStream();

        assert is != null;
        
        /*
         * @todo restart processes if it dies before we shut it down, but no
         * more than some #of tries. if the process dies then we will not have
         * any data for this host.
         * 
         * @todo this code for monitoring processes is a mess and should
         * probably be re-written from scratch. the processReader task
         * references the readerFuture via isAlive() but the readerFuture is not
         * even assigned until after we submit the processReader task, which
         * means that it can be running before the readerFuture is set! The
         * concrete implementations of ProcessReaderHelper all poll isAlive()
         * until the readerFuture becomes available.
         */

        log.info("starting process reader: "+processReader);

        processReader.start(is);

        log.info("submitting process reader task: "+processReader);

        readerFuture = readService.submit(processReader);
        
        log.info("readerFuture: done="+readerFuture.isDone());

    }

    /**
     * Stops the process
     */
    public void stop() {

        if (readerFuture == null)
            throw new IllegalStateException();

        // attempt to cancel the reader.
        readerFuture.cancel(true/* mayInterruptIfRunning */);

        // shutdown the thread running the reader.
        readService.shutdownNow();

        if (process != null) {

            // destroy the running process.
            process.destroy();

            process = null;

            is = null;

        }

        readerFuture = null;

    }
    
    /**
     * Return <code>true</code> unless the process is known to be dead.
     */
    public boolean isAlive() {
        
        if(readerFuture==null || readerFuture.isDone() || process == null || is == null) {
            
            log.info("Not alive: readerFuture="
                    + readerFuture
                    + (readerFuture != null ? "done=" + readerFuture.isDone()
                            : "") + ", process=" + process + ", is=" + is);
            
            return false;
            
        }
        
        return true;
        
    }
    
}
