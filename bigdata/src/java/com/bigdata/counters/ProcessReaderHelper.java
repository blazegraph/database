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
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;

public abstract class ProcessReaderHelper extends
        AbstractProcessReader {

    /**
     * The {@link Reader} from which the output of the process will be read.
     */
    protected LineNumberReader r = null;

    public ProcessReaderHelper() {

    }

    /**
     * Creates a {@link LineNumberReader} from the {@link InputStream}.
     * 
     * @param is
     *            The input stream from which the output of the process will
     *            be read.
     */
    public void start(InputStream is) {

        if (r != null)
            throw new IllegalStateException();
        
        super.start( is );
        
        r = new LineNumberReader( new InputStreamReader( is ));
        
    }

    /**
     * Override to return the {@link ActiveProcess}.
     */
    abstract protected ActiveProcess getActiveProcess();
    
    /**
     * Returns the next line and blocks if a line is not available.
     * 
     * @return The next line.
     * 
     * @throws InterruptedException 
     * 
     * @throws IOException
     *             if the source is closed.
     * @throws InterruptedException
     *             if the thread has been interrupted (this is
     *             normal during shutdown).
     */
    public String readLine() throws IOException, InterruptedException {
        
        while(getActiveProcess().isAlive()) {
            
            if(Thread.currentThread().isInterrupted()) {
                
                throw new InterruptedException();
                
            }
            
            if(!r.ready()) {
                
                Thread.sleep(100/*ms*/);
                
                continue;
                
            }

            final String s = r.readLine();
            
            if(DEBUG) {
                
                log.debug(s);
                
            }
            
            return s;
            
        }
        
        throw new IOException("Closed");
        
    }
    
    public void run() {
        
        try {
            readProcess();
        } catch (InterruptedException e) {
            AbstractStatisticsCollector.log.info("Interrupted - will halt.");
        } catch (Exception e) {
            AbstractStatisticsCollector.log.fatal(e.getMessage(),e);
        }
        
    }

    /**
     * Responsible for reading the data.
     * 
     * @throws Exception
     */
    abstract protected void readProcess() throws Exception;

}