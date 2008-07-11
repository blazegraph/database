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
 * Created on Apr 6, 2008
 */

package com.bigdata.service;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import org.apache.log4j.MDC;

import com.bigdata.journal.ITimestampService;
import com.bigdata.journal.ITransactionManager;
import com.bigdata.util.MillisecondTimestampFactory;

/**
 * Basic {@link ITimestampService} implementation provides distinct and strictly
 * increasing timestamps with no more than millisecond resolution.
 * 
 * @todo this will probably get upgraded to an {@link ITransactionManager}. See
 *       {@link TransactionService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class TimestampService implements ITimestampService, IServiceShutdown {

    private final Properties properties;
    
    /**
     * An object wrapping the properties specified to the ctor.
     * @return
     */
    public Properties getProperties() {
        
        return new Properties( properties );
        
    }
    
    /**
     * Options understood by the {@link TimestampService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface Options {
        
    }
    
    /**
     * @param properties
     *            See {@link Options}.
     */
    public TimestampService(Properties properties) {

        if (properties == null)
            throw new IllegalArgumentException();

        // clone to avoid side effects of modifications by the caller.
        this.properties = (Properties) properties.clone();
        
        this.open = true;
        
    }

    public long nextTimestamp() throws IOException {
        
        return MillisecondTimestampFactory.nextMillis();
        
    }
    
    /**
     * Sets up the {@link MDC} logging context. You should do this on every
     * client facing point of entry and then call {@link #clearLoggingContext()}
     * in a <code>finally</code> clause. You can extend this method to add
     * additional context.
     * <p>
     * This implementation add the "serviceUUID" parameter to the {@link MDC}.
     * The serviceUUID is, in general, assigned asynchronously by the service
     * registrar. Once the serviceUUID becomes available it will be added to the
     * {@link MDC}. This datum can be injected into log messages using
     * %X{serviceUUID} in your log4j pattern layout.
     */
    protected void setupLoggingContext() {

        try {
            
            // Note: This _is_ a local method call.
            
            UUID serviceUUID = getServiceUUID();
            
            // Will be null until assigned by the service registrar.
            
            if (serviceUUID == null) {

                return;
                
            }
            
            // Add to the logging context for the current thread.
            
            MDC.put("serviceUUID", serviceUUID.toString());

        } catch(Throwable t) {
            /*
             * Ignore.
             */
        }
        
    }

    /**
     * Clear the logging context.
     */
    protected void clearLoggingContext() {
        
        MDC.remove("serviceUUID");
        
    }

    /**
     * Return the {@link UUID} for the {@link ITimestampService}.
     * 
     * @throws IOException
     */
    abstract public UUID getServiceUUID() throws IOException;

    public boolean isOpen() {
        
        return open;
        
    }
    
    synchronized public void shutdown() {
 
        open = false;
        
    }
    
    synchronized public void shutdownNow() {
        
        open = false;
        
    }

    private boolean open;
    
}
