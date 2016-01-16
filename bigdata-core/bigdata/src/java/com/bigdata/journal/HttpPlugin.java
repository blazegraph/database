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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

import org.apache.log4j.Logger;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
import com.bigdata.counters.httpd.CounterSetHTTPD;

/**
 * {@link CounterSetHTTPD} plug-in.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
@SuppressWarnings("deprecation")
public class HttpPlugin implements IPlugIn<Journal, CounterSetHTTPD> {

    private static final Logger log = Logger.getLogger(HttpPlugin.class);

    public interface Options {
        
        /**
         * Integer option specifies the port on which an httpd service will be
         * started that exposes the {@link CounterSet} for the client (default
         * {@value #DEFAULT_HTTPD_PORT}). When ZERO (0), a random port will be
         * used and the actual port selected may be discovered using
         * {@link Journal#getHttpdURL()}. The httpd service may be disabled by
         * specifying <code>-1</code> as the port.
         */
        String HTTPD_PORT = Journal.class.getName() + ".httpdPort";

        /**
         * The default http service port is <code>-1</code>, which means
         * performance counter reporting is disabled by default.
         */
        String DEFAULT_HTTPD_PORT = "-1";

    }

    /**
     * httpd reporting the live counters -or- <code>null</code> if not enabled.
     * <p>
     * Note: Guarded by <code>synchronized(this)</code>.
     * 
     * @see Options#HTTPD_PORT
     */
    private CounterSetHTTPD httpd = null;
    
    /**
     * The URL that may be used to access the httpd service exposed by this
     * client -or- <code>null</code> if not enabled.
     * <p>
     * Note: Guarded by <code>synchronized(this)</code>.
     */
    private String httpdURL = null;

    /**
     * {@inheritDoc}
     * <p>
     * Start the local httpd service (if enabled). The service is started on the
     * {@link Journal#getHttpdPort()}, on a randomly assigned port if the port
     * is <code>0</code>, or NOT started if the port is <code>-1</code>. If the
     * service is started, then the URL for the service is reported to the load
     * balancer and also written into the file system. When started, the httpd
     * service will be shutdown with the federation.
     */
    @Override
    public void startService(final Journal indexManager) {

        final int httpdPort = Integer.valueOf(indexManager.getProperty(
                Options.HTTPD_PORT, Options.DEFAULT_HTTPD_PORT));

        if (log.isInfoEnabled())
            log.info(Options.HTTPD_PORT + "=" + httpdPort
                    + (httpdPort == -1 ? " (disabled)" : ""));

        if (httpdPort == -1) {

            return;

        }

        final CounterSetHTTPD httpd;
        try {

            httpd = new CounterSetHTTPD(httpdPort, indexManager);

        } catch (IOException e) {

            log.error("Could not start httpd: port=" + httpdPort, e);

            return;

        }

        final String httpdURL;
        try {

            httpdURL = "http://"
                    + AbstractStatisticsCollector.fullyQualifiedHostName + ":"
                    + httpd.getPort() + "/?path="
                    + URLEncoder.encode("", "UTF-8");
            
        } catch (UnsupportedEncodingException ex) {
            
            log.error("Could not start httpd: " + ex, ex);

            httpd.shutdownNow();
            
            return;
        
        }

        synchronized (this) {

            // save reference to the daemon.
            this.httpd = httpd;

            // the URL that may be used to access the local httpd.
            this.httpdURL = httpdURL;

            if (log.isInfoEnabled())
                log.info("Performance counters: " + httpdURL);

        }

    }

    @Override
    public void stopService(final boolean immediateShutdown) {

        synchronized (this) {
        
            if (httpd != null) {
            
                httpd.shutdown();

                httpd = null;

                httpdURL = null;
            
            }

        }

    }

    @Override
    public CounterSetHTTPD getService() {

        synchronized (this) {

            return httpd;

        }

    }

    @Override
    public boolean isRunning() {

        synchronized (this) {

            return httpd != null;

        }

    }

    /**
     * The URL that may be used to access the httpd service exposed by this
     * client -or- <code>null</code> if not enabled.
     */
    final public String getHttpdURL() {

        synchronized (this) {

            return httpdURL;

        }

    }
    
}
