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

package com.bigdata.util.config;

import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.LogManager;
import java.util.logging.ErrorManager;
import java.util.logging.SimpleFormatter;

/**
 * Implements a <code>java.util.logging</code> handler that redirects logged
 * messages to a Log4j Appender. This allows all the log messages to be
 * collected into a single log file.
 *
 * <b>Configuration:</b>
 * The Log4jLoggingHandler is initialized using the following LogManager
 * configuration properties:
 * <ul>
 * <li> com.bigdata.util.config.Log4jLoggingHandler.loggername
 *      specifies the name of a Log4j Logger that will be used to find
 *      the Log4j Appenders used for actually logging the message. This
 *      property is optional and defaults to the log4j root logger.
 * </ul>
 */
public class Log4jLoggingHandler extends Handler {

    private final org.apache.log4j.Logger log4jLogger;

    public Log4jLoggingHandler() {
        String log4jLoggerName = 
            LogManager.getLogManager().getProperty
                ("com.bigdata.util.config.Log4jLoggingHandler.loggername");
        if((log4jLoggerName != null) && !log4jLoggerName.equals("")) {
            this.log4jLogger = LogUtil.getLog4jLogger(log4jLoggerName);
        } else {
            this.log4jLogger = LogUtil.getLog4jRootLogger();
        }

        // Get the handler's configuration
        this.setLevel(java.util.logging.Level.FINEST);
        this.setFormatter(new SimpleFormatter());
    }

    public void publish(LogRecord record) {
        if(! isLoggable(record)) return;

        String formattedMessage;
        try {
            formattedMessage = getFormatter().formatMessage(record);
        } catch(Exception e) {
            reportError(null, e, ErrorManager.FORMAT_FAILURE);
            return;
        }
        log4jLogger.callAppenders
            ( new XLoggingEvent(log4jLogger, record, formattedMessage) );
    }

    public void close() { /* no-op */ }

    public void flush() { /* no-op */ }


    static class XLoggingEvent extends org.apache.log4j.spi.LoggingEvent {

        private final String origLoggerName;

        public XLoggingEvent(org.apache.log4j.Logger log4jLogger,
                             LogRecord record,
                             String formattedMessage)
        {
            super( "org.apache.log4j.Logger",
                   log4jLogger,
                   record.getMillis(),
                   translateLevel(record.getLevel()),
                   formattedMessage,
                   record.getThrown() );

            this.origLoggerName = record.getLoggerName();
        }

        // Override to return the name of the java.util logger
        public String getLoggerName() {
            return origLoggerName;
        }

        protected static org.apache.log4j.Level translateLevel
                                              (java.util.logging.Level level)
        {
            int lv = level.intValue();
            if(lv > java.util.logging.Level.SEVERE.intValue()) {
                return org.apache.log4j.Level.FATAL;
            } else if(lv == java.util.logging.Level.SEVERE.intValue()) {
                return org.apache.log4j.Level.ERROR;
            } else if(lv >= java.util.logging.Level.WARNING.intValue()) {
                return org.apache.log4j.Level.WARN;
            } else if(lv >= java.util.logging.Level.INFO.intValue()) {
                return org.apache.log4j.Level.INFO;
            } else if(lv >= java.util.logging.Level.CONFIG.intValue()) {
                return org.apache.log4j.Level.INFO;
            } else if(lv >= java.util.logging.Level.FINE.intValue()) {
                return org.apache.log4j.Level.DEBUG;
            } else {
                return org.apache.log4j.Level.TRACE;
            }
        }
    }
}
