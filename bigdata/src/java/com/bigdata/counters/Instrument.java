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
 * Created on Mar 14, 2008
 */

package com.bigdata.counters;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * Abstract class for reporting instrumented values supporting some useful
 * conversions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class Instrument<T> implements IInstrument<T> {

    protected static Logger log = Logger.getLogger(Instrument.class);
    
    /** <code>N/A</code> */
    protected static final transient String NA = "N/A";
    
    /**
     * Converts an event count whose durations were measured in elapsed
     * nanoseconds to an event rate per second.
     * 
     * @param counter
     *            The event count.
     * @param nanos
     *            The elapsed nanoseconds for the events.
     * 
     * @return Either {@value #NA} if <i>nanos</i> is ZERO (0) or the rate per
     *         second for the event.
     * 
     * @todo move to a util class? deprecate and remove in favor of Double counters?
     */
    public final String nanosToPerSec(long counter, long nanos) {

        long secs = TimeUnit.SECONDS.convert(nanos, TimeUnit.NANOSECONDS);

        if (secs == 0L)
            return NA;

        return "" + counter / secs;

    }

    /**
     * Take a sample, setting the current value and timestamp using either
     * {@link #setValue(Object)} or {@link #setValue(Object, long)}.
     */
    abstract protected void sample();    

    /**
     * Set the value.
     * 
     * @param value
     *            The value, which will be associated with the current time as
     *            reported by {@link System#currentTimeMillis()}.
     */
    final public void setValue(T value) {
        
        setValue(value, System.currentTimeMillis());
        
    }

    /**
     * Set the value.
     * 
     * @param value
     *            The value.
     * @param timestamp
     *            The timestamp for that value.
     */
    final public void setValue(T value,long timestamp) {

        if(log.isInfoEnabled())
            log.info("value="+value+", timestamp="+timestamp);
        
        this.value = value;
        
        this.lastModified = timestamp;
        
    }
    
    final public T getValue() {
        
        sample();
        
        return value;
        
    }
    
    /**
     * Return the current value without taking another {@link #sample()}
     */
    public T getCurrentValue() {
        
        return value;
        
    }
    
    final public long lastModified() {
        
        return lastModified;
        
    }
    
    /**
     * The current value -or- <code>null</code> if there is no current value.
     */
    protected T value;
    
    /**
     * The timestamp associated with {@link #value}.
     */
    protected long lastModified;

}
