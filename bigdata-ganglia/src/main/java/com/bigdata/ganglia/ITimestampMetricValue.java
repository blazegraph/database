/*
   Copyright (C) SYSTAP, LLC 2006-2012.  All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.bigdata.ganglia;

/**
 * A read-only interface for a reported or observed metric value with a
 * timestamp and a reference to the {@link IGangliaMetadataMessage} for that
 * metric.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface ITimestampMetricValue {

    /**
     * The metadata declaration for this metric.
     */
    public IGangliaMetadataMessage getMetadata();

    /**
     * The timestamp of the last reported/received value (milliseconds).
     * <p>
     * Note: Tmax is expressed in seconds, so be sure to do the conversion when
     * necessary.
     */
    public long getTimestamp();

    /**
     * The age in seconds of the last reported/received value (this reports
     * seconds for compatibility with tmax and dmax, both of which also use
     * seconds).  
     */
    public int getAge();

    /**
     * The last reported/received value and <code>null</code> if no value has
     * been reported/received.
     */
    public Object getValue();

}