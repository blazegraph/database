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
 * Created on Apr 20, 2008
 */

package com.bigdata.counters;

import com.bigdata.counters.ICounterSet.IInstrumentFactory;

/**
 * Used to read in {@link CounterSet} XML, aggregating data into
 * {@link HistoryInstrument}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultInstrumentFactory implements IInstrumentFactory {

    public static final DefaultInstrumentFactory INSTANCE = new DefaultInstrumentFactory();
    
    private DefaultInstrumentFactory() {
        
    }

    public IInstrument newInstance(Class type) {

        if(type==null) throw new IllegalArgumentException();
        
        if (type == Double.class || type == Float.class) {

            return new HistoryInstrument<Double>(new Double[] {});
            
        } else if (type == Long.class || type == Integer.class) {
            
            return new HistoryInstrument<Long>(new Long[] {});
            
        } else if( type == String.class ) {
            
            return new StringInstrument();
            
        } else {
            
            throw new UnsupportedOperationException("type: "+type);
            
        }
        
    }

    static class StringInstrument extends Instrument<String> {
      
        public void sample() {}
        
    };
    
}
