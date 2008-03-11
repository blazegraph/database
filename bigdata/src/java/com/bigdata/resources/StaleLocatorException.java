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
 * Created on Mar 8, 2008
 */

package com.bigdata.resources;

import com.bigdata.mdi.PartitionLocator;
import com.bigdata.service.DataService;

/**
 * An instance of this class is thrown when an index partition has been split,
 * joined, or moved to indicate that the client has a stale
 * {@link PartitionLocator} and should refresh the locator for the key range
 * covered by the named index partition and retry their request on the
 * appropriate {@link DataService}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class StaleLocatorException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = 8595818286122546905L;

    private String name, reason;
    
    /**
     * De-serialization ctor.
     */
    public StaleLocatorException() {
        super();
    }

    public String getName() {
        
        return name;
        
    }
    
    public String getReason() {
        
        return reason;
        
    }
    
    /**
     * @param name
     *            The name of the index partition.
     * @param reason
     *            "split", "join" or "move" as appropriate.
     */
    public StaleLocatorException(String name,String reason) {
        
        super("locator stale: name="+name+", reason="+reason);
        
        this.name = name;
        
        this.reason = reason;
        
    }

//    /**
//     * @param arg0
//     * @param arg1
//     */
//    public StaleLocatorException(String arg0, Throwable arg1) {
//        super(arg0, arg1);
//    }
//
//    /**
//     * @param arg0
//     */
//    public StaleLocatorException(Throwable arg0) {
//        super(arg0);
//    }

}
