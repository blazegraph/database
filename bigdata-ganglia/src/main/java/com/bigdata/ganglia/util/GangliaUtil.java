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
package com.bigdata.ganglia.util;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * Utility class.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class GangliaUtil {

    /**
     * Parses a space and/or comma separated sequence of server
     * specifications of the form <i>hostname</i> or <i>hostname:port</i>.
     * If the specs string is null, defaults to
     * <i>localhost</i>:<i>defaultPort</i>.
     * 
     * @return An array of one or more {@link InetSocketAddress} objects.
     */
    static public InetSocketAddress[] parse(final String specs,
            final String defaultAddr, final int defaultPort) {
        
        final List<InetSocketAddress> result = new LinkedList<InetSocketAddress>();
        
        if (specs == null) {

            result.add(new InetSocketAddress(defaultAddr, defaultPort));
            
        } else {

            final String[] specStrings = specs.split("[ ,]+");
            
            for (String specString : specStrings) {
            
                final int colon = specString.indexOf(':');
                
                if (colon < 0 || colon == specString.length() - 1) {
                
                    result.add(new InetSocketAddress(specString, defaultPort));
                    
                } else {
                    
                    final String hostname = specString.substring(0, colon);
                    
                    final int port = Integer.parseInt(specString
                            .substring(colon + 1));

                    result.add(new InetSocketAddress(hostname, port));
                    
                }
                
            }
            
        }

        return result.toArray(new InetSocketAddress[result.size()]);
        
    }

}
