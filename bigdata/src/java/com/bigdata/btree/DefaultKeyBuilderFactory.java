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
 * Created on Jul 3, 2008
 */

package com.bigdata.btree;

import java.io.Serializable;
import java.util.Properties;

/**
 * Default factory for Unicode {@link IKeyBuilder}s. This does NOT generate
 * thread-local instances.
 * 
 * @see ThreadLocalKeyBuilderFactory
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultKeyBuilderFactory implements IKeyBuilderFactory, Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -3285057306742134508L;
    
    private final Properties properties;
    
    /**
     * Configures instances using {@link KeyBuilder#newUnicodeInstance()}
     */
    public DefaultKeyBuilderFactory() {
        
        properties = null;
        
    }
    
    /**
     * Configures instances using
     * {@link KeyBuilder#newUnicodeInstance(Properties)}
     * 
     * @param properties
     *            Used to configure the {@link IKeyBuilder}.
     */
    public DefaultKeyBuilderFactory(Properties properties) {
        
        if (properties == null)
            throw new IllegalArgumentException();
        
        this.properties = properties;
        
    }

    public IKeyBuilder getKeyBuilder() {

        if (properties == null) {

            return KeyBuilder.newUnicodeInstance();
            
        } else {
            
            return KeyBuilder.newUnicodeInstance(properties);
            
        }
        
    }

}
