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
 * Created on Jan 4, 2009
 */

package com.bigdata.jini.start;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

/**
 * A service that is implemented in java and started directly using java.  The
 * value of the "jvmargs" property in the <code>com.bigdata.jini.start</code>
 * component will be combined with the "args" property for the specific service.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo extract jvmargs from com.bigdata.jini.start. if we can really uses
 *       lists and things, then it could be a list and combined with the args on
 *       the class config.
 */
public class JavaServiceConfiguration extends ServiceConfiguration {

    /**
     * 
     */
    private static final long serialVersionUID = 3688535928764283524L;

    /**
     * @param cls
     * @param config
     * @throws ConfigurationException
     */
    public JavaServiceConfiguration(Class cls, Configuration config)
            throws ConfigurationException {

        super(cls.getName(),//
                getServiceCount(cls.getName(), config),//
                getReplicationCount(cls.getName(), config),//
                concat(getJavaArgs(config),getArgs(cls.getName(), config)),//
                getParams(cls.getName(), config)//
        );

    }

    protected static String[] concat(String[] a, String[] b) {
        
        final String[] c = new String[a.length+b.length];
        
        System.arraycopy(a, 0, c, 0, a.length);

        System.arraycopy(b, 0, c, a.length, b.length);
        
        return c;
        
    }
    
    public static String[] getJavaArgs(Configuration config)
            throws ConfigurationException {

        return (String[]) config.getEntry(
                ServicesManager.Options.NAMESPACE, "args",
                String[].class, new String[] {}/* defaultValue */);
    
    }
    
}
