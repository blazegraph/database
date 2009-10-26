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
 * Created on Sep 10, 2009
 */

package com.bigdata;

import com.bigdata.jini.start.process.ProcessHelper;

/**
 * A class for those few statics that it makes sense to reference from other
 * places.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataStatics {

    /**
     * A flag used for a variety of purposes during performance tuning. The use
     * of the flag makes it easier to figure out where those {@link System#err}
     * messages are coming from. This should always be off in the trunk.
     */
    public static final boolean debug = false;

    /**
     * The #of lines of output from a child process which will be echoed onto
     * {@link System#out} when that child process is executed. This makes it
     * easy to track down why a child process dies during service start. If you
     * want to see more output from the child process, then you should set the
     * log level for the {@link ProcessHelper} class to INFO.
     * 
     * @see ProcessHelper
     */
    public static int echoProcessStartupLineCount = 20;
    
}
