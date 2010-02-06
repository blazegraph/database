/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Aug 15, 2007
 */

package com.bigdata.sparse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class validates column and schema name constraints. 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NameChecker {

    /**
     * The constraint on schema and column names.
     */
    public final static Pattern pattern_name = Pattern.compile("[\\w._/]+");
    
    /**
     * Assert that the string is valid as the name of a schema. Names must be
     * alphanumeric and may also include any of {<code>.</code>,
     * <code>_</code>, or <code>/</code>}.
     * 
     * @param s
     *            The string.
     * 
     * @throws IllegalArgumentException
     *             if the string is not valid as the name of a schema.
     */
    static public void assertSchemaName(String s)
            throws IllegalArgumentException {

        if (s == null)
            throw new IllegalArgumentException();

        if (s.length() == 0)
            throw new IllegalArgumentException();

        if (s.indexOf('\0') != -1)
            throw new IllegalArgumentException(); 

        final Matcher m = pattern_name.matcher(s);
        
        if(!m.matches()) throw new IllegalArgumentException();
        
    }

    /**
     * Assert that the string is valid as the name of a column. Names must be
     * alphanumeric and may also include any of {<code>.</code>,
     * <code>_</code>, or <code>/</code>}.
     * 
     * @param s
     *            The string.
     * 
     * @throws IllegalArgumentException
     *             if the string is not valid as the name of a column.
     */
    static public void assertColumnName(final String s)
            throws IllegalArgumentException {

        if (s == null)
            throw new IllegalArgumentException();

        if (s.length() == 0)
            throw new IllegalArgumentException();

        if (s.indexOf('\0') != -1)
            throw new IllegalArgumentException();

        final Matcher m = pattern_name.matcher(s);

        if (!m.matches())
            throw new IllegalArgumentException(s);

    }

}
