//
//  ========================================================================
//  Copyright (c) 1995-2014 Mort Bay Consulting Pty. Ltd.
//  ------------------------------------------------------------------------
//  All rights reserved. This program and the accompanying materials
//  are made available under the terms of the Eclipse Public License v1.0
//  and Apache License v2.0 which accompanies this distribution.
//
//      The Eclipse Public License is available at
//      http://www.eclipse.org/legal/epl-v10.html
//
//      The Apache License v2.0 is available at
//      http://www.opensource.org/licenses/apache2.0.php
//
//  You may elect to redistribute this code under either of these licenses.
//  ========================================================================
//
/*
 * Note: This class was extracted from org.eclipse.jetty.util.StringUtil.
 * It contains only those methods that we need that are not already part
 * of the general servlet API.  (We can not rely on jetty being present
 * since the WAR deployment does not bundle the jetty dependencies.)
 */
package com.bigdata.rdf.sail.webapp.client;

/** Fast String Utilities.
*
* These string utilities provide both convenience methods and
* performance improvements over most standard library versions. The
* main aim of the optimizations is to avoid object creation unless
* absolutely required.
*/
public class StringUtil {

    /**
     * Convert String to an long. Parses up to the first non-numeric character.
     * If no number is found an IllegalArgumentException is thrown
     * 
     * @param string
     *            A String containing an integer.
     * @return an int
     */
    public static long toLong(final String string) {
        long val = 0;
        boolean started = false;
        boolean minus = false;

        for (int i = 0; i < string.length(); i++) {
            char b = string.charAt(i);
            if (b <= ' ') {
                if (started)
                    break;
            } else if (b >= '0' && b <= '9') {
                val = val * 10L + (b - '0');
                started = true;
            } else if (b == '-' && !started) {
                minus = true;
            } else
                break;
        }

        if (started)
            return minus ? (-val) : val;
        throw new NumberFormatException(string);
    }

}
