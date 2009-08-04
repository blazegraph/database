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
 * Created on Feb 19, 2009
 */

package com.bigdata.jini.util;

import java.io.File;
import java.util.concurrent.TimeUnit;

import net.jini.config.Configuration;

/**
 * A utility class to help with {@link Configuration}s.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ConfigMath {

    public static int add(int a, int b) {
        return a + b;
    }

    public static long add(long a, long b) {
        return a + b;
    }
    
    public static double add(double a, double b) {
        return a + b;
    }

    public static int multiply(int a, int b) {
        return a * b;
    }

    public static long multiply(long a, long b) {
        return a * b;
    }

    public static double multiply(double a, double b) {
        return a * b;
    }

    /**
     * Useful for enums which can't be handled otherwise.
     * 
     * @param o
     * 
     * @return
     */
    public static String toString(Object o) {
        
        return o.toString();
        
    }
    
    /**
     * Convert seconds to nanoseconds.
     * 
     * @param s
     *            seconds.
     *            
     * @return Nanoseconds.
     */
    public static long s2ns(final long s) {

        return TimeUnit.SECONDS.toNanos(s);

    }

    /**
     * Convert milliseconds to nanoseconds.
     * 
     * @param ms
     *            Milliseconds.
     *            
     * @return Nanoseconds.
     */
    public static long ms2ns(final long ms) {

        return TimeUnit.MILLISECONDS.toNanos(ms);

    }

    /**
     * Convert seconds to milliseconds.
     * 
     * @param s
     *            seconds.
     *            
     * @return Milliseconds.
     */
    public static long s2ms(final long s) {

        return TimeUnit.SECONDS.toMillis(s);

    }

    /**
     * Convert minutes to milliseconds.
     * 
     * @param m
     *            minutes.
     *            
     * @return Milliseconds.
     */
    public static long m2ms(final long m) {

        return TimeUnit.MINUTES.toMillis(m);

    }

    /**
     * Return the absolute path for the file.
     * 
     * @param file
     *            The file.
     *            
     * @return The absolute path for that file.
     */
    public static String getAbsolutePath(final File file) {
    
        return file.getAbsolutePath();
        
    }

    /**
     * Return the absolute file for the file.
     * 
     * @param file
     *            The file.
     *            
     * @return The absolute file for that file.
     */
    public static File getAbsoluteFile(final File file) {
        
        return file.getAbsoluteFile();
        
    }
    
    /**
     * Convert a file into an absolute URI and return its representation.
     * 
     * @param file
     *            The file.
     *            
     * @return The respresentation of the corresponding absolute URI.
     */
    public static String getURIString(final File file) {
        
        return file.getAbsoluteFile().toURI().toString();
        
    }

}
