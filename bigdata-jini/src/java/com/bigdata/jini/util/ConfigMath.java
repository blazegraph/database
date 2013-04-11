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

import com.sun.jini.config.ConfigUtil;

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

    public static int divide(int a, int b) {
        return a / b;
    }

    public static long divide(long a, long b) {
        return a / b;
    }

    public static double divide(double a, double b) {
        return a / b;
    }

    /**
     * Useful for enums which can't be handled otherwise.
     * 
     * @param o
     * 
     * @return
     */
    public static String toString(final Object o) {
        
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
     * Convert days to milliseconds.
     * 
     * @param d
     *            days
     *            
     * @return Milliseconds.
     */
    public static long d2ms(final long m) {

        return TimeUnit.DAYS.toMillis(m);

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
     * @return The representation of the corresponding absolute URI.
     */
    public static String getURIString(final File file) {
        
        return file.getAbsoluteFile().toURI().toString();
        
    }

    /**
     * Quote a string value.
     * 
     * @param v
     *            The value.
     * 
     * @return The quoted value.
     * 
     * @todo Use {@link ConfigUtil#stringLiteral(String)} instead?
     */
    static public String q(final String v) {
        
        final int len = v.length();
        
        final StringBuilder sb = new StringBuilder(len + 10);
        
        sb.append("\"");
        
        for(int i=0; i<len; i++) {
            
            char c = v.charAt(i);
            
            switch(c) {
            
            case '\\':
                sb.append("\\\\");
                break;
    
            default:
                sb.append(c);
                
            }
            
        }
        
        sb.append("\"");
        
        return sb.toString(); 
        
    }

    /**
     * Combines the two arrays, appending the contents of the 2nd array to the
     * contents of the first array.
     * 
     * @param a
     * @param b
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T> T[] concat(final T[] a, final T[] b) {
    
        if (a == null && b == null)
            return a;
    
        if (a == null)
            return b;
    
        if (b == null)
            return a;
    
        final T[] c = (T[]) java.lang.reflect.Array.newInstance(a.getClass()
                .getComponentType(), a.length + b.length);
    
        // final String[] c = new String[a.length + b.length];
    
        System.arraycopy(a, 0, c, 0, a.length);
    
        System.arraycopy(b, 0, c, a.length, b.length);
    
        return c;
    
    }

    /**
     * Trinary logic operator (if-then-else).
     * 
     * @param condition
     *            The boolean condition.
     * @param ifTrue
     *            The result if the condition is <code>true</code>.
     * @param ifFalse
     *            The result if the condition is <code>false</code>.
     *            
     * @return The appropriate argument depending on whether the
     *         <i>condition</i> is <code>true</code> or <code>false</code>.
     */
    public static <T> T trinary(final boolean condition, final T ifTrue,
            final T ifFalse) {
        
        if(condition) {
            
            return ifTrue;
            
        }
        
        return ifFalse;
        
    }

    /**
     * Return <code>true</code> iff the argument is <code>null</code>.
     * 
     * @param o
     *            The argument.
     */
    public static boolean isNull(final Object o) {
        
        return o == null;
        
    }

    /**
     * Return <code>true</code> iff the argument is not <code>null</code>.
     * 
     * @param o
     *            The argument.
     */
    public static boolean isNotNull(final Object o) {

//        ConfigMath.trinary(ConfigMath.isNull(bigdata.service)
//                , new Comment("Auto-generated ServiceID")
//                , new ServiceUUID( bigdata.serviceId )
//             );

        return o != null;
        
    }
    
}
