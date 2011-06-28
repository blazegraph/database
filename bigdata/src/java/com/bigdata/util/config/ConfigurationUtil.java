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

package com.bigdata.util.config;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.jini.config.ConfigUtil;
import net.jini.url.httpmd.HttpmdUtil;

import java.io.IOException;

/**
 * Utility class that provides a set of static convenience methods
 * related to configuration and deployment of the Bigdata services.
 * Although useful in general, the methods in this utility class may be
 * particularly useful when employed from within a Jini configuration
 * file.
 * <p>
 * This class cannot be instantiated.
 */
public class ConfigurationUtil {

    private static final Logger logger = 
        LogUtil.getLog4jLogger( ConfigurationUtil.class );

    /**
     * Concatenates the given <code>String</code> arrays, returning
     * the result in a new <code>String</code> array.
     */
    public static String[] concat(String[] a, String[] b) {
        String[] result = new String[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    /** 
     * Creates and returns a <code>String</code> array consisting of the
     * contents of the given <code>prevArgList</code> and the arguments
     * reference in the given <code>argString</code>. 
     * <p>
     * Note that the contents of the <code>argString</code> parameter is
     * <code>split</code> on the RS (<i>record separator</i>) control
     * character ("\036").
     */
    public static String[] createArgList(String[] prevArgList,
                                         String   argString)
    {
        if(argString.matches("^[ \t]*$")) return prevArgList;

        String[] newArgs = argString.split("\036");
        return concat(prevArgList, newArgs);
    }

    /**
     * Using the given parameters in the appropriate manner, this method
     * constructs and returns a <code>String</code> whose value is a valid
     * Java RMI <i>codebase</i> specification. This method returns a
     * codebase supporting one of two possible protocols: the standard
     * <i>http</i> protocol, or the Jini <i>httpmd</i> protocol; which
     * was created to support codebase integrity when running a remote
     * service configured for secure code downloading. 
     * <p>
     * The <code>mdAlgorithm</code> parameter is used to indicate to
     * this method which type of codebase should be constructed and
     * returned. If <code>null</code> is input to <code>mdAlgorithm</code>,
     * or if it has one of the values "none", "off", or "" (the empty
     * <code>String</code>), then this method will construct and returned
     * a standard HTTP-based codebase; otherwise, an HTTPMD-based codebase
     * will constructed and returned.
     * <p>
     * Note that the <code>name</code> parameter is handled specially by
     * this method. The value input to that <code>String</code> parameter
     * is interpretted in one of two ways by this method:
     * <p><ul>
     *   <li> The name of the local <i>network interface</i> over which the
     *        class server will communicate with clients requesting the
     *        associated service's downloadable classes through the codebase
     *        returned by this method.
     *   <li> The local <i>or remote</i> IP address or name of the <i>host</i>
     *        on which the class server is running and serving the 
     *        associated service's classes.
     * </ul></p>
     * 
     * This method first treats the value of the <code>name</code> parameter
     * as a network interface, attempting to determine that interface's
     * corresponding IP address. Should that attempt fail, then this method
     * then assumes that the caller intended this parameter to be interpretted
     * as an IP address or host name; in which case, this method uses
     * the value of the parameter as the <i>address/host</i> component of
     * codebase being constructed.
     * <p>
     * This method is intended to provide a convenient mechanism for
     * constructing a service's codebase from within a Jini configuration
     * file. To support both development and deployment scenarios, it
     * is important to be able to flexibly construct such codebases from
     * either a local network interface specification or from a
     * specification that represents the name of a remote host running
     * a shared codebase class server on the appropriate network.
     *
     * @param name        <code>String</code> referencing either a network
     *                    interface, IP address, or host name, each of which
     *                    will be used either to determine the appropriate
     *                    value, or as the actual value, for the 
     *                    <i>address/host</i> component of the returned 
     *                    codebase.
     *
     * @param jarFile     <code>String</code> value that is used for the 
     *                    <i>jar</i> component of the returned codebase;
     *                    which references the JAR file containing the
     *                    downloadable classes that make up the desired 
     *                    service codebase.
     *
     * @param port        <code>int</code> value that is used for the 
     *                    <i>port</i> component of the returned codebase;
     *                    which is the port on which the class server will
     *                    listen for download requests from clients of the
     *                    associated service.
     *
     * @param srcRoot     <code>String</code> value that is used when
     *                    computing an HTTPMD-based codebase. The value
     *                    of this parameter references the root directory
     *                    path (or URL of the directory) containing the JAR
     *                    file(s) (or class file(s)) containing the
     *                    downloadable classes that make up the desired
     *                    service codebase. This parameter is typically the
     *                    root directory from which the class server will
     *                    serve the service's <i>downloadable JAR files</i>.
     *                    Note that this parameter is not used in the
     *                    construction of the returned codebase if the
     *                    <code>mdAlgorithm</code> parameter is 
     *                    <code>null</code> or if it has one of the values
     *                    "none", "off", or "" (the empty <code>String</code>).
     *
     * @param mdAlgorithm <code>String</code> value indicating the 
     *                    message digest algorithm to use when constructing
     *                    an HTTPMD-based codebase (for example, "sha",
     *                    "md5", etc.). Note that if this parameter is
     *                    <code>null</code>, then this method will 
     *                    contruct an HTTP-based codebase.
     *
     * @return a <code>String</code> whose value is a valid Java RMI
     *         <i>codebase</i> specification represented in either 
     *         standard <i>http</i> protocol format, or Jini <i>httpmd</i>
     *         protocol format.
     *
     * @throws IOException if an I/O exception occurs while reading data
     *         from the <code>srcRoot</code>.
     *
     * @throws NullPointerException if <code>null</code> is input for
     *         <code>name</code> or <code>jarFile</code>, and/or
     *         <code>null</code> is input for <code>srcRoot</code>
     *         when <code>mdAlgorithm</code> is non-<code>null</code>
     *         and is not equal to "none", "off", or "" (note that if 
     *         <code>mdAlgorithm</code> is <code>null</code> or equal to
     *         "none", "off", or "", then <code>srcRoot</code> can take
     *         any value, including <code>null</code>).
     *
     * @throws IllegalArgumentException if the value input for 
     *         <code>port</code> is negtive.
     */
    public static String computeCodebase(String  name,
                                         String  jarFile,
                                         int     port,
                                         String  srcRoot,
                                         String  mdAlgorithm)
                                                         throws IOException
    {
        if(name == null) throw new NullPointerException("name cannot be null");
        if(jarFile == null) throw new NullPointerException
                                                 ("jarFile cannot be null");
        if(port < 0) throw new IllegalArgumentException
                                                 ("port cannot be negative");
        boolean doHttpmd = true;
        if(    (mdAlgorithm == null) 
            || (("").equals(mdAlgorithm))
            || (("off").equals(mdAlgorithm))
            || (("none").equals(mdAlgorithm)) )
        {
            doHttpmd = false;
        }
        if( doHttpmd && (srcRoot == null) ) {
            throw new NullPointerException
                       ("srcRoot cannot be null when constructing "
                        +"an HTTPMD codebase");
        }

        String codebase = null;
        String ipAddr   = name;

        // Determine if name is a Nic name or a host name 
        try {
            ipAddr = NicUtil.getIpAddress(name);
        } catch(Exception e) {
            // must be a hostname  
            logger.log(Level.TRACE, name+" - not a valid "
                           +"network interface, assuming host name");
        }

        // Construct the codebase, either httpmd or http 
        if(doHttpmd) {
            String httpmdUrl = 
               "httpmd://"+ipAddr+":"+port+"/"+jarFile+";"+mdAlgorithm+"=0";
            codebase = HttpmdUtil.computeDigestCodebase(srcRoot, httpmdUrl);;
        } else {//use httpmd
            codebase = "http://"+ipAddr+":"+port+"/"+jarFile;
        }
        logger.log(Level.TRACE, "codebase = "+codebase);

        return codebase;
    }

    /**
     * Convenience method that ultimately calls the primary 5-argument
     * version of this method. This method allows one to input a
     * <code>String</code> value for the port to use when constructing
     * the codebase; which can be useful when invoking this method
     * from a Jini configuration file.
     *
     * @throws NumberFormatException if the value input for <code>port</code>
     *         does not contain a parsable <code>int</code>.
     */
    public static String computeCodebase(String  name,
                                         String  jarFile,
                                         String  port,
                                         String  srcRoot,
                                         String  mdAlgorithm)
                                                         throws IOException
    {
        return computeCodebase(name, jarFile, Integer.parseInt(port),
                               srcRoot, mdAlgorithm);
    }

    /**
     * Convenient three-argument version of <code>computeCodebase</code>
     * that will always return a codebase <code>String</code> represented
     * in standard <i>http</i> protocol format.
     */
    public static String computeCodebase(String  name,
                                         String  jarFile,
                                         int     port) throws IOException
    {
        return computeCodebase(name, jarFile, port, null, null);
    }

    /**
     * Convenient three-argument version of <code>computeCodebase</code>
     * that takes a <code>String</code> value for the <code>port</code>
     * parameter, always return a codebase <code>String</code> represented
     * in standard <i>http</i> protocol format.
     *
     * @throws NumberFormatException if the value input for <code>port</code>
     *         does not contain a parsable <code>int</code>.
     */
    public static String computeCodebase(String  name,
                                         String  jarFile,
                                         String  port) throws IOException
    {
        return computeCodebase(name, jarFile, Integer.parseInt(port));
    }
}
