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
 * Created on Mar 26, 2008
 */

package com.bigdata.counters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.system.SystemUtil;

/**
 * Utility to return the PID of the JVM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PIDUtil {

    static protected final Logger log = Logger
            .getLogger(PIDUtil.class);

    static protected final boolean INFO = log.isInfoEnabled();
    
    /**
     * Return the PID of the Java VM under Linux using bash.
     * 
     * @return The PID.
     * 
     * @throws RuntimeException
     *             if anything goes wrong.
     */
    static public int getLinuxPIDWithBash() {

        final List<String> commands = new LinkedList<String>();

        final Process pr;
        
        try {
        
            commands.add("/bin/bash");
            
            commands.add("-c");
            
            commands.add("echo $PPID");
            
            ProcessBuilder pb = new ProcessBuilder(commands);
            
            pr = pb.start();
            
            pr.waitFor();
        
        } catch (Exception ex) {
        
            throw new RuntimeException("Problem running command: ["
                    + commands + "]", ex);
            
        }
        
        if (pr.exitValue() == 0) {
        
            BufferedReader outReader = new BufferedReader(
                    new InputStreamReader(pr.getInputStream()));
            
            final String val;
            try {
            
                val = outReader.readLine().trim();
                
            } catch (IOException ex) {
                
                throw new RuntimeException(ex);
                
            }
            
            if(INFO)
            log.info("read: [" + val + "]");
            
            int pid = Integer.parseInt(val);
            
            return pid;
            
        } else {
            
            throw new RuntimeException("Could not get PID: exitValue="
                    + pr.exitValue());
            
        }

    }

    /**
     * Return the JVM PID.
     * 
     * @throws UnsupportedOperationException
     *             if the pid can not be extracted.
     * 
     * @see RuntimeMXBean#getName(), A web search will show that this is
     *      generally of the form "pid@host". However this is definately NOT
     *      guarenteed by the javadoc.
     */
    static public int getPIDWithRuntimeMXBean() {

        final String name = ManagementFactory.getRuntimeMXBean().getName();

        final Matcher matcher = pidPattern.matcher(name);

        if (!matcher.find()) {

            throw new UnsupportedOperationException(
                    "Could not extract pid from [" + name + "]");

        }

        final int pid = Integer.parseInt(matcher.group(1));

        if(INFO)
            log.info("pid=" + pid);

        return pid;

    }

    static private final Pattern pidPattern = Pattern.compile("^([0-9]+)@");

    /**
     * Tries each of the methods in this class and returns the PID as reported
     * by the first method that succeeds. The order in which the methods are
     * tried SHOULD reflect the likelyhood that the method will get it right.
     * 
     * @return The PID of this JVM (best guess).
     */
    public static int getPID() {

            try {

            if (!SystemUtil.operatingSystem().toLowerCase().startsWith("win")) {

                return getLinuxPIDWithBash();
            }

        } catch (Throwable t) {

            log.warn(t);

        }
        
        return getPIDWithRuntimeMXBean();
        
    }
    
    /**
     * Utility for checking which method works on your platform.
     * 
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        System.out.println("pid=" + getPID());

        // gives you a chance to look at top, etc.
        Thread.sleep(5000);

    }

}
