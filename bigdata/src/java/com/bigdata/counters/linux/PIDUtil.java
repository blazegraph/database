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

package com.bigdata.counters.linux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Utility to return the PID of the JVM.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class PIDUtil {

    static protected final Logger log = Logger
            .getLogger(PIDUtil.class);

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
            
            log.info("read: [" + val + "]");
            
            int pid = Integer.parseInt(val);
            
            return pid;
            
        } else {
            
            throw new RuntimeException("Could not get PID: exitValue="
                    + pr.exitValue());
            
        }

    }

//  /**
//  * Return the JVM PID.
//  * 
//  * @throws UnsupportedOperationException
//  *             if the pid can not be extracted.
//  * 
//  * @see RuntimeMXBean#getName(), A web search will show that this is
//  *      generally of the form "pid@host".  However this is definately
//  *      NOT guarenteed by the javadoc.
//  */
// public int getPID() {
//     
//     String name = ManagementFactory.getRuntimeMXBean().getName();
//     
//     Matcher matcher = pidPattern.matcher(name);
//     
//     if(!matcher.matches()) {
//         
//         throw new UnsupportedOperationException("Could not extract pid from ["+name+"]");
//         
//     }
//     
//     final int pid = Integer.parseInt(matcher.group(1));
//     
//     log.info("pid="+pid);
//     
//     return pid;
//     
// }
// private final Pattern pidPattern = Pattern.compile("^([0-9]+)@");

}
