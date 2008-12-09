package com.bigdata.counters.linux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

/**
 * Reports on the kernel version for a linux host.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan
 *         Thompson</a>
 * @version $Id$
 * 
 * @todo there is more information lurking in the kernel version.
 * 
 * @see http://en.wikipedia.org/wiki/Linux_kernel for a summary of linux
 *      kernel versioning.
 */
public class KernelVersion {
    
    static protected final Logger log = Logger.getLogger(KernelVersion.class);

    public final int version;
    public final int major;
    public final int minor;
    
    public KernelVersion(final String val) {
        
        final String[] x = val.split("[\\.]");
        
        assert x.length >= 2 : "Not a kernel version? "+val;
        
        version = Integer.parseInt(x[0]);
        major   = Integer.parseInt(x[1]);
        minor   = Integer.parseInt(x[2]);
        
    }
    
    /**
     * Return the version of the Linux kernel as reported by
     * <code>uname</code>
     * 
     * @return The {@link KernelVersion}
     * 
     * @throws RuntimeException
     *             if anything goes wrong.
     */
    static public KernelVersion get() {

        final List<String> commands = new LinkedList<String>();

        final Process pr;
        
        try {
        
            commands.add("/bin/uname");
            
            commands.add("-r");
            
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

            return new KernelVersion(val);
            
        } else {
            
            throw new RuntimeException("Could not get PID: exitValue="
                    + pr.exitValue());
            
        }

    }

}