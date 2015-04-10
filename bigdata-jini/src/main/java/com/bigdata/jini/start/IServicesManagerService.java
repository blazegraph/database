package com.bigdata.jini.start;

import java.io.IOException;
import java.rmi.Remote;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

/**
 * Methods exposed via RMI.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IServicesManagerService extends Remote {
 
    /**
     * Initiates a service configuration push and restarts any stopped
     * processes. This is the same behavior as SIGHUP. It is exposed for RMI
     * both for remote purposes and because some platforms (for example,
     * Windows) don't support SIGHUP.
     * 
     * @param pushConfig
     *            If you want to do a service configuration push.
     * @param restartServices
     *            If you want the services manager that receives the message to
     *            restart any services for which it is responsible which are not
     *            currently running.
     * 
     * @throws IOException
     *             if there is an RMI problem.
     * 
     * @throws ConfigurationException
     *             if there is a problem re-processing the {@link Configuration}.
     */
    public void sighup(boolean pushConfig, boolean restartServices)
            throws IOException, ConfigurationException;
    
}
