package com.bigdata.service.jini;

import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

import com.sun.jini.admin.DestroyAdmin;

/**
 * Adds {@link #shutdown()} and {@link #shutdownNow()} methods that DO NOT
 * destroy the persistent state of the service (it may be restarted after
 * calling these methods) and extends {@link Remote} for RMI compatibility.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface RemoteDestroyAdmin extends Remote, DestroyAdmin {

    /**
     * Shutdown the service, but do not destroy its persistent data.
     */
    public void shutdown() throws IOException;

    /**
     * Immediate or fast shutdown for the service, but does not destroy its
     * persistent data.
     */
    public void shutdownNow() throws IOException;
 
    /**
     * <strong>This DESTROYS the persistent state of the service.</strong> YOU
     * CAN NOT restart the service after calling this method. All persistent
     * state (including any application data) WILL BE DESTROYED.
     */
    public void destroy() throws RemoteException;
    
}
