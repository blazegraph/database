package com.bigdata.service;

import java.io.IOException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.ICounterSetAccess;
import com.bigdata.counters.httpd.CounterSetHTTPD;
import com.bigdata.util.httpd.AbstractHTTPD;

/**
 * Default {@link IFederationDelegate} implementation used by a standard client.
 * This may be extended or replaced using
 * {@link AbstractClient#setDelegate(IFederationDelegate)}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DefaultClientDelegate<T> implements IFederationDelegate<T> {

    private static final Logger log = Logger.getLogger(DefaultClientDelegate.class);
    
    private final UUID uuid = UUID.randomUUID();

    private final AbstractFederation<?> fed;

    private final T clientOrService;

    /**
     * 
     * @param fed
     *            The federation.
     * @param clientOrService
     *            The client or service connected to the federation. This MAY be
     *            <code>null</code> but then {@link #getService()} will also
     *            return <code>null</code>.
     */
    public DefaultClientDelegate(final AbstractFederation<?> fed,
            final T clientOrService) {

        if (fed == null)
            throw new IllegalArgumentException();

        if (clientOrService == null)
            log.warn("Client or service not specified: " + this);

        this.fed = fed;

        this.clientOrService = clientOrService;

    }

    public T getService() {

        return clientOrService;
        
    }
    
    /**
     * Returns a stable but randomly assigned {@link UUID}.
     */
    public UUID getServiceUUID() {

        return uuid;

    }

    /**
     * Return a stable identifier for this client based on the name of the
     * implementation class, the hostname, and the hash code of the client
     * (readable and likely to be unique, but uniqueness is not guaranteed).
     */
    public String getServiceName() {
        
        return fed.getClient().getClass().getName() + "@"
                + AbstractStatisticsCollector.fullyQualifiedHostName + "#"
                + fed.getClient().hashCode();
    
    }
    
    public Class getServiceIface() {

        return fed.getClient().getClass();

    }

    /**
     * NOP.
     */
    public void reattachDynamicCounters() {

    }

    /**
     * Returns <code>true</code>.
     */
    public boolean isServiceReady() {

        return true;

    }

    /**
     * NOP
     */
    public void didStart() {
        
    }

    /** NOP */
    public void serviceJoin(IService service, UUID serviceUUID) {

    }

    /** NOP */
    public void serviceLeave(UUID serviceUUID) {

    }

    public AbstractHTTPD newHttpd(final int httpdPort,
            final ICounterSetAccess access) throws IOException {
        
        return new CounterSetHTTPD(httpdPort, access);
//        {
//
//            public Response doGet(String uri, String method, Properties header,
//                    LinkedHashMap<String, Vector<String>> parms)
//                    throws Exception {
//
//                try {
//
//                    reattachDynamicCounters();
//
//                } catch (Exception ex) {
//
//                    /*
//                     * Typically this is because the live journal has been
//                     * concurrently closed during the request.
//                     */
//
//                    log.warn("Could not re-attach dynamic counters: " + ex, ex);
//
//                }
//
//                return super.doGet(uri, method, header, parms);
//
//            }
//
//        };

    }

}
