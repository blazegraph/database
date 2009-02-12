package com.bigdata.service;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import com.bigdata.counters.AbstractStatisticsCollector;
import com.bigdata.counters.CounterSet;
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
public class DefaultClientDelegate implements IFederationDelegate {

    private final UUID uuid = UUID.randomUUID();

    private final AbstractFederation fed;

    public DefaultClientDelegate(final AbstractFederation fed) {

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;

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
     * (readable and likely to be unique, but uniqueness is not guarenteed).
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
            final CounterSet counterSet) throws IOException {
        
        return new CounterSetHTTPD(httpdPort, counterSet) {

            public Response doGet(String uri, String method, Properties header,
                    LinkedHashMap<String, Vector<String>> parms)
                    throws Exception {

                try {

                    reattachDynamicCounters();

                } catch (Exception ex) {

                    /*
                     * Typically this is because the live journal has been
                     * concurrently closed during the request.
                     */

                    log.warn("Could not re-attach dynamic counters: " + ex, ex);

                }

                return super.doGet(uri, method, header, parms);

            }

        };

    }

}
