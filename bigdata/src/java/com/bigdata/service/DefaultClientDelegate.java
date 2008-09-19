package com.bigdata.service;

import java.util.UUID;

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

    public DefaultClientDelegate(AbstractFederation fed) {

        if (fed == null)
            throw new IllegalArgumentException();

        this.fed = fed;

    }

    public UUID getServiceUUID() {

        return uuid;

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

}
