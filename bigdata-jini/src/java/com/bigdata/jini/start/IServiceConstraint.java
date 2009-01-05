package com.bigdata.jini.start;

import com.bigdata.service.jini.JiniFederation;

/**
 * A constraint on where the service may be instantiated. For example, at
 * most N instances of a service on a host, only on hosts with a given IP
 * address pattern, etc.
 */
public interface IServiceConstraint {
    
    /**
     * Return <code>true</code> iff a service may be instantiated on thiss
     * host.
     */
    public boolean allow(JiniFederation fed);
    
}