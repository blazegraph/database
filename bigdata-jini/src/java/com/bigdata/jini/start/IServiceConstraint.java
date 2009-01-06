package com.bigdata.jini.start;

import java.io.Serializable;

import com.bigdata.service.jini.JiniFederation;

/**
 * A constraint on where the service may be instantiated. For example, at most N
 * instances of a service on a host, only on hosts with a given IP address or
 * pattern, etc.
 */
public interface IServiceConstraint extends Serializable {

    /**
     * Return <code>true</code> iff a service may be instantiated on this
     * host.
     */
    public boolean allow(JiniFederation fed);

}