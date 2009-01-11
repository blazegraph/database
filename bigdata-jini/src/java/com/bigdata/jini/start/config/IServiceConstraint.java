package com.bigdata.jini.start.config;

import java.io.Serializable;

import com.bigdata.service.jini.JiniFederation;

/**
 * A constraint on where the service may be instantiated. For example, at most N
 * instances of a service on a host, only on hosts with a given IP address or
 * pattern, etc.
 * 
 * @todo add load-based constraints, e.g., can not start a new service if
 *       heavily swapping, near limits on RAM or on disk.
 */
public interface IServiceConstraint extends Serializable {

    /**
     * Return <code>true</code> iff a service may be instantiated on this
     * host.
     * <p>
     * Note: Constraints which can be evaluated without the federation reference
     * MUST NOT throw an exception if that reference is <code>null</code>.
     * This allows us to evaluate constraints for boostrap services as well as
     * for {@link ManagedServiceConfiguration}s
     * 
     * @param fed
     *            The federation.
     */
    public boolean allow(JiniFederation fed);

}
