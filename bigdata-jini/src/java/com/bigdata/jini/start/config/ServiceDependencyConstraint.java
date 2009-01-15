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
 * Created on Jan 15, 2009
 */

package com.bigdata.jini.start.config;

import org.apache.log4j.Logger;

/**
 * Constraint that another a service must be running before this one can be
 * started.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
abstract public class ServiceDependencyConstraint implements IServiceConstraint {

    protected static final Logger log = Logger
            .getLogger(ServiceDependencyConstraint.class);

    protected static final boolean INFO = log.isInfoEnabled();

    /*
     * Note: You can't access these constants from a Jini configuration.
     */
    
//    /**
//     * JINI must be running.
//     */
//    public static transient final IServiceConstraint JINI = new JiniRunningConstraint();
//
//    /**
//     * Zookeeper must be running.
//     */
//    public static transient final IServiceConstraint ZOOKEEPER = new JiniRunningConstraint();
//
//    /**
//     * The {@link TransactionServer} must be running - note this service also
//     * provides the {@link ITimestampService} implementation.
//     */
//    public static transient final IServiceConstraint TX = new TXRunningConstraint();
//
//    /**
//     * The {@link MetadataServer} must be running.
//     */
//    public static transient final IServiceConstraint MDS = new JiniRunningConstraint();

}
