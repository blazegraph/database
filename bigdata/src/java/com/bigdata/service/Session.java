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
 * Created on Nov 19, 2008
 */

package com.bigdata.service;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * A (transient) property set associated with some kinds of services.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class Session {

    protected static final Logger log = Logger.getLogger(Session.class);

    private ConcurrentHashMap<String, Object> session = new ConcurrentHashMap<String, Object>();

    public Object get(String name) {

        return session.get(name);

    }

    public Object putIfAbsent(String name, Object value) {

        final Object ret = session.putIfAbsent(name, value);

        if (log.isInfoEnabled())
            log.info("name=" + name + ", size=" + session.size());

        return ret;

    }

    public Object put(String name, Object value) {

        final Object ret = session.put(name, value);

        if (log.isInfoEnabled())
            log.info("name=" + name + ", size=" + session.size());

        return ret;

    }

    public Object remove(String name) {

        final Object ret = session.remove(name);

        if (log.isInfoEnabled())
            log.info("name=" + name + ", size=" + session.size() + ", removed="
                    + (ret != null));

        return ret;

    }

    public Object remove(String name, Object expectedValue) {

        final Object ret = session.remove(name, expectedValue);

        if (log.isInfoEnabled())
            log.info("name=" + name + ", size=" + session.size() + ", removed="
                    + (ret != null));

        return ret;

    }

}
