package com.bigdata.service.jini;

import org.apache.log4j.Logger;

import com.sun.jini.start.LifeCycle;

/**
 * A NOP implementation used when the service is not started by activation
 * (eg, when the service is run from a command line).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public final class FakeLifeCycle implements LifeCycle {

	protected final static transient Logger log = Logger
			.getLogger(FakeLifeCycle.class);

	public FakeLifeCycle() {

	}

	public boolean unregister(final Object arg0) {

		if (log.isInfoEnabled())
			log.info("");

		return true;

	}

}
