/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
 * Created on Oct 31, 2012
 */
package com.bigdata.journal.jini.ha;

import java.io.IOException;
import java.rmi.Remote;
import java.util.UUID;

import org.apache.log4j.Logger;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationException;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.quorum.Quorum;

/**
 * Class extends {@link HAJournal}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class MyHAJournal extends HAJournal {

    private static final Logger log = Logger.getLogger(HAJournal.class);

    public MyHAJournal(final HAJournalServer server,
            final Configuration config,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum)
            throws ConfigurationException, IOException {

        super(server, config, quorum);

    }

    @Override
    protected HAGlue newHAGlue(final UUID serviceId) {

//        return super.newHAGlue(serviceId);
        return new MyHAGlueService(serviceId);
      
    }
    
    /**
     * A {@link Remote} interface for new methods published by the service.
     */
    public static interface MyHAGlue extends HAGlue {

        public void helloWorld() throws IOException;
        
    }
    
    /**
     * Class extends the public RMI interface of the {@link HAJournal}.
     * <p>
     * Note: Any new RMI methods must be (a) declared on an interface; and (b)
     * must throw {@link IOException}.
     */
    protected class MyHAGlueService extends HAJournal.HAGlueService implements
            MyHAGlue {

        protected MyHAGlueService(final UUID serviceId) {

            super(serviceId);

        }

        @Override
        public void helloWorld() throws IOException {

            log.warn("Hello world!");

        }
        
    }

}
