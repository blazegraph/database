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
package com.bigdata.journal.jini.ha;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.UUID;

import net.jini.config.Configuration;

import com.bigdata.ha.HAGlue;
import com.bigdata.ha.QuorumService;
import com.bigdata.journal.Journal;
import com.bigdata.quorum.Quorum;

/**
 * A {@link Journal} that that participates in a write replication pipeline. The
 * {@link HAJournal} is configured an River {@link Configuration}. The
 * configuration includes properties to configured the underlying
 * {@link Journal} and information about the {@link HAJournal}s that will
 * participate in the replication pattern.
 * <p>
 * All instances declared in the {@link Configuration} must be up, running, and
 * able to connect for any write operation to succeed. All instances must vote
 * to commit for an operation to commit. If any units fail (or timeout) then the
 * operation will abort. All instances are 100% synchronized at all commit
 * points. Read-only operations can be load balanced across the instances and
 * uncommitted data will never be visible to readers. Writes must be directed to
 * the first instance in the write replication pipeline. A read error on an
 * instance will internally failover to another instance in an attempt to read
 * from good data.
 * <p>
 * The write replication pipeline is statically configured. If an instance is
 * lost, then the configuration file must be changed, the change propagated to
 * all nodes, and the services "bounced" before writes can resume. Bouncing a
 * service only requires that the Journal is closed and reopened. Services do
 * not have to be "bounced" at the same time, and (possibly new) leader must be
 * "bounced" last to ensure that writes do not propagate until the write
 * pipeline is in a globally consistent order that excludes the down node.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * 
 *         TODO Make sure that ganglia reporting can be enabled.
 * 
 *         TODO Support dynamic reorganization of the write pipeline, including
 *         resynchronization as nodes enter and leave a quorum.
 */
public class HAJournal extends Journal {

//    private static final Logger log = Logger.getLogger(HAJournal.class);

    public interface Options extends Journal.Options {
        
        /**
         * The address at which this journal exposes its write pipeline
         * interface.
         */
        String WRITE_PIPELINE_ADDR = HAJournal.class.getName()
                + ".writePipelineAddr";

    }
    
    private final InetSocketAddress writePipelineAddr;
    
    public HAJournal(final Properties properties) {

        this(properties, null);
        
    }

    public HAJournal(final Properties properties,
            final Quorum<HAGlue, QuorumService<HAGlue>> quorum) {

        super(properties, quorum);

        /*
         * Note: We need this so pass it through to the HAGlue class below.
         * Otherwise this service does not know where to setup its write
         * replication pipeline listener.
         */
        
        writePipelineAddr = (InetSocketAddress) properties
                .get(Options.WRITE_PIPELINE_ADDR);

        if (writePipelineAddr == null)
            throw new RuntimeException(Options.WRITE_PIPELINE_ADDR
                    + " : required property not found.");

    }

    @Override
    protected HAGlue newHAGlue(final UUID serviceId) {

        return new HAGlueService(serviceId, writePipelineAddr);

    }

    protected class HAGlueService extends BasicHA {

        protected HAGlueService(final UUID serviceId,
                final InetSocketAddress writePipelineAddr) {

            super(serviceId, writePipelineAddr);

        }

    }

}
