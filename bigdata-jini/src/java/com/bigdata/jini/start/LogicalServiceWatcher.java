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
 * Created on Jan 7, 2009
 */

package com.bigdata.jini.start;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.zookeeper.AbstractZNodeConditionWatcher;
import com.bigdata.zookeeper.ZooElection;

/**
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LogicalServiceWatcher extends AbstractZNodeConditionWatcher {

    /**
     * @param zookeeper
     * @param zpath
     */
    public LogicalServiceWatcher(ZooKeeper zookeeper, String zpath) {
        super(zookeeper, zpath);
        // TODO Auto-generated constructor stub
    }

    @Override
    protected void clearWatch() throws KeeperException, InterruptedException {
        // TODO Auto-generated method stub

    }

    @Override
    protected boolean isConditionSatisified(WatchedEvent event)
            throws KeeperException, InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    protected boolean isConditionSatisified() throws KeeperException,
            InterruptedException {
        // TODO Auto-generated method stub
        return false;
    }

    private void foo() throws KeeperException, InterruptedException {


        String zroot = null;
        ServiceConfiguration config = null;
        
        List<ACL> acl = null;
        new ZooElection<String>(zookeeper, zroot
                + "/election_create_" + config.className, acl) {
            /**
             * @todo this is a sketch of an election model.
             * 
             * Invoked for the winner. The election breaks when any
             * participant's timeout elapses or when the target #of
             * participants have joined. The first participant to "call" the
             * result decides who is the winner. A timestamp is written
             * by the winner into the data of the election and provides
             * a lease during which no other participant may call the
             * result.
             * 
             * writing the znode of the winner on the election we both
             * provide a lock which breaks race conditions (this indicates
             * that the election has already been called) and provide a 
             * signal to the participants watching for the election result.
             * All participants wake up on that signal, but only the one
             * whose znode was written and the children
             * which competed in that election are deleted.
             * 
             */
            protected void winner() {

            }
            public void awaitWinner(long timeout,TimeUnit unit) {
                throw new UnsupportedOperationException();
            }
        }.awaitWinner(5L, TimeUnit.SECONDS);

    }
    
    /**
     * @deprecated must be refactored for the {@link LogicalServiceWatcher}
     */
    public void xprocess(final WatchedEvent event) {

        JiniFederation fed = null;
        
        IServiceListener listener = null;
        
        if(INFO)
            log.info(event.toString());
        
        final String zpath = event.getPath();
        
        final ZooKeeper zookeeper = fed.getZookeeper();

        try {

            // get the service configuration (and reset our watch).
            final ServiceConfiguration config = (ServiceConfiguration) SerializerUtil
                    .deserialize(zookeeper.getData(zpath, this, new Stat()));

            /*
             * Verify that we could start this service.
             * 
             * @todo add load-based constraints, e.g., can not start a new
             * service if heavily swapping, near limits on RAM or on disk.
             */
            for (IServiceConstraint constraint : config.constraints) {

                if (!constraint.allow(fed)) {

                    if (INFO)
                        log.info("Constraint(s) do not allow service start: "
                                        + config);
                    
                    return;
                    
                }
                
            }
            
            /*
             * FIXME Examine priority queue to figure out which
             * ServicesManager is best suited to start the service instance.
             * Only ServiceManagers which satisify the constraints are
             * allowed to participate, so we need to make that an election
             * for the specific service, give everyone some time to process
             * the event, and then see who is the best candidate to start
             * the service.
             * 
             * @todo does this design violate zookeeper design principles?
             * Especially, we can't wait for asynchronous events so the
             * election will have to be on a created and the ephemeral
             * sequential joins with the election will have to wait for the
             * timeout or for the last to join (satisified the N vs timeout
             * criterion) to write the winner into the data.
             * 
             * Or, how about if we have different standing elections
             * corresponding to different constraints and have the hosts
             * update each election every 60 seconds with their current
             * priority (if it has changed significantly).  The problem
             * with this is that a host can't allow replicated instances
             * of a service onto the same host.
             */
            
            // get task to start the service.
            final Callable task = config.newServiceStarter(fed,
                    listener, zpath);
            
            /*
             * Submit the task.
             * 
             * The service will either start or fail to start. We don't
             * check the future since all watcher events are serialized.
             */
            fed.getExecutorService().submit(task)
            .get()// forces immediate start of the service.
            ;
            
        } catch (Throwable e) {

            log.error("zpath=" + zpath, e);

            throw new RuntimeException(e);

        }

    }

}
