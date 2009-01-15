package com.bigdata.jini.start.config;

import org.apache.zookeeper.ZooKeeper;

import com.bigdata.service.jini.JiniFederation;

/**
 * Constraint that zookeeper must be running (the {@link ZooKeeper} client
 * is {@link ZooKeeper.States#CONNECTED}).
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ZookeeperRunningConstraint extends
        ServiceDependencyConstraint {

    /**
     * 
     */
    private static final long serialVersionUID = -9179574081166981787L;

    public boolean allow(JiniFederation fed) throws Exception {

        final ZooKeeper.States state = fed.getZookeeper().getState();

        switch (state) {

        case CONNECTED:
            return true;

        default:
            if (ServiceDependencyConstraint.INFO)
                ServiceDependencyConstraint.log.info("Zookeeper not connected: state=" + state);

            return false;

        }

    }

}