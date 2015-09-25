package com.bigdata.service.zookeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

import com.bigdata.service.AbstractDistributedFederation;
import com.bigdata.service.IBigdataClient;
import com.bigdata.zookeeper.ZooKeeperAccessor;
import com.bigdata.zookeeper.start.BigdataZooDefs;
import com.bigdata.zookeeper.start.config.ZookeeperClientConfig;

public abstract class ZookeeperFederation<T> extends
		AbstractDistributedFederation<T> {

	protected final ZooKeeperAccessor zooKeeperAccessor;
	protected final ZookeeperClientConfig zooConfig;

	public ZookeeperFederation(IBigdataClient client) {
		super(client);
		zooConfig = null;
		zooKeeperAccessor = null;
	}		

	public ZookeeperFederation(IBigdataClient client,
			ZookeeperClientConfig zooConfig) {

		super(client);

		/*
		 * Connect to a zookeeper service in the declare ensemble of zookeeper
		 * servers.
		 * 
		 * @todo if the zookeeper integration is to be optional then we can not
		 * insist on the zooconfig an this section needs to be conditional on
		 * whether or not zookeeper was configured.
		 */

		this.zooConfig = zooConfig;

		try {
			zooKeeperAccessor = new ZooKeeperAccessor(zooConfig.servers,
					zooConfig.sessionTimeout);
		} catch (Exception ex) {

			log.fatal("Problem ZooKeeperAccessor:  " + ex.getMessage(), ex);

			try {

				shutdownNow();

			} catch (Throwable t) {

				log.error(t.getMessage(), t);

			}

			throw new RuntimeException(ex);

		}
	}

	/**
	 * Return the zookeeper client configuration.
	 */
	public ZookeeperClientConfig getZooConfig() {
	    
	    return zooConfig;
	    
	}

	/**
	 * Return an object that may be used to obtain a {@link ZooKeeper} client
	 * and that may be used to obtain the a new {@link ZooKeeper} client if the
	 * current session has been expired (an absorbing state for the
	 * {@link ZooKeeper} client).
	 */
	public ZooKeeperAccessor getZookeeperAccessor() {
	
	    return zooKeeperAccessor;
	    
	}

	/**
	 * Return a {@link ZooKeeper} client.
	 * <p>
	 * Note: This is a shorthand for obtaining a valid {@link ZooKeeper} client
	 * from the {@link ZooKeeperAccessor}. If the session associated with the
	 * current {@link ZooKeeper} client is expired, then a distinct
	 * {@link ZooKeeper} client associated with a distinct session will be
	 * returned. See {@link #getZookeeperAccessor()} which lets you explicitly
	 * handle a {@link SessionExpiredException} or the {@link ZooKeeper}
	 * {@link ZooKeeper.States#CLOSED} state.
	 * 
	 * @see #getZookeeperAccessor()
	 * 
	 * @todo timeout variant w/ unit?
	 */
	public ZooKeeper getZookeeper() {
	
	    try {
	
	        return zooKeeperAccessor.getZookeeper();
	        
	    } catch (InterruptedException ex) {
	        
	        throw new RuntimeException(ex);
	        
	    }
	    
	}

	/**
	 * Create key znodes used by the federation.
	 * 
	 * @throws KeeperException
	 * @throws InterruptedException
	 * 
	 * @todo probably better written using a timeout than the caller's zk inst.
	 */
	public void createKeyZNodes(final ZooKeeper zookeeper) throws KeeperException,
			InterruptedException {
			
			    final String zroot = zooConfig.zroot;
			    final List<ACL> acl = zooConfig.acl;
			    
			    final String[] a = new String[] {
			
			            // znode for the federation root.
			            zroot,
			
			            // znode for configuration metadata.
			            zroot + "/" + BigdataZooDefs.CONFIG,
			
			            // znode dominating most locks.
			            zroot + "/" + BigdataZooDefs.LOCKS,
			
			            // znode dominating lock nodes for creating new physical services.
			            zroot + "/" + BigdataZooDefs.LOCKS_CREATE_PHYSICAL_SERVICE,
			
			            // znode whose children are the per-service type service configurations.
			            zroot + "/" + BigdataZooDefs.LOCKS_SERVICE_CONFIG_MONITOR,
			
			            // znode for the resource locks (IResourceLockManager)
			            zroot + "/" + BigdataZooDefs.LOCKS_RESOURCES,
			
			    };
			
			    for (String zpath : a) {
			
			        try {
			
			            zookeeper.create(zpath, new byte[] {}/* data */, acl,
			                    CreateMode.PERSISTENT);
			
			        } catch (NodeExistsException ex) {
			
			            // that's fine - the configuration already exists.
			            if (log.isDebugEnabled())
			                log.debug("exists: " + zpath);
			
			            return;
			
			        }
			
			    }
			
			}

}