package com.bigdata.service.jini;

import java.rmi.Remote;

import net.jini.admin.Administrable;

/**
 * Extends {@link Remote} for RMI compatibility.
 * 
 * @todo There are other jini interfaces that we do not yet implement. Note: You
 *       need to extend Remote in order for these APIs to be exported!
 * 
 * <pre>
 * public interface RemoteJoinAdmin extends Remote, JoinAdmin {
 * 
 * }
 * 
 * public interface RemoteDiscoveryAdmin extends Remote, DiscoveryAdmin {
 * 
 * }
 * 
 * public interface RemoteStorageLocationAdmin extends Remote,
 *         StorageLocationAdmin {
 * 
 * }
 * </pre>
 */
public interface RemoteAdministrable extends Remote, Administrable {

}