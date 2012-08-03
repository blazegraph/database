package com.bigdata.gom.om;

import org.openrdf.model.URI;

/**
 * The IIDGenerator interface is used to create default object URI
 * ids for new objects.  This can be exploited by various use cases where
 * a unique id can be conveniently managed by the system.
 * <p>
 * The interface allows applications to control the ID creation if required.
 * 
 * @author Martyn Cutcher
 *
 */
public interface IIDGenerator {
	
	URI genId();

	/**
	 * A rollback hook is required
	 */
	void rollback();

}
