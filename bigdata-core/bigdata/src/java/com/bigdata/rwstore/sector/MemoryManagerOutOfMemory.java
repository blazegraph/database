package com.bigdata.rwstore.sector;

/**
 * Thrown if there are not sufficient resources available to satisfy a blocking
 * request against an {@link IMemoryManager}
 * 
 * @author thompsonbry
 */
public class MemoryManagerOutOfMemory extends MemoryManagerResourceError {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MemoryManagerOutOfMemory() {
	}

}
