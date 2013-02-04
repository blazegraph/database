package com.bigdata.rawstore;

import java.io.OutputStream;

/**
 * This class provides a stream-based allocation mechanism.
 * <p>
 * The intention is that an IPSOutputStream (PS for PerSistent) is
 * returned by a storage service to which data can be written.
 * When all data has been written the address from which to retriev
 * an InputStream can be requested.
 * 
 * @author Martyn Cutcher
 *
 */
public abstract class IPSOutputStream extends OutputStream {

	/**
	 * Called when writes to the stream are complete.
	 * @return the address from which an InputStream can be retrieved.
	 */
	public abstract long getAddr();
}
