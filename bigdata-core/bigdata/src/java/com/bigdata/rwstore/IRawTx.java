package com.bigdata.rwstore;

/**
 * A hook used to support session protection by incrementing and decrementing a
 * transaction counter within an {@link IStore}. As long as a transaction is
 * active we can not release data which is currently marked as freed but was
 * committed at the point the session started.
 * 
 * @author <a href="mailto:martyncutcher@users.sourceforge.net">Martyn
 *         Cutcher</a>
 */
public interface IRawTx {
    
    /**
     * Close the transaction.
     */
	public void close();
}
