package com.bigdata.btree.proc;

import com.bigdata.btree.raba.IRaba;
import com.bigdata.service.ndx.ClientIndexView;

/**
 * Interface for procedures that are mapped across one or more index partitions
 * based on an array of keys. The keys are interpreted as variable length
 * unsigned byte[]s and MUST be in sorted order. The {@link ClientIndexView}
 * will transparently break down the procedure into one procedure per index
 * partition based on the index partitions spanned by the keys.
 * <p>
 * Note: Implementations of this interface MUST declare an
 * {@link AbstractKeyArrayIndexProcedureConstructor} that will be used to create
 * the instances of the procedure mapped onto the index partitions.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public interface IKeyArrayIndexProcedure extends IIndexProcedure {

    /**
     * The #of keys/tuples
     * 
     * @deprecated by {@link #getKeys()}
     */
    public int getKeyCount();
    
    /**
     * Return the key at the given index.
     * 
     * @param i
     *            The index (origin zero).
     * 
     * @return The key at that index.
     * 
     * @deprecated by {@link #getKeys()}
     */
    public byte[] getKey(int i);
    
    /**
     * Return the value at the given index.
     * 
     * @param i
     *            The index (origin zero).
     * 
     * @return The value at that index.
     * 
     * @deprecated by {@link #getValues()}
     */
    public byte[] getValue(int i);

    /**
     * The keys.
     * 
     * @return The keys and never <code>null</code>.
     */
    public IRaba getKeys();

    /**
     * The values.
     * 
     * @return The values -or- <code>null</code> if no values were associated
     *         with the {@link IIndexProcedure}.
     */
    public IRaba getValues();

}
