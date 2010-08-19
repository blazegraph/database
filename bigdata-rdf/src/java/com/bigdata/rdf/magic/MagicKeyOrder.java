package com.bigdata.rdf.magic;

import java.io.Serializable;
import java.util.Comparator;

import com.bigdata.rdf.internal.IV;
import com.bigdata.striterator.AbstractKeyOrder;
import com.bigdata.striterator.IKeyOrder;

public class MagicKeyOrder extends AbstractKeyOrder<IMagicTuple> implements IKeyOrder<IMagicTuple>, Serializable {

    /**
     * Generated serialization version.
     */
    private static final long serialVersionUID = 8429846038676605623L;

    private final String indexName;
    
    private final int[] keyMap;
    
    public MagicKeyOrder(final String indexName, final int[] keyMap) {
        
        this.indexName = indexName;
        
        this.keyMap = keyMap;
        
    }
    
    public Comparator<IMagicTuple> getComparator() {
        
        return new Comparator<IMagicTuple>() {

            public int compare(IMagicTuple o1, IMagicTuple o2) {
                
                if (o1 == o2) {
                    return 0;
                }
        
                // compare terms one by one in the appropriate key order
                for (int i = 0; i < keyMap.length; i++) {
                    IV t1 = o1.getTerm(keyMap[i]);
                    IV t2 = o2.getTerm(keyMap[i]);
                    int ret = t1.compareTo(t2);
                    if (ret != 0) {
                        return ret;
                    }
                }

                // all terms match
                return 0;
                
            }
            
        };
        
    }

    public String getIndexName() {
        
        return indexName;
        
    }
    
    public int[] getKeyMap() {
        
        return keyMap;
        
    }
    
    public int getKeyOrder(final int keyPos) {
        
        return keyMap[keyPos];
        
    }
    
    public int getKeyArity() {
        
        return keyMap.length;
        
    }
    
    public boolean isPrimary() {
        
        for (int i = 0; i < keyMap.length; i++) {
            if (i != keyMap[i]) {
                return false;
            }
        }
        return true;
        
    }
    
    public boolean canService(final int[] bound) {
        
        return MagicKeyOrderStrategy.match(bound, keyMap);
        
    }
    
}
