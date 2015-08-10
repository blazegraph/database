package com.bigdata.rdf.spo;

public enum ModifiedEnum {

    INSERTED, REMOVED, UPDATED, NONE;
    
    public static boolean[] toBooleans(final ModifiedEnum[] modified, final int n) {
        
        final boolean[] b = new boolean[n*2];
        for (int i = 0; i < n; i++) {
            switch(modified[i]) {
            case INSERTED:
                b[i*2] = true;
                b[i*2+1] = false;
                break;
            case REMOVED:
                b[i*2] = false;
                b[i*2+1] = true;
                break;
            case UPDATED:
                b[i*2] = true;
                b[i*2+1] = true;
                break;
            case NONE:
            default:
                b[i*2] = false;
                b[i*2+1] = false;
                break;
            }
        }
        
        return b;
        
    }
    
    public static ModifiedEnum[] fromBooleans(final boolean[] b, final int n) {
        
        assert n <= b.length && n % 2 == 0 : "n="+n+", b.length="+b.length;
        
        final ModifiedEnum[] m = new ModifiedEnum[n/2];
        for (int i = 0; i < n; i+=2) {
            if (b[i] && !b[i+1])
                m[i/2] = INSERTED;
            else if (!b[i] && b[i+1])
                m[i/2] = REMOVED;
            else if (b[i] && b[i+1])
                m[i/2] = UPDATED;
            else
                m[i/2] = NONE;
        }
        
        return m;
        
    }
    
}
