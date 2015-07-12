package com.bigdata.blueprints;


public class BigdataGraphEdit { 
    
    public static enum Action {
        
        Add, Remove;
        
    }

    private final Action action;

    private final BigdataGraphAtom atom;
    
    private final long timestamp;
    
    public BigdataGraphEdit(final Action action, final BigdataGraphAtom atom) {
        this(action, atom, 0l);
    }

    public BigdataGraphEdit(final Action action, final BigdataGraphAtom atom, 
            final long timestamp) {
        this.action = action;
        this.atom = atom;
        this.timestamp = timestamp;
    }

    public Action getAction() {
        return action;
    }

    public BigdataGraphAtom getAtom() {
        return atom;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public String getId() {
        return atom.getId();
    }

    @Override
    public String toString() {
        return "BigdataGraphEdit [action=" + action + ", atom=" + atom
                + ", timestamp=" + timestamp + "]";
    }

}

