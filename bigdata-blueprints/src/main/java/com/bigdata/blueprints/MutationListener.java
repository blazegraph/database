package com.bigdata.blueprints;

import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;

public class MutationListener implements IChangeLog {
    
    int nInserted = 0;
    int nRemoved = 0;
    int nUpdated = 0;
    
    public MutationListener() {
        clear();
    }
    
    public void clear() {
        nInserted = nRemoved = nUpdated = 0;
    }
    
    public int getNumInserted() {
        return nInserted;
    }
    
    public int getNumRemoved() {
        return nRemoved;
    }
    
    public int getNumUpdated() {
        return nInserted;
    }
    
    @Override
    public void changeEvent(final IChangeRecord record) {
        final ChangeAction action = record.getChangeAction();
        if (action == ChangeAction.INSERTED) {
            nInserted++;
        } else if (action == ChangeAction.REMOVED) {
            nRemoved++;
        } else {
            nUpdated++;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void transactionAborted() {
    }

    @Override
    public void transactionBegin() {
    }

    @Override
    public void transactionCommited(long arg0) {
    }

    @Override
    public void transactionPrepare() {
    }

}

