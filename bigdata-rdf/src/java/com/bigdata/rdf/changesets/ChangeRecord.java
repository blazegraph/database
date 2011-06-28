package com.bigdata.rdf.changesets;

import java.util.Comparator;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPOComparator;

public class ChangeRecord implements IChangeRecord {
    
    private final ISPO stmt;
    
    private final ChangeAction action;
    
//    private final StatementEnum oldType;
    
    public ChangeRecord(final ISPO stmt, final ChangeAction action) {
        
//        this(stmt, action, null);
//        
//    }
//    
//    public ChangeRecord(final BigdataStatement stmt, final ChangeAction action, 
//            final StatementEnum oldType) {
//        
        this.stmt = stmt;
        this.action = action;
//        this.oldType = oldType;
        
    }
    
    public ChangeAction getChangeAction() {
        
        return action;
        
    }

//    public StatementEnum getOldStatementType() {
//        
//        return oldType;
//        
//    }

    public ISPO getStatement() {
        
        return stmt;
        
    }
   
    @Override
    public boolean equals(Object o) {
        
        if (o == this)
            return true;
        
        if (o == null || o instanceof IChangeRecord == false)
            return false;
        
        final IChangeRecord rec = (IChangeRecord) o;
        
        final ISPO stmt2 = rec.getStatement();
        
        // statements are equal
        if (stmt == stmt2 || 
                (stmt != null && stmt2 != null && stmt.equals(stmt2))) {
            
            // actions are equal
            return action == rec.getChangeAction();
            
        }
        
        return false;
        
    }
    
    public String toString() {
        
        StringBuilder sb = new StringBuilder();
        
        sb.append(action).append(": ").append(stmt);
        
        return sb.toString();
        
    }
    
    public static final Comparator<IChangeRecord> COMPARATOR = 
        new Comparator<IChangeRecord>() {
        
        public int compare(final IChangeRecord r1, final IChangeRecord r2) {
            
            final ISPO spo1 = r1.getStatement();
            final ISPO spo2 = r2.getStatement();
            
            return SPOComparator.INSTANCE.compare(spo1, spo2);
            
        }
        
    };
    
}
