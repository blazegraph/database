package com.bigdata.objndx;

/**
 * A helper class that collects statistics on an {@link AbstractBTree}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo add nano timers and track storage used by the index. The goal is to
 *       know how much of the time of the server is consumed by the index,
 *       what percentage of the store is dedicated to the index, how
 *       expensive it is to do some scan-based operations (merged down,
 *       delete of transactional isolated persistent index), and evaluate
 *       the buffer strategy by comparing accesses with IOs.
 */
public class Counters {

    protected final AbstractBTree btree;
    
    public Counters(AbstractBTree btree) {
        
        assert btree != null;
        
        this.btree = btree;
        
    }
    
    int nfinds = 0; // #of keys looked up in the tree by lookup(key).
    int nbloomRejects = 0; // #of keys rejected by the bloom filter in lookup(key).
    int ninserts = 0;
    int nremoves = 0;
    int nindexOf = 0;
    int ngetKey = 0;
    int ngetValue = 0;
    int rootsSplit = 0;
    int rootsJoined = 0;
    int nodesSplit = 0;
    int nodesJoined = 0;
    int leavesSplit = 0;
    int leavesJoined = 0; // @todo also merge vs redistribution of keys on remove (and insert if b*-tree)
    int nodesCopyOnWrite = 0;
    int leavesCopyOnWrite = 0;
    int nodesRead = 0;
    int leavesRead = 0;
    int nodesWritten = 0;
    int leavesWritten = 0;
    long bytesRead = 0L;
    long bytesWritten = 0L;

    // @todo consider changing to logging so that the format will be nicer
    // or just improve the formatting.
    public String toString() {
        
        return 
        "height="+btree.getHeight()+
        ", #nodes="+btree.getNodeCount()+
        ", #leaves="+btree.getLeafCount()+
        ", #entries="+btree.getEntryCount()+
        ", #find="+nfinds+
        ", #bloomRejects="+nbloomRejects+
        ", #insert="+ninserts+
        ", #remove="+nremoves+
        ", #indexOf="+nindexOf+
        ", #getKey="+ngetKey+
        ", #getValue="+ngetValue+
        ", #roots split="+rootsSplit+
        ", #roots joined="+rootsJoined+
        ", #nodes split="+nodesSplit+
        ", #nodes joined="+nodesJoined+
        ", #leaves split="+leavesSplit+
        ", #leaves joined="+leavesJoined+
        ", #nodes copyOnWrite="+nodesCopyOnWrite+
        ", #leaves copyOnWrite="+leavesCopyOnWrite+
        ", read ("+nodesRead+" nodes, "+leavesRead+" leaves, "+bytesRead+" bytes)"+
        ", wrote ("+nodesWritten+" nodes, "+leavesWritten+" leaves, "+bytesWritten+" bytes)"
        ;
        
    }

}
