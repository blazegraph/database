package com.bigdata.service;

import java.io.Serializable;

import com.bigdata.btree.proc.IIndexProcedure;
import com.bigdata.journal.AbstractTask;
import com.bigdata.journal.DropIndexTask;
import com.bigdata.journal.RegisterIndexTask;

/**
 * Interface for procedures that require access to the {@link IDataService}
 * and or the federation.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * 
 * @todo register index and drop index could be rewritten as submitted
 *       procedures derived from this class. This would simplify the
 *       {@link IDataService} API and metrics collection further. The
 *       implementations would have to be distinct from
 *       {@link RegisterIndexTask} and {@link DropIndexTask} since those
 *       extend {@link AbstractTask} - that class does not implement
 *       {@link IIndexProcedure} and can not be sent across the wire.
 */
public interface IDataServiceAwareProcedure extends Serializable {

    /**
     * Invoked before the task is executed to given the procedure a
     * reference to the {@link IDataService} on which it is executing.
     */
    void setDataService(DataService dataService);
    
}