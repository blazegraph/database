package com.bigdata.journal;

/**
 * Drop a named index (unisolated write operation).
 * <p>
 * Note: the dropped index will continue to be visible to unisolated readers or
 * {@link IsolationEnum#ReadCommitted} isolated operations (since they read from
 * the most recent committed state) until the next commit. However, unisolated
 * writers that execute after the index has been dropped will NOT be able to see
 * the index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class DropIndexTask extends AbstractTask {

    public DropIndexTask(ConcurrentJournal journal,String name) {

        super(journal,ITx.UNISOLATED,false/*readOnly*/,name);

    }

    /**
     * Drop the named index.
     *  
     * @return A {@link Boolean} value that is <code>true</code> iff the
     *         index was pre-existing at the time that this task executed
     *         and therefore was dropped. <code>false</code> is returned
     *         iff the index did not exist at the time that this task was
     *         executed.
     */
    public Object doTask() throws Exception {

        journal.assertOpen();

        String name = getOnlyResource();

        try {

            journal.dropIndex(name);

        } catch (NoSuchIndexException ex) {

            /*
             * The index does not exist.
             */

            log.info("Index does not exist: " + name);

            return Boolean.FALSE;

        }

        return Boolean.TRUE;

    }
    
}