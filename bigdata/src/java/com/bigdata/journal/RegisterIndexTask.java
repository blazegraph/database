package com.bigdata.journal;

import com.bigdata.btree.IIndex;

/**
 * Register a named index (unisolated write operation).
 * <p>
 * Note: the registered index will NOT be visible to unisolated readers or
 * isolated operations until the next commit. However, unisolated writers
 * that execute after the index has been registered will be able to see the
 * registered index.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class RegisterIndexTask extends AbstractIndexTask {

    final private IIndex btree;
    
    /**
     * @param journal
     * @param name
     * @param btree
     *            The index object. Use
     *            <code>new UnisolatedBTree(journal, UUID.randomUUID()</code>
     *            to register a new index that supports isolation.
     */
    public RegisterIndexTask(ConcurrentJournal journal, String name, IIndex btree) {

        super(journal, ITx.UNISOLATED, false/*readOnly*/, name);
        
        if(btree==null) throw new NullPointerException();
        
        this.btree = btree;
        
    }

    /**
     * Create the named index(s).
     * 
     * @return A {@link Boolean} value that is true iff the index was
     *         created and false iff it already exists.
     */
    protected Object doTask() throws Exception {

        journal.assertOpen();

        String name = getOnlyResource();
        
        synchronized (journal.name2Addr) {

            try {
                
                // add to the persistent name map.
                journal.name2Addr.add(name, btree);

                log.info("Registered index: name=" + name + ", class="
                        + btree.getClass() + ", indexUUID="
                        + btree.getIndexUUID());
                
            } catch(IndexExistsException ex) {
                
                log.info("Index exists: "+name);
                
                return Boolean.FALSE;
                
            }

        }

        // report event (the application has access to the named index).
        ResourceManager.openUnisolatedBTree(name);

        return Boolean.TRUE;

    }

}