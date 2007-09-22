package com.bigdata.service.mapReduce;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
     * Abstract base class for the state of a map/reduce job that is relevant to
     * a {@link MapService} or a {@link ReduceService}.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public class AbstractJobState {

        /**
         * The job identifier.
         */
        final UUID uuid;

        /**
         * The job start time.
         */
        final long begin = System.currentTimeMillis();
        
//        /**
//         * The #of map tasks started for this job on this service.
//         */
//        long nstarted = 0L;
//        
//        /**
//         * The #of map tasks ended
//         */
//        long nended = 0L;

        /**
         * the running tasks for this job. The key is the task UUID. The value
         * is the {@link Future} for that task.
         */
        Map<UUID,Future<Object>> futures = new ConcurrentHashMap<UUID, Future<Object>>();
        
        protected AbstractJobState(UUID uuid) {

            if(uuid==null) throw new IllegalArgumentException();
            
            this.uuid = uuid;
            
        }

        /**
         * Cancel all running tasks for this job.
         * <p>
         * Note: The job MUST be cancelled first since otherwise tasks could
         * continue to be queued while this method is running.
         * 
         * @todo could be done in parallel?
         */
        public void cancelAll() {

            int n = 0;
            
            Iterator<Future<Object>> itr = futures.values().iterator();
            
            while(itr.hasNext()) {
                
                Future<Object> future;
                
                try {
                 
                    future = itr.next();
                    
                } catch(NoSuchElementException ex) {
                    
                    MapService.log.info("Exhausted by concurrent completion.");
                    
                    break;
                    
                }
                
                future.cancel(true/*may interrupt if running*/);
                
                n++;
                
                try {
                    
                    itr.remove();
                    
                } catch(NoSuchElementException ex) {
                    
                    MapService.log.info("Task already gone.");
                    
                }
                
            }
            
            MapService.log.info("Cancelled "+n+" tasks for job="+uuid);
            
        }

        /**
         * Cancel the task if it is running.
         * 
         * @param task
         *            The task identifier.
         * 
         * @return true if the job was cancelled.
         */
        public boolean cancel(UUID task) {
            
            Future<Object> future = futures.remove(task);
            
            if(future!=null && future.cancel(true/*may interrupt if running*/)) {

                MapService.log.info("Cancelled task: job="+uuid+", task="+task);
                
                return true;
                
            } else {
                
                MapService.log.info("Could not cancel task - not running? : job="+uuid+", task="+task);

                return false;
                
            }
            
        }
        
    }