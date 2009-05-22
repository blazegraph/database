/*

Copyright (C) SYSTAP, LLC 2006-2008.  All rights reserved.

Contact:
     SYSTAP, LLC
     4501 Tower Road
     Greensboro, NC 27410
     licenses@bigdata.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package edu.lehigh.swat.bench.ubt.bigdata;

import java.util.Properties;

import com.bigdata.service.jini.JiniClient;
import com.bigdata.service.jini.JiniFederation;

/**
 * Configuration for a {@link JiniClient} that will connect to an existing
 * {@link JiniFederation}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JDS extends BigdataRepositoryFactory {

    public JDS(String database) {

        super(database);

    }

    @Override
    public IRepositoryLifeCycle newLifeCycle() {

        return new LifeCycleJDS();

    }

    protected class LifeCycleJDS implements
            IRepositoryLifeCycle<JiniFederation> {

        private JiniFederation fed;

        public Properties getProperties() {

            return JDS.this.getProperties();

        }
    
        protected JiniFederation connect() {

            if (fed == null) {
                
                /*
                 * The client configuration file.
                 * 
                 * Note: You can set properties for the client in this file.
                 */

                final String[] args = new String[] {
                        database
                };
                
                fed = JiniClient.newInstance(args).connect();
                
            }
            
            /*
             * Await at least N data services and one metadata service
             * (otherwise abort).
             */
            {

                final Properties properties = getProperties();

                final int minDataServices = Integer
                        .parseInt(properties
                                .getProperty(
                                        BigdataRepositoryFactory.Options.MIN_DATA_SERVICES,
                                        BigdataRepositoryFactory.Options.DEFAULT_MIN_DATA_SERVICES));

                final long timeout = 10 * 1000;// ms

                System.out.println("Awaiting data services: minDataServices="
                        + minDataServices + ", timeout=" + timeout + "ms");
                int N;
                try {
                    N = fed.awaitServices(minDataServices, timeout).length;
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }

                System.out.println("Will run with " + N + " data services");
            }
            
            return fed;
            
        }
        
        public JiniFederation open() {

            return connect();

        }

        public void close(JiniFederation indexManager) {

//            if (indexManager.isOpen()) {
//
//                if(BigdataRepositoryFactory.log.isInfoEnabled()) {
//                    
//                    BigdataRepositoryFactory.log.info("Close: "+indexManager);
//                    
//                }
//
//                indexManager.shutdown();
//                
//            }

        }
        
        protected void finalize() throws Throwable {

            if (fed != null && fed.isOpen()) {

                // normal shutdown please.
                fed.shutdown();

            }

            super.finalize();

        }

    }

}
