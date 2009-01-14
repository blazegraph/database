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
/*
 * Created on Jan 13, 2009
 */

package com.bigdata.jini.start;

import java.util.LinkedList;
import java.util.List;

import net.jini.config.Configuration;
import net.jini.config.ConfigurationProvider;

import com.bigdata.Banner;
import com.bigdata.jini.start.config.JavaServiceConfiguration;
import com.bigdata.jini.start.config.ServiceConfiguration;
import com.bigdata.jini.start.config.JavaServiceConfiguration.JavaServiceStarter;
import com.bigdata.jini.start.process.ProcessHelper;

/**
 * Starts an unmanaged service using the specified configuration.
 * <p>
 * The service is executed in a child process using
 * {@link ServiceConfiguration#newServiceStarter(IServiceListener)}. That child
 * process will have the environment and command line as described for the
 * service in the {@link Configuration} resource.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ServiceStarter {

    /**
     * 
     */
    public ServiceStarter() {
    }

    /**
     * Executes the named service configuration. The configuration file and any
     * overrides will be passed onto the child process. For example:
     * 
     * <pre>
     * java com.bigdata.start.ServiceStarter -Djava.security.policy=policy.all com.bigdata.jini.start.ServicesManagerServer src/resources/config/bigdata.config
     * </pre>
     * 
     * would execute the {@link ServicesManagerServer}, bootstrapping its own
     * execution environment from the same configuration file that it will use
     * itself.
     * <p>
     * Similar commands can be used to start application specific jini clients
     * serving as masters for distributed jobs.
     * 
     * @param args
     *            Options followed by the name of the service configuration to
     *            execute followed by the the jini {@link Configuration} file
     *            (or URL), and then by optional overrides.
     *            <p>
     *            The defined options are:
     *            <dl>
     *            <dt>-n</dt>
     *            <dd>Write the command that would be executed on stdout, but
     *            do not execute the command. This is useful both if you want to
     *            run the command directly and if you want to validate the
     *            {@link Configuration}.</dd>
     *            </dl>
     * 
     * @throws Exception
     * 
     * @todo it would be nice if we could start any subclass
     *       {@link JavaServiceConfiguration} in this manner. The problem is
     *       that there is no declarative link between the class name of the
     *       server and the class name of the {@link ServiceConfiguration}. We
     *       would need to be able to figure out the
     *       {@link ServiceConfiguration} class from the server class name,
     *       instantiate the specific {@link ServiceConfiguration} class, and
     *       then use it to start the service.
     */
    public static void main(String[] args) throws Exception {

        boolean noExecute = false;
        
        // break out options.
        int i = 0;
        for (; i < args.length; i++) {
            
            final String arg = args[i];
            
            if("-n".equals(arg)) {
                
                noExecute = true;
                
                continue;
                
            } else if(arg.startsWith("-")) {
                
                System.err.println("Unknown option: "+arg);
                
            } else {
            
                i--;
                
                break;
                
            }
            
        }

        if ((args.length - i) < 2) {

            /*
             * Not enough arguments remaining?
             * 
             * Note: You must specify the className and the Configuration.
             */
            
            usage();

            System.exit(1);

        }
        
        // the class to execute.
        final String className = args[i++];

        final List<String> passedOn = new LinkedList<String>();

        for (; i < args.length; i++) {

            passedOn.add(args[i]);
            
        }

        // the Configuration file and any overrides.
        final String[] args2 = passedOn.toArray(new String[]{});
        
        /*
         * Read jini configuration.
         */
        final Configuration config = ConfigurationProvider.getInstance(args2);

        /*
         * Create a service configuration object for the named class and then
         * start an instance of that class.
         */
        final JavaServiceConfiguration serviceConfig = new JavaServiceConfiguration(
                className, config);

        // pass through any options.
        serviceConfig.options = ServiceConfiguration.concat(
                serviceConfig.options, args2);

        // get the service starter.
        final JavaServiceStarter serviceStarter = serviceConfig
                .newServiceStarter(new ServiceListener());
        
        // always echo the command that we will execute.
        writeCommand(serviceStarter.newProcessBuilder());
        
        if(!noExecute) {
            
            // show banner.
            Banner.banner();
         
            // run the service.
            serviceStarter.call();
            
        }
        
        System.exit(0);
        
    }
    
    static private void writeCommand(ProcessBuilder processBuilder) {
        
        boolean first = true;
        
        for(String s : processBuilder.command()) {
            
            if(!first) {

                System.out.print(' ');
                
            }
            
            System.out.print(s);
            
            first = false;
            
        }
        
        System.out.println();
        
    }
    
    protected static void usage() {
        
        System.err.println("usage: [-n] class configuration [override]*");
        
    }

    /**
     * A NOP impl.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    private static class ServiceListener implements IServiceListener {

        public void add(ProcessHelper service) {

        }

        public void remove(ProcessHelper service) {

        }

    }

}
