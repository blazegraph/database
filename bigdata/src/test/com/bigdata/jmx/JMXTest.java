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
 * Created on Mar 26, 2009
 */

package com.bigdata.jmx;

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.monitor.CounterMonitor;

/**
 * Class demonstrates the ability to declare get/set and get methods on the the
 * {@link IFoo} interface and expose a {@link Foo} implementation of that
 * interface as a {@link StandardMBean}. You can run {@link #main(String[])}
 * and start <code>jconsole</code> and inspect/set the "foo" attribute and see
 * the change on the read-only "bar" attribute. No fuss, no muss.
 * <p>
 * There are some caveats. In general, you can declare interfaces having getters
 * and directly expose the services implementing those interfaces as MBeans. If
 * the service supports a dynamic change to the value of an attribute, then you
 * can also put the settor on the interface to be exposed as an MBean.
 * <p>
 * The get/set methods need to be <code>synchronized</code> so that changes
 * made by a setter will become immediately visible to other code also
 * synchronized on the same reference. If the service internally uses the
 * getter, then consider having district objects on which you synchronize for
 * get() methods which are heavily used in order to minimize contention for the
 * object's monitor. The other option is to make the field for the attribute
 * <code>volatile</code>.
 * <p>
 * Since MBean methods do not declare RMI exceptions there can be a conflict in
 * the interface if the method is also exposed for RMI, e.g., for a jini-based
 * service.  You need to have two interfaces for this case.
 * <p>
 * There may be some cases where an attribute needs to be broken into a current
 * value and a target value, much like adjusting clocks to network time.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class JMXTest {

    /**
     * Interface to be exposed as an MBean.
     *  
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public interface IFoo {
        
        public int getFoo();
        
        public int setFoo(int foo);
        
        public String getBar();
        
    }
    
    /**
     * Object implementing that interface.
     * 
     * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
     * @version $Id$
     */
    public static class Foo implements IFoo {

        private int foo = 0;

        synchronized public int getFoo() {
            return foo;
        }

        // note: synchronized for visiblility of changes.
        synchronized public int setFoo(int foo) {
        
            final int t = this.foo;
            
            this.foo = foo;
            
            return t;
        }
        
        synchronized public String getBar() {
            return "bar" + foo;
        }

    }

    public interface IClock {
        
        public long getTime();
        
    }
    
    public static class Clock extends Thread implements IClock {
        
        private long time;
        
        public long getTime() {
            
            return time;
            
        }
        
        public void run() {
            
            while(true) {
                
                time = System.currentTimeMillis();
                
                try {
                    // this sets the update granularity.
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // done;
                    break;
                }
                
            }
            
        }
        
    }
    
    /**
     * When you run this class it registers an instance of {@link Foo} as an
     * MBean and then waits forever. You can run <code>jconsole</code> and
     * verify that you can see the MBean and its attributes and change attribute
     * values using the setter(s) declared on the {@link IFoo} interface.
     * 
     * @param args
     *            Not used.
     *            
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     * @throws NotCompliantMBeanException
     * @throws MalformedObjectNameException
     * @throws NullPointerException
     * @throws InterruptedException
     */
    public static void main(String[] args)
            throws InstanceAlreadyExistsException, MBeanRegistrationException,
            NotCompliantMBeanException, MalformedObjectNameException,
            NullPointerException, InterruptedException {

        // use this object to register your mbeans.
        final MBeanServer testMBeanServer = ManagementFactory
                .getPlatformMBeanServer();

        // create instance of your object.
        final IFoo foo = new Foo();

        // wrap as a standard mbean exposing the IFoo interface.
        final Object fooMBean = new StandardMBean(foo, IFoo.class);

        /*
         * Sets up a Clock MBean exposing the system time that updates its
         * internal state every 100 milliseconds. Then sets up a CounterMonitor
         * which observes that Clock MBean. Both are MBean and both have to be
         * registered before the notifications can be generated. The
         * CounterMBean is very picky about the data types for its arguments
         * (Threshold, Offset) all of which need to be Long since IClock is
         * returning a [long].
         * 
         * Based on http://javaboutique.internet.com/tutorials/jmx2/index2.html -
         * there are nearly no examples that I could find on the web for counter
         * monitor.
         */
        {

            // create and start clock.
            final Clock clock = new Clock();

            clock.setDaemon(true);

            clock.start();

            // wrap as a standard mbean exposing the IClockinterface.
            final Object clockMBean = new StandardMBean(clock, IClock.class);

            /*
             * Setup monitor for the clock.
             */
            final CounterMonitor cmon = new CounterMonitor();

            // register the mbean.
            testMBeanServer.registerMBean(fooMBean, new ObjectName(
                    "com.bigdata", "name", "Foo"));

            cmon.addObservedObject(new ObjectName("com.bigdata:name=Clock"));

            cmon.setObservedAttribute("Time");

            cmon.setGranularityPeriod(1000/* ms */);

            cmon.setInitThreshold((Long) System.currentTimeMillis());

            // offset after event trigger for retrigger.
            cmon.setOffset((Long) 5000L/* ms since clock is also time */);

            // set the difference mode ?!?
            cmon.setDifferenceMode(false);

            // make sure we get notified.
            cmon.setNotify(true);

            // register clock
            testMBeanServer.registerMBean(clockMBean, new ObjectName(
                    "com.bigdata:name=Clock"));

            // register the monitor.
            testMBeanServer.registerMBean(cmon, new ObjectName(
                    "com.bigdata:name=ClockMonitor"));

            // start the monitor (you can do this via jconsole as well).
            cmon.start();
        
        }
        
        // verify visible using jconsole.
        Thread.sleep(Long.MAX_VALUE);

    }

}
