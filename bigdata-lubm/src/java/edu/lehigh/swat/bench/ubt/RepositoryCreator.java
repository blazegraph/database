/**
 * by Yuanbo Guo
 * Semantic Web and Agent Technology Lab, CSE Department, Lehigh University, USA
 * Copyright (C) 2004
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 59 Temple
 * Place, Suite 330, Boston, MA 02111-1307 USA
 */

package edu.lehigh.swat.bench.ubt;

import java.lang.reflect.Constructor;

import edu.lehigh.swat.bench.ubt.api.Repository;
import edu.lehigh.swat.bench.ubt.api.RepositoryFactory;

public abstract class RepositoryCreator {
    
    /**
     * Creates the Repository object from the specified repository factory class
     * name.
     * 
     * @param factoryName
     *            Name of the repository factory class.
     * @param database
     *            The value of the <code>database</code> property from the kb
     *            config file. This parameter is used iff the
     *            {@link RepositoryFactory} declares a ctor accepting the
     *            {@link String} <i>database</i> as its sole argument.
     *            Otherwise the zero argument ctor must be defined and will be
     *            used (the ctor specified by the original test harness).
     * 
     * @return The created Repository object, null if the specified class name
     *         is invalid.
     */
    protected static Repository createRepository(String factoryName,
            String database) {
        
        RepositoryFactory factory;
        
        try {
            
            final Class<RepositoryFactory> cls = (Class<RepositoryFactory>) Class
                    .forName(factoryName);
            
            final Constructor<RepositoryFactory> ctor;
            try {

                // lookup the one arg ctor.
                ctor = cls.getConstructor(new Class[] { String.class });

            } catch (NoSuchMethodException ex) {

                // use the zero arg ctor instead.
                factory = cls.newInstance();

                return factory.create();

            }
            
            // use the one arg ctor.
            factory = ctor.newInstance(new Object[] { database });
                        
            return factory.create();
    
        } catch (Exception e) {
            
            e.printStackTrace();
            
            return null;
            
        }
    
    }
    
}