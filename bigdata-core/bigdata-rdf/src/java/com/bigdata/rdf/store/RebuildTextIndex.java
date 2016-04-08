/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

package com.bigdata.rdf.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.bigdata.Banner;
import com.bigdata.journal.ITx;
import com.bigdata.journal.Journal;
import com.bigdata.journal.Options;


/**
 * A utility class to rebuild text index in an {@link AbstractTripleStore}.
 */


public class RebuildTextIndex {

    /**
     * Utility method to rebuid text index in a local journal.
     * 
     * @param args
     *            <code>[-namespace <i>namespace</i>] [-forceCreate] propertyFile</code>
     *            where
     *            <dl>
     *            <dt>-namespace</dt>
     *            <dd>The namespace of the KB instance.</dd>
     *            <dt>-forceCreate</dt>
     *            <dd>When <code>true</code> a new text index will be created
     *            if does not exist, <code>false</code> is default</dd>
     *            <dt>propertyFile</dt>
     *            <dd>The configuration file for the database instance.</dd>
     *            </dl>
     */
    public static void main(final String[] args) throws IOException {
        
        Banner.banner();
        
        String namespace = null;
        
        boolean forceCreate = false;
        
        int i = 0;
        
        while (i < args.length) {
            
            final String arg = args[i];
            
            if (arg.startsWith("-")) {
                
                if (arg.equals("-namespace")) {
                    
                    namespace = args[++i];
                    
                } else if (arg.equals("-forceCreate")){
                    
                    forceCreate = true;
                    
                } else {
                    
                    System.err.println("Unknown argument: " + arg);
                    
                    usage();
                    
                }
                
            } else {
                
                break;
                
            }
            
            i++;
            
        }
        
        final int remaining = args.length - i;
        
        if (remaining < 1/*allow run w/o any named files or directories*/) {
            
            System.err.println("Not enough arguments.");
            
            usage();
            
        }
        
        final String propertyFileName = args[i++];
        
        final Properties properties = processProperties(propertyFileName);
        
        File journal = new File(properties.getProperty(Options.FILE));
        
        if (journal.exists()) {
            
            
            System.out.println("Journal: " + properties.getProperty(Options.FILE));
            
            Journal jnl = null;
            
            try {
                
                jnl = new Journal(properties);
                
                System.out.println("Rebuild text index:");
                
                if (namespace == null) {
                
                    List<String> namespaces = jnl.getGlobalRowStore().getNamespaces(jnl.getLastCommitTime());
                    
                    for (String nm : namespaces) {
                        
                        AbstractTripleStore kb = (AbstractTripleStore) jnl
                                .getResourceLocator().locate(nm, ITx.UNISOLATED);
                        
                        if (kb.getLexiconRelation().isTextIndex()) {
                        
                        
                            kb.getLexiconRelation().rebuildTextIndex(false /*forceCreate*/);
                            
                            System.out.println(nm + " - completed");
                            
                        
                        } else {
                            
                            System.out.println(nm + " -  no text index");
                            
                        }
                        
                    }
                
                } else {
                    
                        AbstractTripleStore kb = (AbstractTripleStore) jnl
                                .getResourceLocator().locate(namespace, ITx.UNISOLATED);
                        
                        if (kb != null) {
                            
                            kb.getLexiconRelation().rebuildTextIndex(forceCreate);
                            
                            System.out.println(namespace + " - completed");
                        
                        } else { 
                            
                            System.err.println("Namespace " + namespace + " does not exist");
                            
                        }
                        
                }
                
            } finally {
                
                jnl.close();
                
            }
            
        } else {
            
            System.err.println("Journal " + journal + " does not exist");
            
        }

        
    }
    
    public static Properties processProperties(final String propertyFileName) throws IOException {
        
        final File propertyFile = new File(propertyFileName);
        
        if (!propertyFile.exists()) {
            
            throw new FileNotFoundException(propertyFile.toString());
            
        }
        
        final Properties properties = new Properties();
        
        final InputStream is = new FileInputStream(propertyFile);
        try {
            properties.load(is);
        } finally {
            if (is != null) {
                is.close();
            }
        }
        
        return properties;
   }
    
    private static void usage() {
        
        System.err.println("usage: [-namespace namespace] [-forceCreate] propertyFile");
        
        System.exit(1);
        
        
    }
    
}
