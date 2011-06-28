/**

Copyright (C) SYSTAP, LLC 2006-2007.  All rights reserved.

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
package com.bigdata.io;

import java.io.File;
import java.io.FileFilter;

/**
 * Helper class to filter for files with a known basename and extension.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class NameAndExtensionFilter implements FileFilter
{

    /**
     * From the constructor.
     */
    final private String basename;
    final private String ext;
    
    /**
     * Used to replace a <code>null</code> if no matching files are found with
     * an empty File[].
     */
    final private static File[] EMPTY_ARRAY = new File[]{};
    
    public NameAndExtensionFilter( String basename, String ext )
    {
    
        if( basename == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        if( ext == null ) {
            
            throw new IllegalArgumentException();
            
        }
        
        this.basename = basename;
        
        this.ext = ext;
        
    }
    
    /**
     * The <i>basename</i> parameter provided to the constructor.
     */
    public String getBaseName() {

        return basename;
        
    }
    
    /**
     * Accepts files ending with a <code>.log</code> extension whose name
     * component is shared by the name component found in
     * {@link #getLogBaseName()}.
     */
    public boolean accept(File pathname) {

        final String expectedName = new File( basename ).getName();
        
        final String actualName = pathname.getName();
        
        if (ext.length() == 0 || actualName.endsWith(ext)) {
        
            if( actualName.startsWith( expectedName ) ) {
                
                return true;
                
            }
        
        }
        
        return false;
        
    }
    
    /**
     * Return any existing log files within the directory identified by
     * {@link #getBaseName()}. If the basename names a directory, then all
     * files in that directory are scanned. Otherwise the parent directory
     * which would contain any file created using basename is scanned.
     * 
     * @return An array of zero or more files.
     */
    public File[] getFiles()
    {
    
        File file = new File( basename ).getAbsoluteFile();
        
        File dir = (file.isDirectory() ? file : file.getParentFile());
        
        File[] files = dir.listFiles( this );
        
        if( files == null ) {
        
            return EMPTY_ARRAY;
        
        } else {
            
            return files;
            
        }
        
    }
    
}