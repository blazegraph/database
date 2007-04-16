/**

The Notice below must appear in each file of the Source Code of any
copy you distribute of the Licensed Product.  Contributors to any
Modifications may add their own copyright notices to identify their
own contributions.

License:

The contents of this file are subject to the CognitiveWeb Open Source
License Version 1.1 (the License).  You may not copy or use this file,
in either source code or executable form, except in compliance with
the License.  You may obtain a copy of the License from

  http://www.CognitiveWeb.org/legal/license/

Software distributed under the License is distributed on an AS IS
basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.  See
the License for the specific language governing rights and limitations
under the License.

Copyrights:

Portions created by or assigned to CognitiveWeb are Copyright
(c) 2003-2003 CognitiveWeb.  All Rights Reserved.  Contact
information for CognitiveWeb is available at

  http://www.CognitiveWeb.org

Portions Copyright (c) 2002-2003 Bryan Thompson.

Acknowledgements:

Special thanks to the developers of the Jabber Open Source License 1.0
(JOSL), from which this License was derived.  This License contains
terms that differ from JOSL.

Special thanks to the CognitiveWeb Open Source Contributors for their
suggestions and support of the Cognitive Web.

Modifications:

*/
package com.bigdata.scaleup;

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