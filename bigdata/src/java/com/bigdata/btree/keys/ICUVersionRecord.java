/**

Copyright (C) SYSTAP, LLC 2006-2011.  All rights reserved.

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
 * Created on Mar 21, 2011
 */

package com.bigdata.btree.keys;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.bigdata.journal.Name2Addr;
import com.ibm.icu.util.VersionInfo;

/**
 * Persistent record in which we store the version metadata for the ICU
 * dependency in use when the journal was created. bigdata uses Unicode sort
 * keys for various indices, including {@link Name2Addr}. A change in the ICU
 * version can result in sort keys which are NOT compatible. Binary
 * compatibility for Unicode sort keys is an absolute requirement for bigdata.
 * The purpose of this persistence capable data record is to note the version of
 * ICU against which bigdata was linked with the associated binary store file
 * was created.
 * <p>
 * Note: This can result in data which apparently becomes "lost", such as this
 * <a href="http://sourceforge.net/apps/trac/bigdata/ticket/193>trac issue</a>.
 * The underlying problem was substituting a newer version of ICU for the one
 * included in the bigdata distribution. Such errors are now caught by detecting
 * a change in the ICU runtime environment.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class ICUVersionRecord implements Externalizable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private VersionInfo icuVersion;
    private VersionInfo ucolRuntimeVersion;
    private VersionInfo ucolBuilderVersion;
    private VersionInfo ucolTailoringsVersion;
    
    /**
     * The ICU software version number.
     * 
     * @see VersionInfo#ICU_VERSION
     */
    public VersionInfo getICUVersion() {
        return icuVersion;
    }
    
    /**
     * If this version number changes, then the sort keys for the same Unicode
     * string could be different.
     * 
     * @see VersionInfo#UCOL_RUNTIME_VERSION
     */
    public VersionInfo getUColRuntimeVersion() {
        return ucolRuntimeVersion;
    }
    
    /**
     * If this version number changes, then the same tailoring might result in
     * assigning different collation elements to code points (which could break
     * binary compatibility on sort keys).
     * 
     * @see VersionInfo#UCOL_BUILDER_VERSION
     */
    public VersionInfo getUColBuilderVersion() {
        return ucolBuilderVersion;
    }
    
    /**
     * The version of the collation tailorings.
     * 
     * @see VersionInfo#UCOL_TAILORINGS_VERSION
     */
    public VersionInfo getUColTailoringsVersion() {
        return ucolTailoringsVersion;
    }
    
    /**
     * Factory returns a record reporting on the ICU dependency as currently
     * linked with the code base.
     */
    public static ICUVersionRecord newInstance() {
        
        final ICUVersionRecord r = new ICUVersionRecord(//
                VersionInfo.ICU_VERSION,//
                VersionInfo.UCOL_RUNTIME_VERSION,//
                VersionInfo.UCOL_BUILDER_VERSION,//
                VersionInfo.UCOL_TAILORINGS_VERSION//
                );

        return r;

    }

    ICUVersionRecord(//
            VersionInfo icuVersion,//
            VersionInfo ucolRuntimeVesion,//
            VersionInfo ucolBuilderVersion,//
            VersionInfo ucolTailoringsVersion//
    ) {

        this.icuVersion = icuVersion;

        this.ucolRuntimeVersion = ucolRuntimeVesion;

        this.ucolBuilderVersion = ucolBuilderVersion;

        this.ucolTailoringsVersion = ucolTailoringsVersion;

    }

    /**
     * De-serialization contructor <strong>only</strong>.
     */
    public ICUVersionRecord() {

    }


    /** The initial version of this data record. */
    private static final transient int VERSION0 = 0; 
    
    private static final transient int CURRENT_VERSION = VERSION0;
    
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {

        final int version = in.readInt();
        switch (version) {
        case VERSION0:
            break;
        default:
            throw new IOException("Unknown version: " + version);
        }

        icuVersion = readVersionInfo(in);
        ucolRuntimeVersion = readVersionInfo(in);
        ucolBuilderVersion = readVersionInfo(in);
        ucolTailoringsVersion = readVersionInfo(in);

    }

    public void writeExternal(ObjectOutput out) throws IOException {
     
        out.writeInt(CURRENT_VERSION);
        writeVersionInfo(icuVersion, out);
        writeVersionInfo(ucolRuntimeVersion, out);
        writeVersionInfo(ucolBuilderVersion, out);
        writeVersionInfo(ucolTailoringsVersion, out);
        
    }

    private VersionInfo readVersionInfo(final ObjectInput in) throws IOException {

        final int major = in.readInt();
        final int minor = in.readInt();
        final int micro = in.readInt();
        final int milli = in.readInt();
        
        return VersionInfo.getInstance(major, minor, milli, micro);
        
    }
    
    private void writeVersionInfo(final VersionInfo v, final ObjectOutput out)
            throws IOException {

        out.writeInt(v.getMajor());
        out.writeInt(v.getMinor());
        out.writeInt(v.getMicro());
        out.writeInt(v.getMilli());

    }

    /**
     * A human readable representation of the data record.
     */
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getName());
        sb.append("{icuVersion=" + icuVersion);
        sb.append(",ucolRuntimeVersion=" + ucolRuntimeVersion);
        sb.append(",ucolBuilderVersion=" + ucolBuilderVersion);
        sb.append(",ucolTailoringsVersion=" + ucolTailoringsVersion);
        sb.append("}");
        return sb.toString();
    }
    
    public int hashCode() {
        return super.hashCode();
    }
    
    public boolean equals(final Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ICUVersionRecord))
            return false;
        final ICUVersionRecord r = (ICUVersionRecord) o;
        if (!icuVersion.equals(r.icuVersion))
            return false;
        if (!ucolRuntimeVersion.equals(r.ucolRuntimeVersion))
            return false;
        if (!ucolBuilderVersion.equals(r.ucolBuilderVersion))
            return false;
        if (!ucolTailoringsVersion.equals(r.ucolTailoringsVersion))
            return false;
        return true;
    }

    /**
     * Writes out the {@link ICUVersionRecord} for the current classpath.
     * 
     * @param args
     *            Ignored.
     */
    static public void main(String[] args) {

        System.out.println(ICUVersionRecord.newInstance().toString());

        final ICUVersionRecord a = ICUVersionRecord.newInstance();
        final ICUVersionRecord b = ICUVersionRecord.newInstance();
        
        if(!a.equals(b))
            throw new AssertionError();
        if(!b.equals(a))
            throw new AssertionError();
        
    }

}
