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
/*
 * Created on Jun 4, 2011
 */

package com.bigdata.rdf.vocab.decls;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.vocab.VocabularyDecl;

/**
 * Vocabulary and namespace for LUBM using the default namespace.
 * 
 * @see http://swat.cse.lehigh.edu/projects/lubm/ 
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class LUBMVocabularyDecl implements VocabularyDecl {

    private static final String NAMESPACE = "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#";
    
    static private final URI[] uris = new URI[]{
        // namespace
        new URIImpl(NAMESPACE),//
        // classes
        new URIImpl(NAMESPACE + "AdministrativeStaff"),//
        new URIImpl(NAMESPACE + "Article"),//
        new URIImpl(NAMESPACE + "AssistantProfessor"),//
        new URIImpl(NAMESPACE + "AssociateProfessor"),//
        new URIImpl(NAMESPACE + "Book"),//
        new URIImpl(NAMESPACE + "Chair"),//
        new URIImpl(NAMESPACE + "ClericalStaff"),//
        new URIImpl(NAMESPACE + "College"),//
        new URIImpl(NAMESPACE + "ConferencePaper"),//
        new URIImpl(NAMESPACE + "Course"),//
        new URIImpl(NAMESPACE + "Dean"),//
        new URIImpl(NAMESPACE + "Department"),//
        new URIImpl(NAMESPACE + "Director"),//
        new URIImpl(NAMESPACE + "Employee"),//
        new URIImpl(NAMESPACE + "Faculty"),//
        new URIImpl(NAMESPACE + "FullProfessor"),//
        new URIImpl(NAMESPACE + "GraduateCourse"),//
        new URIImpl(NAMESPACE + "GraduateStudent"),//
        new URIImpl(NAMESPACE + "Institute"),//
        new URIImpl(NAMESPACE + "JournalArticle"),//
        new URIImpl(NAMESPACE + "Lecturer"),//
        new URIImpl(NAMESPACE + "Manual"),//
        new URIImpl(NAMESPACE + "Organization"),//
        new URIImpl(NAMESPACE + "Person"),//
        new URIImpl(NAMESPACE + "PostDoc"),//
        new URIImpl(NAMESPACE + "Professor"),//
        new URIImpl(NAMESPACE + "Program"),//
        new URIImpl(NAMESPACE + "Publication"),//
        new URIImpl(NAMESPACE + "Research"),//
        new URIImpl(NAMESPACE + "ResearchAssistant"),//
        new URIImpl(NAMESPACE + "ResearchGroup"),//
        new URIImpl(NAMESPACE + "Schedule"),//
        new URIImpl(NAMESPACE + "Software"),//
        new URIImpl(NAMESPACE + "Specification"),//
        new URIImpl(NAMESPACE + "Student"),//
        new URIImpl(NAMESPACE + "SystemsStaff"),//
        new URIImpl(NAMESPACE + "TeachingAssistant"),//
        new URIImpl(NAMESPACE + "TechnicalReport"),//
        new URIImpl(NAMESPACE + "UndergraduateStudent"),//
        new URIImpl(NAMESPACE + "University"),//
        new URIImpl(NAMESPACE + "UnofficialPublication"),//
        new URIImpl(NAMESPACE + "VisitingProfessor"),//
        new URIImpl(NAMESPACE + "Work"),//
        // predicates
        new URIImpl(NAMESPACE + "advisor"),//
        new URIImpl(NAMESPACE + "affiliatedOrganizationOf"),//
        new URIImpl(NAMESPACE + "affiliateOf"),//
        new URIImpl(NAMESPACE + "age"),//
        new URIImpl(NAMESPACE + "degreeFrom"),//
        new URIImpl(NAMESPACE + "doctoralDegreeFrom"),//
        new URIImpl(NAMESPACE + "emailAddress"),//
        new URIImpl(NAMESPACE + "hasAlumnus"),//
        new URIImpl(NAMESPACE + "headOf"),//
        new URIImpl(NAMESPACE + "listedCourse"),//
        new URIImpl(NAMESPACE + "mastersDegreeFrom"),//
        new URIImpl(NAMESPACE + "member"),//
        new URIImpl(NAMESPACE + "memberOf"),//
        new URIImpl(NAMESPACE + "name"),//
        new URIImpl(NAMESPACE + "officeNumber"),//
        new URIImpl(NAMESPACE + "orgPublication"),//
        new URIImpl(NAMESPACE + "publicationAuthor"),//
        new URIImpl(NAMESPACE + "publicationDate"),//
        new URIImpl(NAMESPACE + "publicationResearch"),//
        new URIImpl(NAMESPACE + "researchInterest"),//
        new URIImpl(NAMESPACE + "researchProject"),//
        new URIImpl(NAMESPACE + "softwareDocumentation"),//
        new URIImpl(NAMESPACE + "softwareVersion"),//
        new URIImpl(NAMESPACE + "subOrganizationOf"),//
        new URIImpl(NAMESPACE + "takesCourse"),//
        new URIImpl(NAMESPACE + "teacherOf"),//
        new URIImpl(NAMESPACE + "teachingAssistantOf"),//
        new URIImpl(NAMESPACE + "telephone"),//
        new URIImpl(NAMESPACE + "tenured"),//
        new URIImpl(NAMESPACE + "title"),//
        new URIImpl(NAMESPACE + "undergraduateDegreeFrom"),//
        new URIImpl(NAMESPACE + "worksFor"),//
    };

    public LUBMVocabularyDecl() {
    }
    
    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();
        
    }

}
