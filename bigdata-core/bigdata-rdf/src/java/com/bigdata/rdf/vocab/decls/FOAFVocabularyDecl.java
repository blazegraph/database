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
 * Vocabulary and namespace for FOAF, not including terms marked as "archaic".
 * 
 * @see http://xmlns.com/foaf/spec/
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class FOAFVocabularyDecl implements VocabularyDecl {

    /*
     * Namespace
     */
    public static final String NAMESPACE = "http://xmlns.com/foaf/0.1/";
    /*
     * Classes
     */
    public static final URI Agent = new URIImpl(NAMESPACE + "Agent"); 
    public static final URI Document = new URIImpl(NAMESPACE + "Document"); 
    public static final URI Group= new URIImpl(NAMESPACE + "Group"); 
    public static final URI Image = new URIImpl(NAMESPACE + "Image"); 
    public static final URI LabelProperty = new URIImpl(NAMESPACE + "LabelProperty"); 
    public static final URI OnlineAccount = new URIImpl(NAMESPACE + "OnlineAccount"); 
    public static final URI OnlineChatAccount = new URIImpl(NAMESPACE + "OnlineChatAccount"); 
    public static final URI OnlineEcommerceAccount = new URIImpl(NAMESPACE + "OnlineEcommerceAccount"); 
    public static final URI OnlineGamingAccount = new URIImpl(NAMESPACE + "OnlineGamingAccount"); 
    public static final URI Organization = new URIImpl(NAMESPACE + "Organization"); 
    public static final URI Person = new URIImpl(NAMESPACE + "Person");
    public static final URI PersonalProfileDocument = new URIImpl(NAMESPACE + "PersonalProfileDocument"); 
    public static final URI Project = new URIImpl(NAMESPACE + "Project");
    /*
     * Properties.
     */
    public static final URI account = new URIImpl(NAMESPACE + "account");
    public static final URI accountName = new URIImpl(NAMESPACE + "accountName");
    public static final URI accountServiceHomepage = new URIImpl(NAMESPACE + "accountServiceHomepage");
    public static final URI age = new URIImpl(NAMESPACE + "age");
    public static final URI aimChatID = new URIImpl(NAMESPACE + "aimChatID");
    public static final URI based_near = new URIImpl(NAMESPACE + "based_near");
    public static final URI birthday = new URIImpl(NAMESPACE + "birthday");
    public static final URI currentProject = new URIImpl(NAMESPACE + "currentProject");
    public static final URI depiction = new URIImpl(NAMESPACE + "depiction");
    public static final URI depicts = new URIImpl(NAMESPACE + "depicts");
    public static final URI familyName = new URIImpl(NAMESPACE + "familyName");
    public static final URI firstName = new URIImpl(NAMESPACE + "firstName");
    public static final URI gender = new URIImpl(NAMESPACE + "gender");
    public static final URI givenName = new URIImpl(NAMESPACE + "givenName");
    public static final URI homepage = new URIImpl(NAMESPACE + "homepage");
    public static final URI icqChatID = new URIImpl(NAMESPACE + "icqChatID");
    public static final URI img = new URIImpl(NAMESPACE + "img");
    public static final URI interest = new URIImpl(NAMESPACE + "interest");
    public static final URI isPrimaryTopicOf = new URIImpl(NAMESPACE + "isPrimaryTopicOf");
    public static final URI jabberID = new URIImpl(NAMESPACE + "jabberID");
    public static final URI knows = new URIImpl(NAMESPACE + "knows");
    public static final URI lastName = new URIImpl(NAMESPACE + "lastName");
    public static final URI logo = new URIImpl(NAMESPACE + "logo");
    public static final URI made = new URIImpl(NAMESPACE + "made");
    public static final URI maker = new URIImpl(NAMESPACE + "maker");
    public static final URI mbox = new URIImpl(NAMESPACE + "mbox");
    public static final URI mbox_sha1sum = new URIImpl(NAMESPACE + "mbox_sha1sum");
    public static final URI member = new URIImpl(NAMESPACE + "member");
    public static final URI membershipClass = new URIImpl(NAMESPACE + "membershipClass");
    public static final URI msnChatID = new URIImpl(NAMESPACE + "msnChatID");
    public static final URI myersBriggs = new URIImpl(NAMESPACE + "myersBriggs");
    public static final URI name = new URIImpl(NAMESPACE + "name");
    public static final URI nick = new URIImpl(NAMESPACE + "nick");
    public static final URI openid = new URIImpl(NAMESPACE + "openid");
    public static final URI page = new URIImpl(NAMESPACE + "page");
    public static final URI pastProject = new URIImpl(NAMESPACE + "pastProject");
    public static final URI phone = new URIImpl(NAMESPACE + "phone");
    public static final URI plan = new URIImpl(NAMESPACE + "plan");
    public static final URI primaryTopic = new URIImpl(NAMESPACE + "primaryTopic");
    public static final URI publications = new URIImpl(NAMESPACE + "publications");
    public static final URI schoolHomepage = new URIImpl(NAMESPACE + "schoolHomepage");
    public static final URI sha1 = new URIImpl(NAMESPACE + "sha1");
    public static final URI skypeID = new URIImpl(NAMESPACE + "skypeID");
    public static final URI status = new URIImpl(NAMESPACE + "status");
    public static final URI thumbnail = new URIImpl(NAMESPACE + "thumbnail");
    public static final URI tipjar = new URIImpl(NAMESPACE + "tipjar");
    public static final URI title = new URIImpl(NAMESPACE + "title");
    public static final URI topic = new URIImpl(NAMESPACE + "topic");
    public static final URI topic_interest = new URIImpl(NAMESPACE + "topic_interest");
    public static final URI weblog = new URIImpl(NAMESPACE + "weblog");
    public static final URI workInfoHomepage = new URIImpl(NAMESPACE + "workInfoHomepage");
    public static final URI workplaceHomepage = new URIImpl(NAMESPACE + "workplaceHomepage");
    public static final URI yahooChatID = new URIImpl(NAMESPACE + "yahooChatID");
    // archaic
//  new URIImpl(NAMESPACE + "dnaChecksum"),//
//  new URIImpl(NAMESPACE + "family_name"),//
//  new URIImpl(NAMESPACE + "fundedBy"),//
//  new URIImpl(NAMESPACE + "geekcode"),//
//  new URIImpl(NAMESPACE + "givenname"),//
//  new URIImpl(NAMESPACE + "holdsAccount"),//
//  new URIImpl(NAMESPACE + "theme"),//

    /*
     * Note: While FOAF is pretty dynamic, DO NOT add new terms to this
     * vocabulary as that can introduce a vocabulary inconsistency (which will
     * be reported as an error for existing KBs). Instead, the NAMESPACE will be
     * used to provide a reasonably tight representation for any new terms added
     * to this vocabulary over time.
     */
    static private final URI[] uris = new URI[] {//
        new URIImpl(NAMESPACE),//
            // Classes
            Agent, Document, Group, Image, LabelProperty, OnlineAccount,
            OnlineChatAccount, OnlineEcommerceAccount, OnlineGamingAccount,
            Organization, Person, PersonalProfileDocument, Project,
            // Properties
            account, accountName, accountServiceHomepage, age, aimChatID,
            based_near, birthday, currentProject, depiction, depicts,
            familyName, firstName, gender, givenName, homepage, icqChatID, img,
            interest, isPrimaryTopicOf, jabberID, knows, lastName, logo, made,
            maker, mbox, mbox_sha1sum, member, membershipClass, msnChatID,
            myersBriggs, name, nick, openid, page, pastProject, phone, plan,
            primaryTopic, publications, schoolHomepage, sha1, skypeID, status,
            thumbnail, tipjar, title, topic, topic_interest, weblog,
            workInfoHomepage, workplaceHomepage, yahooChatID,
    };

    public FOAFVocabularyDecl() {
    }

    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(uris)).iterator();

    }

}
