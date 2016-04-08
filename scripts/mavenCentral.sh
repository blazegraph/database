#!/bin/bash
# Deploys to maven central, which uses the maven-central profile in the parent POM.
#
# You must have a ~/.m2/settings.xml setup with username and password for maven central.
#    
#    <?xml version="1.0"?>
#    <settings>
#      <servers>
#        <server>
#          <id>ossrh</id>
#          <username>username</username>
#          <password>password</password>
#        </server>
#      </servers>
#    </settings>
#  
#   It is recommended that you use Maven password encryption:  https://maven.apache.org/guides/mini/guide-encryption.html
#

BASE_DIR=`dirname $0`

#Deploy the parent pom artifacts
mvn -f "${BASE_DIR}"/../pom.xml install deploy -N -DskipTests=true -P maven-central,Development

#Deploy the core artifacts
mvn -f "${BASE_DIR}"/../pom.xml install deploy -DskipTests=true -P maven-central,Development

#Deploy the deployment artifacts
mvn -f "${BASE_DIR}"/../blazegraph-artifacts/pom.xml install deploy -N -DskipTests=true -Pmaven-central

#Temporarily disabled for blazegraph-rpm see BLZG-1725.  Using specific artifacts
projects="blazegraph-jar bigdata-jar blazegraph-war bigdata-war blazegraph-deb"
for project in $projects; do
	mvn -f "${BASE_DIR}"/../$project/pom.xml install deploy -DskipTests=true -P maven-central
done



