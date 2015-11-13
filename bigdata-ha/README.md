The bigdata-ha and bigdata-ha-test modules are not included in the Blazegraph parent POM.  
If you use the maven eclipse commands below into a workspace with the rest of the artifacts checked out,
you'll be able to move and build between then.

To run, you need to have zookeeper installed and your MAVEN_HOME set.  You need to edit
bigdata-ha-test/testServices.properties with the values of the zookeeper installation.

From a checked out repo:

````
cd bigdata-ha
mvn eclipse:eclipse -Declipse.workspace=/path/to/your/workspace/
cd ../bigdata-ha-test/
mvn eclipse:eclipse -Declipse.workspace=/path/to/your/workspace/
````

Import the bigdata-ha and bigdata-ha-test in your Eclipse workspace.
