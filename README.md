# Content Graph

The content graph module provides a bridge between the JCR and MySQL. It is able to quickly index predefined JCR content in a simple data model. Instead of using slow and resource intensive JCR queries to retrieve information from the JCR you can query the MySQL database instead. This provides a great range of relational goodness you wouldn't otherwise have. 

#### Prerequisites:

* MySQL Server with a database for each of your AEM instances
* Install this bundle onto all your AEM instances.

#### Configuration

* Look for bundles starting with 'UoA' and configure:
	* the paths that are included in the reindexer
	* the paths that are to be excluded (e.g. usergenerated content)
	* the number of seconds between periodic writes
	* the JDBC connection string, username and password

#### Manager servlets

The bundle ships with a number of servlets you can use to manage the index:

* `/bin/contentgraph/reindex.do`: to reindex the index
* `/bin/contentgraph/reset.do`: to reset the synchronisation state table and force a reindex

#### Project structure

The project is split up into three modules:

* `contentgraph-bundle`: this bundle provides the generic functionality that is required to synchronise content between the JCR and any other system. It gathers all `Synchronizer` service implementations and calls them when something changes.

* `dbsynchronizer`: this bundle provides the MySQL database synchronization functionality that is triggered by the first bundle.

* `contentgraph-content`: the bundle that embeds the bundles above and installs them into the OSGi container.

#### Cool things

Because of the way the infrastructure is built you can synchronize the JCR with many other systems. One nice use of this system (which has yet to be implemented) would be to signal your ESB that certain content in the CMS has changed so that downstream systems are triggered to retrieve the latest version.

If you have any questions, just mail me: marnixkok@gmail.com