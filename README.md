# nifi-couchdb
This project provides a number of Nifi processors for getting and putting documents and data from and to a CouchDB 2 database.

It is still early days -- but we have the following processors working:

  * GetCouchDBAllDocuments -- gets all documents of a specified database
  * GetCouchDBDocument -- gets a document for which the FlowFile specifies _id and (optionally) _rev
  * GetCouchDBView -- gets the content of a View
  * GetCouchDBList -- gets the content of a View, passed through a List function in the same design document
  * PutCouchDBDocument -- puts the FlowFile into CouchDB and returns the document as it is stored
