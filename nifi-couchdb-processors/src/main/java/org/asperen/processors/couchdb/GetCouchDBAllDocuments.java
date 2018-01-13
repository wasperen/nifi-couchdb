package org.asperen.processors.couchdb;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.lightcouch.View;


@Tags({"couchdb", "ingres"})
@CapabilityDescription("Retrieves all documents from a CouchDB database. "
		+ "Either in one FlowFile or in separate pages with a maximum number of rows.")
public class GetCouchDBAllDocuments extends AbstractCouchDBView {

	static final String VIEW_ALL_DOCS = "_all_docs";

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		View view = this.dbClient.view(VIEW_ALL_DOCS);
		retrieveView(context, session, VIEW_ALL_DOCS, view);
	}

	
}
