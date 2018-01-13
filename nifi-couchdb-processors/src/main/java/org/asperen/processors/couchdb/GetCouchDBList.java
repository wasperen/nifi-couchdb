package org.asperen.processors.couchdb;

import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.View;

public class GetCouchDBList extends GetCouchDBView {
	
	public static final PropertyDescriptor COUCHDB_LISTNAME = new PropertyDescriptor
            .Builder().name("COUCHDB_LISTNAME")
            .displayName("List name")
            .description("The name of the list.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_MIMETYPE = new PropertyDescriptor
            .Builder().name("COUCHDB_MIMETYPE")
            .displayName("Mime-type")
            .description("The mime-type that is requested.")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_LISTNAME);
		descriptors.add(COUCHDB_MIMETYPE);
		super.initDescriptors(descriptors);
	}
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		createDbClient(context);
		String viewName = context.getProperty(COUCHDB_VIEWNAME).getValue();
		String listName = context.getProperty(COUCHDB_LISTNAME).getValue();
		String mimeType = context.getProperty(COUCHDB_MIMETYPE).getValue();
		View view = this.dbClient.list(viewName, listName, mimeType);
		retrieveView(context, session, listName.concat("@").concat(viewName), view);
	}

}
