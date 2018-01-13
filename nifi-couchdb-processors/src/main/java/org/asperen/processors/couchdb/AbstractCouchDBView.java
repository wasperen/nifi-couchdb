package org.asperen.processors.couchdb;

import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.View;

public abstract class AbstractCouchDBView extends AbstractCouchDB {

	public static final PropertyDescriptor COUCHDB_LIMIT = new PropertyDescriptor
            .Builder().name("COUCHDB_LIMIT")
            .displayName("Limit")
            .description("The maximum number of results to return, or -1 (the default) which means: all).")
            .defaultValue("-1")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_SKIP = new PropertyDescriptor
            .Builder().name("COUCHDB_SKIP")
            .displayName("Skip")
            .description("The number of results to skip (defaults to 0).")
            .required(false)
            .defaultValue("0")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_DESCENDING = new PropertyDescriptor
            .Builder().name("COUCHDB_DESCENDING")
            .displayName("Descending")
            .description("Indicate if the result should be sorted in descending order (defaults to false).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
	
	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_LIMIT);
		descriptors.add(COUCHDB_SKIP);
		descriptors.add(COUCHDB_DESCENDING);
		super.initDescriptors(descriptors);
	}
	
	protected void retrieveView(ProcessContext context, ProcessSession session, String fileName, View view) throws ProcessException {
		try {
			if (context.getProperty(COUCHDB_LIMIT).asInteger() > 0)
				view.limit(context.getProperty(COUCHDB_LIMIT).asInteger());
			if (context.getProperty(COUCHDB_SKIP).asInteger() > 0)
				view.skip(context.getProperty(COUCHDB_SKIP).asInteger());
			if (context.getProperty(COUCHDB_DESCENDING).asBoolean())
				view.descending(true);
			
			FlowFile flowFile = createFlowFile(session, fileName);
			flowFile = session.importFrom(view.queryForStream(), flowFile);
			session.transfer(flowFile, SUCCESS);
		} catch (Exception e) {
			context.yield();
			session.rollback();
			getLogger().error("Failed to retrieve all documents", e);
			throw new ProcessException(e);
		}
		
		session.commit();
	}

}