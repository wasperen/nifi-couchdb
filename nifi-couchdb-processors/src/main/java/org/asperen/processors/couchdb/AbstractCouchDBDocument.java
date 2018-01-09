package org.asperen.processors.couchdb;

import java.util.List;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

public abstract class AbstractCouchDBDocument extends AbstractCouchDB {

	public static final PropertyDescriptor COUCHDB_INCLUDE_DOCUMENT = new PropertyDescriptor
	            .Builder().name("COUCHDB_INCLUDE_DOC")
	            .displayName("Include document")
	            .description("Whether or not to include the document that has been saved (defaults to false).")
	            .required(false)
	            .defaultValue("false")
	            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
	            .build();
	
	public static final Relationship FAILURE = new Relationship.Builder()
	            .name("FAILURE")
	            .description("Outputs the FlowFiles that have failed to be stored.")
	            .build();

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_INCLUDE_DOCUMENT);
		super.initDescriptors(descriptors);
	}

	@Override
	protected void initRelationships(Set<Relationship> relationships) {
		relationships.add(FAILURE);
		super.initRelationships(relationships);
	}

	protected boolean createDbClientOrFail(ProcessContext context, ProcessSession session, FlowFile flowFile) {
		try {
			createDbClient(context);
		} catch (ProcessException e) {
			getLogger().error("Could not obtain CouchDB connection", e);

			session.transfer(flowFile, FAILURE);
			return false;
		}
		return true;
	}
}
