package org.asperen.processors.couchdb;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.Page;
import org.lightcouch.View;

import com.google.gson.JsonObject;

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
	
	public static final PropertyDescriptor COUCHDB_PAGINATED = new PropertyDescriptor
            .Builder().name("COUCHDB_PAGINATED")
            .displayName("Paginated")
            .description("Indicate if the result should come in pages (defaults to false). Only works for simple string keys.")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_PAGESIZE = new PropertyDescriptor
            .Builder().name("COUCHDB_PAGESIZE")
            .displayName("Page size")
            .description("The maximum number of rows to load into a single page flow-file (defaults to 100).")
            .required(false)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

	@Override
	protected void initDescriptors(List<PropertyDescriptor> descriptors) {
		descriptors.add(COUCHDB_LIMIT);
		descriptors.add(COUCHDB_SKIP);
		descriptors.add(COUCHDB_DESCENDING);
		descriptors.add(COUCHDB_PAGINATED);
		descriptors.add(COUCHDB_PAGESIZE);
		super.initDescriptors(descriptors);
	}

	protected void retrieveView(ProcessContext context, ProcessSession session, String viewName) throws ProcessException {
		createDbClient(context);
		
		try {
			View view = this.dbClient.view(viewName);
			if (context.getProperty(COUCHDB_LIMIT).asInteger() > 0)
				view.limit(context.getProperty(COUCHDB_LIMIT).asInteger());
			if (context.getProperty(COUCHDB_SKIP).asInteger() > 0)
				view.skip(context.getProperty(COUCHDB_SKIP).asInteger());
			if (context.getProperty(COUCHDB_DESCENDING).asBoolean())
				view.descending(true);
			
			if (context.getProperty(COUCHDB_PAGINATED).asBoolean()) {
				int pageSize = context.getProperty(COUCHDB_PAGESIZE).asInteger();
				String nextPage = null;
				do {
					final Page<JsonObject> page = view.queryPage(pageSize, nextPage, JsonObject.class);
					final String jsonPage = this.dbClient.getGson().toJson(page);
					
					FlowFile flowFile = createFlowFile(session, viewName + "_" + String.valueOf(page.getPageNumber()));
					flowFile = session.write(flowFile, new OutputStreamCallback() {
						
						@Override
						public void process(OutputStream out) throws IOException {
							out.write(jsonPage.getBytes());
						}
						
					});
					session.transfer(flowFile, SUCCESS);
					
					nextPage = page.isHasNext() ? page.getNextParam() : null;
				} while (nextPage != null);
			} else {
				FlowFile flowFile = createFlowFile(session, viewName);
				flowFile = session.importFrom(view.queryForStream(), flowFile);
				session.transfer(flowFile, SUCCESS);
			} 
		} catch (Exception e) {
			context.yield();
			session.rollback();
			getLogger().error("Failed to retrieve all documents", e);
			throw new ProcessException(e);
		}
		
		session.commit();
	}

}