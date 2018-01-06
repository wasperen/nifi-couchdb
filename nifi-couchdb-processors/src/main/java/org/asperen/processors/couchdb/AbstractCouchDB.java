package org.asperen.processors.couchdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbException;
import org.lightcouch.CouchDbProperties;

public abstract class AbstractCouchDB extends AbstractProcessor {

	public static final PropertyDescriptor COUCHDB_SERVER = new PropertyDescriptor
            .Builder().name("COUCHDB_SERVER")
            .displayName("Server address")
            .description("Address of the CouchDB to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_PORT = new PropertyDescriptor
            .Builder().name("COUCHDB_PORT")
            .displayName("Server port")
            .description("Port of the CouchDB to connect to (defaults to 5984).")
            .required(false)
            .defaultValue("5984")
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_USERNAME = new PropertyDescriptor
            .Builder().name("COUCHDB_USERNAME")
            .displayName("Username")
            .description("Username to use to log into the CouchDB server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_PASSWORD = new PropertyDescriptor
            .Builder().name("COUCHDB_PASSWORD")
            .displayName("Password")
            .description("Password to us to log into the CouchDB server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
	
	public static final PropertyDescriptor COUCHDB_DATABASE = new PropertyDescriptor
            .Builder().name("COUCHDB_DATABASE")
            .displayName("Database")
            .description("Database to connect to on the CouchDB server.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
	
	public static final PropertyDescriptor COUCHDB_AUTOCREATE = new PropertyDescriptor
            .Builder().name("COUCHDB_AUTOCREATE")
            .displayName("Auto-create")
            .description("Whether or not to create the database if it does not exist (defaults to false).")
            .required(false)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
	
	public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Outputs the json resonse from the CouchDB server.")
            .build();

	private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    
    protected CouchDbClient dbClient = null;

    protected void initDescriptors(List<PropertyDescriptor> descriptors) {
    	this.descriptors = Collections.unmodifiableList(descriptors);
    }
    
    protected void initRelationships(Set<Relationship> relationships) {
        this.relationships = Collections.unmodifiableSet(relationships);
    }
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(COUCHDB_SERVER);
        descriptors.add(COUCHDB_PORT);
        descriptors.add(COUCHDB_USERNAME);
        descriptors.add(COUCHDB_PASSWORD);
        descriptors.add(COUCHDB_DATABASE);
        descriptors.add(COUCHDB_AUTOCREATE);
        initDescriptors(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        initRelationships(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    protected void createDbClient(ProcessContext context) throws ProcessException  {
		try {
			CouchDbProperties properties = new CouchDbProperties()
					  .setDbName(context.getProperty(COUCHDB_DATABASE).getValue())
					  .setCreateDbIfNotExist(context.getProperty(COUCHDB_AUTOCREATE).asBoolean())
					  .setProtocol("http")
					  .setHost(context.getProperty(COUCHDB_SERVER).getValue())
					  .setPort(context.getProperty(COUCHDB_PORT).asInteger())
					  .setUsername(context.getProperty(COUCHDB_USERNAME).getValue())
					  .setPassword(context.getProperty(COUCHDB_PASSWORD).getValue())
					  .setMaxConnections(100)
					  .setConnectionTimeout(0);
	
			this.dbClient = new CouchDbClient(properties);
		} catch (CouchDbException e) {
			throw new ProcessException(e);
		}
	}

	@OnStopped
	public void onStopped() {
		if (this.dbClient != null)
			try {
				dbClient.close();
			} catch (IOException e) {
				getLogger().warn("exception when closing CouchDB connection");
			}
	}
	
}
