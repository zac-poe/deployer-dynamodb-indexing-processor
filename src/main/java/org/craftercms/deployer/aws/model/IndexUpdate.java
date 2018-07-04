package org.craftercms.deployer.aws.model;

/**
 * Represents an update to be applied in the search index.
 *
 * @author joseross
 */
public class IndexUpdate {

    /**
     * Id of the document in the search index.
     */
    protected String id;

    /**
     * Type of the update.
     */
    protected Type type;

    /**
     * Full body to be updated in the search index.
     */
    protected String body;

    /**
     * Name of the field to be updated in the search index.
     */
    protected String field;

    /**
     * Value for the field to be updated in the search index.
     */
    protected String value;

    /**
     * Indicates if the value should be split during indexing.
     */
    protected boolean multivalued;

    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public String getBody() {
        return body;
    }

    public void setBody(final String body) {
        this.body = body;
    }

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public void setField(final String field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(final String value) {
        this.value = value;
    }

    public boolean isMultivalued() {
        return multivalued;
    }

    public void setMultivalued(final boolean multivalued) {
        this.multivalued = multivalued;
    }

    /**
     * Types of updates supported.
     */
    public enum Type {
        DOCUMENT,
        FIELD
    }

}
