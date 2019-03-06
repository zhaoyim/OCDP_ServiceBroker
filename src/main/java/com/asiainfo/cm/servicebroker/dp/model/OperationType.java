package com.asiainfo.cm.servicebroker.dp.model;

/**
 * Created by baikai on 7/25/16.
 */
public enum OperationType {
    /**
     * Indicates that a service instance has been created.
     */
    PROVISION("provision"),

    /**
     * Indicates that a service instance has been deleted.
     */
    DELETE("delete"),

    /**
     * Indicates that a service instance has been updated.
     */
    UPDATE("update");

    private final String state;

    OperationType(String state) {
        this.state = state;
    }

    public String getValue() {
        return state;
    }
}