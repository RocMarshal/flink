package org.apache.flink.connector.jdbc.base;

import javax.annotation.Nullable;

import java.io.Serializable;

/** User defined state. */
public interface OptionalUserDefinedState extends Serializable {
    @Nullable
    Serializable getOptionalUserDefinedState();

    void setOptionalUserDefinedState(@Nullable Serializable optionalUserDefinedState);
}
