/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.connector.jdbc;

import org.apache.eventmesh.connector.jdbc.source.SourceMateData;

import lombok.Getter;
import lombok.Setter;

public final class Payload {

    public static final String AFTER_FIELD = "after";

    public static final String BEFORE_FIELD = "before";

    public static final String SOURCE = "source";

    public static final String PAYLOAD_BEFORE = "payload.before";

    public static final String PAYLOAD_AFTER = "payload.after";

    @Getter
    @Setter
    private SourceMateData source;

    @Getter
    @Setter
    private String ddl;

    @Getter
    @Setter
    private CatalogChanges catalogChanges;

    @Getter
    @Setter
    private DataChanges dataChanges;

    @Getter
    @Setter
    private long timestamp;

    /**
     * Constructs an empty <code>HashMap</code> with the default initial capacity (16) and the default load factor (0.75).
     */
    public Payload() {
        this.timestamp = System.currentTimeMillis();
    }

    public Payload withSource(SourceMateData source) {
        this.source = source;
        return this;
    }

    public Payload withDdl(String ddl) {
        this.ddl = ddl;
        return this;
    }

    public Payload withCatalogChanges(CatalogChanges catalogChanges) {
        this.catalogChanges = catalogChanges;
        return this;
    }

    public Payload withDataChanges(DataChanges dataChanges) {
        this.dataChanges = dataChanges;
        return this;
    }

    public SourceMateData ofSourceMateData() {
        return getSource();
    }

    public CatalogChanges ofCatalogChanges() {
        return getCatalogChanges();
    }

    public DataChanges ofDataChanges() {
        return getDataChanges();
    }

    public String ofDdl() {
        return getDdl();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final Payload payload;

        private Builder() {
            payload = new Payload();
        }

        public Builder withSource(SourceMateData source) {
            payload.withSource(source);
            return this;
        }

        public Payload build() {
            return payload;
        }
    }

}
