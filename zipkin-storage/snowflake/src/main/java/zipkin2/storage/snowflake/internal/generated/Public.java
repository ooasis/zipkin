/*
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
/*
 * This file is generated by jOOQ.
 */
package zipkin2.storage.snowflake.internal.generated;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import org.jooq.Catalog;
import org.jooq.Table;
import org.jooq.impl.SchemaImpl;

import zipkin2.storage.snowflake.internal.generated.tables.SpanAnnotations;
import zipkin2.storage.snowflake.internal.generated.tables.SpanTags;
import zipkin2.storage.snowflake.internal.generated.tables.Spans;


/**
 * This class is generated by jOOQ.
 */
@Generated(
    value = {
        "http://www.jooq.org",
        "jOOQ version:3.11.7"
    },
    comments = "This class is generated by jOOQ"
)
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Public extends SchemaImpl {

    private static final long serialVersionUID = 1437903270;

    /**
     * The reference instance of <code>PUBLIC</code>
     */
    public static final Public PUBLIC = new Public();

    /**
     * The table <code>PUBLIC.SPANS</code>.
     */
    public final Spans SPANS = zipkin2.storage.snowflake.internal.generated.tables.Spans.SPANS;

    /**
     * The table <code>PUBLIC.SPAN_ANNOTATIONS</code>.
     */
    public final SpanAnnotations SPAN_ANNOTATIONS = zipkin2.storage.snowflake.internal.generated.tables.SpanAnnotations.SPAN_ANNOTATIONS;

    /**
     * The table <code>PUBLIC.SPAN_TAGS</code>.
     */
    public final SpanTags SPAN_TAGS = zipkin2.storage.snowflake.internal.generated.tables.SpanTags.SPAN_TAGS;

    /**
     * No further instances allowed
     */
    private Public() {
        super("PUBLIC", null);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Catalog getCatalog() {
        return DefaultCatalog.DEFAULT_CATALOG;
    }

    @Override
    public final List<Table<?>> getTables() {
        List result = new ArrayList();
        result.addAll(getTables0());
        return result;
    }

    private final List<Table<?>> getTables0() {
        return Arrays.<Table<?>>asList(
            Spans.SPANS,
            SpanAnnotations.SPAN_ANNOTATIONS,
            SpanTags.SPAN_TAGS);
    }
}
