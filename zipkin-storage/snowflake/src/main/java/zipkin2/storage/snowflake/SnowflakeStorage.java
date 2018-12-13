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
package zipkin2.storage.snowflake;

import com.google.auto.value.AutoValue;
import org.jooq.ExecuteListenerProvider;
import org.jooq.conf.Settings;
import zipkin2.internal.Nullable;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;

import javax.sql.DataSource;
import java.util.concurrent.Executor;

@AutoValue
public abstract class SnowflakeStorage extends StorageComponent {

  @Override
  public SpanStore spanStore() {
    return new SnowflakeSpanStore(this);
  }

  @Override
  public SpanConsumer spanConsumer() {
    return new SnowflakeSpanConsumer(this);
  }

  @AutoValue.Builder
  public static abstract class Builder extends StorageComponent.Builder {

    boolean strictTraceId = true;
    boolean searchEnabled = true;
    Settings settings = new Settings().withRenderSchema(false);

    @Override
    public abstract Builder strictTraceId(boolean strictTraceId);

    @Override
    public abstract Builder searchEnabled(boolean searchEnabled);

    public abstract Builder datasource(DataSource datasource);

    public abstract Builder settings(Settings settings);

    public abstract Builder listenerProvider(@Nullable ExecuteListenerProvider listenerProvider);

    public abstract Builder executor(Executor executor);

    @Override
    public abstract SnowflakeStorage build();

    Builder() {}
  }

  abstract boolean strictTraceId();
  abstract boolean searchEnabled();
  abstract DataSource datasource();
  abstract @Nullable  Settings settings();
  abstract @Nullable ExecuteListenerProvider listenerProvider();
  abstract Executor executor();

  public static Builder builder() {
    return new AutoValue_SnowflakeStorage.Builder();
  }

}
