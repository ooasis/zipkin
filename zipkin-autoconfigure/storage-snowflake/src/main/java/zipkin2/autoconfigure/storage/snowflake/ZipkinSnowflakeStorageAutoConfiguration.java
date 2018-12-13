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
package zipkin2.autoconfigure.storage.snowflake;

import java.util.concurrent.Executor;
import javax.sql.DataSource;
import org.jooq.ExecuteListenerProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.snowflake.SnowflakeStorage;

@Configuration
@EnableConfigurationProperties(ZipkinSnowflakeStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "snowflake")
@ConditionalOnMissingBean(StorageComponent.class)
class ZipkinSnowflakeStorageAutoConfiguration {
  @Autowired(required = false)
  ZipkinSnowflakeStorageProperties snowflake;

  @Autowired(required = false)
  @Qualifier("tracingExecuteListenerProvider")
  ExecuteListenerProvider listener;

  @Bean
  @ConditionalOnMissingBean(Executor.class)
  Executor executor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setThreadNamePrefix("ZipkinSnowflakeStorage-");
    executor.initialize();
    return executor;
  }

  @Bean
  @ConditionalOnMissingBean(DataSource.class)
  DataSource snowflakelDataSource() {
    return snowflake.toDataSource();
  }

  @Bean
  StorageComponent storage(
      Executor executor,
      DataSource dataSource,
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId) {
    return SnowflakeStorage.builder()
        .strictTraceId(strictTraceId)
        .searchEnabled(true)
        .executor(executor)
        .datasource(dataSource)
        .build();
  }
}
