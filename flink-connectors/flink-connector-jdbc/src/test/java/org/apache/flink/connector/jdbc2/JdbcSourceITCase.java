/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc2.source.ContinuousEnumerationSettings;
import org.apache.flink.connector.jdbc2.source.JdbcSource;
import org.apache.flink.connector.jdbc2.source.JdbcSourceBuilder;
import org.apache.flink.connector.jdbc2.source.enumerator.SqlTemplateSplitEnumerator;
import org.apache.flink.connector.jdbc2.source.enumerator.parameter.provider.JdbcGenericParameterValuesProvider;
import org.apache.flink.connector.jdbc2.source.reader.extractor.RowResultExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;

public class JdbcSourceITCase {

    @Test
    public void test() throws Exception {
        JdbcSource<Row> jdbcSource =
                new JdbcSourceBuilder<Row>()
                        .setDBUrl("jdbc:mysql://localhost:3306/test")
                        .setDriverName("com.mysql.cj.jdbc.Driver")
                        .setResultExtractor(new RowResultExtractor())
                        .setPassword("123456789")
                        .setUsername("root")
                        .setTypeInformation(TypeInformation.of(new TypeHint<Row>() {}))
                        .setSqlSplitEnumeratorProvider(
                                new SqlTemplateSplitEnumerator.TemplateSqlSplitEnumeratorProvider()
                                        .setSqlTemplate("select * from t1 where id=?")
                                        .setParameterValuesProvider(
                                                new JdbcGenericParameterValuesProvider(
                                                        new Serializable[][] {{1}, {1}})))
                        .setContinuousEnumerationSettings(
                                new ContinuousEnumerationSettings(Duration.ofSeconds(5)))
                        .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(jdbcSource, WatermarkStrategy.noWatermarks(), "mysource").print();
        env.execute();
    }
}
