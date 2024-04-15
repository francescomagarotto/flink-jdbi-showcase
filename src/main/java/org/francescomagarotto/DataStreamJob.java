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

package org.francescomagarotto;

import java.sql.Connection;
import java.sql.ResultSet;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.francescomagarotto.sink.JdbiOutputFormat;
import org.francescomagarotto.sink.JdbiSink;
import org.francescomagarotto.sink.Person;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        try (final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();) {
            JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptionsBuilder()
                    .withDriverName("org.h2.Driver")
                    .withPassword("sa")
                    .withUsername("sa")
                    .withUrl("jdbc:h2:mem:testdb;INIT=RUNSCRIPT FROM 'classpath:init.sql'")
                    .build();
            SimpleJdbcConnectionProvider connectionProvider = new SimpleJdbcConnectionProvider(
                    jdbcOptions);
            JdbiSink<Person> personJdbiSink = new JdbiSink<>(
                    new JdbiOutputFormat<>("INSERT INTO PERS(name) VALUES (:name)",
                            connectionProvider,
                            JdbcExecutionOptions.builder().withBatchIntervalMs(1000)
                                    .withBatchSize(100).withMaxRetries(2).build()));

            env.fromSequence(0, 1000000)
                    .map(String::valueOf)
                    .map(Person::new)
                    .addSink(personJdbiSink);
            /*
             * Here, you can start creating your execution plan for Flink.
             *
             * Start with getting some data from the environment, like
             * 	env.fromSequence(1, 10);
             *
             * then, transform the resulting DataStream<Long> using operations
             * like
             * 	.filter()
             * 	.flatMap()
             * 	.window()
             * 	.process()
             *
             * and many more.
             * Have a look at the programming guide:
             *
             * https://nightlies.apache.org/flink/flink-docs-stable/
             *
             */

            // Execute program, beginning computation.
            env.execute("Flink Java API Skeleton");
        }
        //private final transient String sql;
        //protected final JdbcConnectionProvider connectionProvider;
        //private final JdbcExecutionOptions executionOptions;

    }
}
