package org.apache.flink.connector.rabbitmq2.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQContainerClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.CONSISTENCY_MODE;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PASSWORD;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.PORT;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.QUEUE;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.USERNAME;
import static org.apache.flink.connector.rabbitmq2.table.RabbitMQConnectorOptions.VIRTUAL_HOST;
import static org.assertj.core.api.Assertions.assertThat;

/** A Test class for RabbitMQ table. */
@RunWith(Parameterized.class)
public class RabbitMQTableITCase {

    private static final int rabbitmqPort = 5672;
    private static final String virtualHost = "/";
    private static final String rabbitTable = "rabbitTable";
    private static final String selectAll = "select * from " + rabbitTable;
    private static final String insertSql =
            "insert into " + rabbitTable + " (`id`, `name`) values %s";
    private static final DockerImageName image =
            DockerImageName.parse("rabbitmq").withTag("3.7.25-management-alpine");

    private StreamExecutionEnvironment sEnv;
    private StreamTableEnvironment tEnv;

    @Rule
    public RabbitMQContainer rabbitMq = new RabbitMQContainer(image).withExposedPorts(rabbitmqPort);

    @Parameterized.Parameter public ConsistencyMode consistencyMode;

    @Parameterized.Parameters(name = "consistencyMode = {0}")
    public static Collection<ConsistencyMode> parameters() {
        return Arrays.asList(
                ConsistencyMode.AT_LEAST_ONCE,
                ConsistencyMode.AT_MOST_ONCE,
                ConsistencyMode.EXACTLY_ONCE);
    }

    @Before
    public void setup() {
        sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.enableCheckpointing(10L);
        sEnv.setParallelism(1);
        tEnv = StreamTableEnvironment.create(sEnv);
    }

    private void executeWithSuccessExceptionDetection(StreamExecutionEnvironment sEnv)
            throws Exception {
        try {
            sEnv.execute();
        } catch (Throwable e) {
            // we have to use a specific exception to indicate the job is finished,
            // because the registered RabbitMQ source is infinite.
            if (!isCausedByJobFinished(e)) {
                // re-throw
                throw e;
            }
        }
    }

    private List<TestBean> getRandomRowBeans(int numberOfMessages) {
        List<TestBean> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add(new TestBean(i + 1, UUID.randomUUID().toString()));
        }
        return messages;
    }

    private List<String> toJsonStringList(List<TestBean> testBeans) {
        ObjectMapper mapper = new ObjectMapper();
        return testBeans.stream()
                .map(
                        ele -> {
                            try {
                                return mapper.writeValueAsString(ele);
                            } catch (JsonProcessingException e) {
                                return null;
                            }
                        })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static boolean isCausedByJobFinished(Throwable e) {
        if (e instanceof SuccessException) {
            return true;
        } else if (e.getCause() != null) {
            return isCausedByJobFinished(e.getCause());
        } else {
            return false;
        }
    }

    /** A Test tool sink function. */
    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        public List<String> rows = new ArrayList<>();

        private int expectedSize;
        private ConsistencyMode consistencyMode = ConsistencyMode.EXACTLY_ONCE;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        private TestingSinkFunction(int expectedSize, ConsistencyMode consistencyMode) {
            this(expectedSize);
            this.consistencyMode = Preconditions.checkNotNull(consistencyMode);
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value.toString());
            if (consistencyMode == ConsistencyMode.AT_LEAST_ONCE) {
                if (rows.size() >= expectedSize) {
                    // job finish
                    throw new SuccessException();
                }
            } else if (consistencyMode == ConsistencyMode.AT_MOST_ONCE) {
                if (rows.size() <= expectedSize) {
                    // job finish
                    throw new SuccessException();
                }
            } else if (consistencyMode == ConsistencyMode.EXACTLY_ONCE) {
                if (rows.size() == expectedSize) {
                    // job finish
                    throw new SuccessException();
                }
            }
        }
    }

    /** A bean class for test. */
    @Internal
    @JsonIgnoreProperties
    public static class TestBean implements Serializable {
        private long id;
        private String name;

        public TestBean(long id, String name) {
            this.id = id;
            this.name = name;
        }

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String toSql() {
            return String.format("(%s, '%s')", id, name);
        }
    }

    private String getTableDDL(String queue, ConsistencyMode mode, RabbitMQContainer rabbitMq) {
        return "CREATE TABLE "
                + rabbitTable
                + " (\n"
                + "  `id` BIGINT,\n"
                + "  `name` STRING\n"
                + ")  WITH (\n"
                + " 'connector' = 'rabbitmq',\n"
                + String.format(" '%s' = '%s',\n", VIRTUAL_HOST.key(), virtualHost)
                + String.format(" '%s' = '%s',\n", "format", "json")
                + String.format(" '%s' = '%s',\n", USERNAME.key(), rabbitMq.getAdminUsername())
                + String.format(" '%s' = '%s',\n", PASSWORD.key(), rabbitMq.getAdminPassword())
                + String.format(" '%s' = '%s',\n", QUEUE.key(), queue)
                + String.format(" '%s' = '%s',\n", CONSISTENCY_MODE.key(), mode)
                + String.format(
                        " '%s' = '%s'\n)", PORT.key(), rabbitMq.getMappedPort(rabbitmqPort));
    }

    private String toSqlString(List<TestBean> messages) {
        return messages.stream().map(TestBean::toSql).collect(Collectors.joining(","));
    }

    // --------------- sink test cases ---------

    @Test
    public void testTableSink() throws Exception {
        List<TestBean> messages = getRandomRowBeans(100);
        RabbitMQContainerClient<String> client =
                new RabbitMQContainerClient<>(rabbitMq, new SimpleStringSchema(), messages.size());
        String queue = client.createQueue(true);
        String ddl = getTableDDL(queue, consistencyMode, rabbitMq);
        tEnv.executeSql(ddl);
        String sql = String.format(insertSql, toSqlString(messages));
        tEnv.executeSql(sql).await();
        client.await();
        if (consistencyMode == ConsistencyMode.EXACTLY_ONCE) {
            assertThat(toJsonStringList(messages)).isEqualTo(client.getConsumedMessages());
        } else if (consistencyMode == ConsistencyMode.AT_LEAST_ONCE) {
            assertThat(client.getConsumedMessages()).containsAll(toJsonStringList(messages));
        } else {
            assertThat(toJsonStringList(messages)).containsAll(client.getConsumedMessages());
        }
    }

    // --------------- source test cases ---------

    @Test
    public void testTableSource() throws Exception {
        List<TestBean> messages = getRandomRowBeans(100);
        RabbitMQContainerClient<String> client =
                new RabbitMQContainerClient<>(rabbitMq, new SimpleStringSchema(), messages.size());
        String queue = client.createQueue(false);
        client.sendMessages(new SimpleStringSchema(), toJsonStringList(messages));
        String ddl = getTableDDL(queue, consistencyMode, rabbitMq);
        tEnv.executeSql(ddl);
        Table table = tEnv.sqlQuery(selectAll);
        DataStream<RowData> result = tEnv.toAppendStream(table, RowData.class);
        TestingSinkFunction sink = new TestingSinkFunction(messages.size(), consistencyMode);
        result.addSink(sink).setParallelism(1);
        executeWithSuccessExceptionDetection(sEnv);
    }
}
