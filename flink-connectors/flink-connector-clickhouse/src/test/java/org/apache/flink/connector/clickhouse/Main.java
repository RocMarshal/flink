package org.apache.flink.connector.clickhouse;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public class Main {

    public static final Logger LOG = LoggerFactory.getLogger(Main.class);

    protected static final Schema TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("pid", DataTypes.BIGINT().notNull())
                    .column("col_bigint", DataTypes.BIGINT())
                    .column("col_bigint_unsigned", DataTypes.DECIMAL(20, 0))
                    .column("col_binary", DataTypes.BYTES())
                    .column("col_bit", DataTypes.BOOLEAN())
                    .column("col_blob", DataTypes.BYTES())
                    .column("col_char", DataTypes.STRING())
                    .column("col_date", DataTypes.DATE())
                    .column("col_datetime", DataTypes.TIMESTAMP(0))
                    .column("col_decimal", DataTypes.DECIMAL(10, 0))
                    .column("col_decimal_unsigned", DataTypes.DECIMAL(10, 0))
                    .column("col_double", DataTypes.DOUBLE())
                    .column("col_double_unsigned", DataTypes.DOUBLE())
                    .column("col_enum", DataTypes.STRING())
                    .column("col_float", DataTypes.FLOAT())
                    .column("col_float_unsigned", DataTypes.FLOAT())
                    .column("col_int", DataTypes.INT())
                    .column("col_int_unsigned", DataTypes.BIGINT())
                    .column("col_integer", DataTypes.INT())
                    .column("col_integer_unsigned", DataTypes.BIGINT())
                    .column("col_json", DataTypes.STRING())
                    .column("col_longblob", DataTypes.BYTES())
                    .column("col_longtext", DataTypes.STRING())
                    .column("col_mediumblob", DataTypes.BYTES())
                    .column("col_mediumint", DataTypes.INT())
                    .column("col_mediumint_unsigned", DataTypes.INT())
                    .column("col_mediumtext", DataTypes.STRING())
                    .column("col_numeric", DataTypes.DECIMAL(10, 0))
                    .column("col_numeric_unsigned", DataTypes.DECIMAL(10, 0))
                    .column("col_real", DataTypes.DOUBLE())
                    .column("col_real_unsigned", DataTypes.DOUBLE())
                    .column("col_set", DataTypes.STRING())
                    .column("col_smallint", DataTypes.SMALLINT())
                    .column("col_smallint_unsigned", DataTypes.INT())
                    .column("col_text", DataTypes.STRING())
                    .column("col_time", DataTypes.TIME(0))
                    .column("col_timestamp", DataTypes.TIMESTAMP(0))
                    .column("col_tinytext", DataTypes.STRING())
                    .column("col_tinyint", DataTypes.TINYINT())
                    .column("col_tinyint_unsinged", DataTypes.TINYINT())
                    .column("col_tinyblob", DataTypes.BYTES())
                    .column("col_varchar", DataTypes.STRING())
                    .column("col_datetime_p3", DataTypes.TIMESTAMP(3).notNull())
                    .column("col_time_p3", DataTypes.TIME(3))
                    .column("col_timestamp_p3", DataTypes.TIMESTAMP(3))
                    .column("col_varbinary", DataTypes.BYTES())
                    .primaryKeyNamed("PRIMARY", Lists.newArrayList("pid"))
                    .build();
    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("yandex/clickhouse-server:18.10.3");
    public static final ClickHouseContainer CLICKHOUSE_CONTAINER =
            (ClickHouseContainer)
                    new ClickHouseContainer(DEFAULT_IMAGE_NAME)
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static String baseUrl;

    @Rule public ExpectedException exception = ExpectedException.none();

    @BeforeClass
    public static void launchContainer() {
        CLICKHOUSE_CONTAINER.start();
        baseUrl =
                CLICKHOUSE_CONTAINER
                        .getJdbcUrl()
                        .substring(0, CLICKHOUSE_CONTAINER.getJdbcUrl().lastIndexOf("/"));
        //        catalog = new MySQLCatalog(TEST_CATALOG_NAME, TEST_DB, TEST_USERNAME, TEST_PWD,
        // baseUrl);
    }

    @AfterClass
    public static void shutdownContainer() {
        CLICKHOUSE_CONTAINER.stop();
        CLICKHOUSE_CONTAINER.close();
    }

    @Test
    public void go() {
        System.out.println(baseUrl);
    }
}
