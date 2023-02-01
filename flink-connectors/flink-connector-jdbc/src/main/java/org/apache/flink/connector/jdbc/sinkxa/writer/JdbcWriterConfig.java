package org.apache.flink.connector.jdbc.sinkxa.writer;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExactlyOnceOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** JdbcWriterConfig. */
public class JdbcWriterConfig implements Serializable {

    private final JdbcExecutionOptions jdbcFlushOptions;
    private final JdbcExactlyOnceOptions jdbcExactlyOnceOptions;
    private final DeliveryGuarantee deliveryGuarantee;

    private final JdbcConnectionOptions jdbcConnectionOptions;

    public JdbcExecutionOptions getJdbcFlushOptions() {
        return jdbcFlushOptions;
    }

    public JdbcExactlyOnceOptions getJdbcExactlyOnceOptions() {
        return jdbcExactlyOnceOptions;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    public JdbcWriterConfig(
            JdbcExecutionOptions jdbcFlushOptions,
            JdbcExactlyOnceOptions jdbcExactlyOnceOptions,
            DeliveryGuarantee deliveryGuarantee,
            JdbcConnectionOptions jdbcConnectionOptions) {
        this.jdbcFlushOptions = jdbcFlushOptions;
        this.jdbcExactlyOnceOptions = jdbcExactlyOnceOptions;
        this.deliveryGuarantee = deliveryGuarantee;
        this.jdbcConnectionOptions = jdbcConnectionOptions;
    }

    public JdbcConnectionOptions getJdbcConnectionOptions() {
        return jdbcConnectionOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof JdbcWriterConfig)) {
            return false;
        }
        JdbcWriterConfig that = (JdbcWriterConfig) o;
        return Objects.equals(getJdbcFlushOptions(), that.getJdbcFlushOptions())
                && Objects.equals(getJdbcExactlyOnceOptions(), that.getJdbcExactlyOnceOptions())
                && getDeliveryGuarantee() == that.getDeliveryGuarantee()
                && Objects.equals(getJdbcConnectionOptions(), that.getJdbcConnectionOptions());
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                getJdbcFlushOptions(),
                getJdbcExactlyOnceOptions(),
                getDeliveryGuarantee(),
                getJdbcConnectionOptions());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static JdbcWriterConfig defaults() {
        return builder().build();
    }

    /** Builder for {@link JdbcWriterConfig}. */
    public static final class Builder {
        private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
        private JdbcExecutionOptions jdbcFlushOptions = JdbcExecutionOptions.builder().build();
        private JdbcExactlyOnceOptions jdbcExactlyOnceOptions = null;

        private JdbcConnectionOptions jdbcConnectionOptions;

        public Builder setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
            this.deliveryGuarantee = deliveryGuarantee;
            return this;
        }

        public Builder setJdbcExecutionOptions(JdbcExecutionOptions jdbcFlushOptions) {
            this.jdbcFlushOptions = jdbcFlushOptions;
            return this;
        }

        public Builder setJdbcExactlyOnceOptions(JdbcExactlyOnceOptions jdbcExactlyOnceOptions) {
            this.jdbcExactlyOnceOptions = jdbcExactlyOnceOptions;
            return this;
        }

        public Builder setJdbcConnectionOptions(JdbcConnectionOptions jdbcConnectionOptions) {
            this.jdbcConnectionOptions = jdbcConnectionOptions;
            return this;
        }

        public JdbcWriterConfig build() {
            if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
                Preconditions.checkArgument(
                        jdbcExactlyOnceOptions != null,
                        "JdbcExecutionOptions mustn't be null, please specialize it first.");
                jdbcConnectionOptions.checkSemantic(deliveryGuarantee);
            }
            return new JdbcWriterConfig(
                    jdbcFlushOptions,
                    jdbcExactlyOnceOptions,
                    deliveryGuarantee,
                    jdbcConnectionOptions);
        }
    }
}
