package org.apache.flink.sql.rabbitmq.connector;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.sql.rabbitmq.connector.source.RabbitmqDynamicTableSource;

import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;


import java.util.HashSet;
import java.util.Set;

public class RabbitmqTableSourceFactory implements DynamicTableSourceFactory {

    private static final String FACTORY_IDENTIFIER = "rabbitmq";

    public static final ConfigOption<String> QUEUE = ConfigOptions.key("queue")
            .stringType()
            .noDefaultValue();


    public static final ConfigOption<String> EXCHANGE_NAME = ConfigOptions.key("exchange-name")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
            .intType()
            .noDefaultValue();
    public static final ConfigOption<Integer> QOS = ConfigOptions.key("qos")
            .intType()
            .defaultValue(100);

    public static final ConfigOption<String> HOSTS = ConfigOptions.key("hosts")
            .stringType()
            .noDefaultValue();


    public static final ConfigOption<String> VIRTUAL_HOST = ConfigOptions.key("virtual-host")
            .stringType()
            .noDefaultValue();


    public static final ConfigOption<String> USERNAME = ConfigOptions.key("username")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password")
            .stringType()
            .noDefaultValue();
    public static final ConfigOption<String> FORMAT = ConfigOptions.key("format")
            .stringType()
            .noDefaultValue();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // either implement your custom validation logic here ...
        // or use the provided helper utility
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);


        // validate all options
        helper.validate();

        // get the validated options
        final ReadableConfig options = helper.getOptions();

        final int port = options.get(PORT);
        final String hosts = options.get(HOSTS);
        final String virtualHost = options.get(VIRTUAL_HOST);
        final String useName = options.get(USERNAME);
        final String password = options.get(PASSWORD);
        final String exchangeName = options.get(EXCHANGE_NAME);
        final String queue = options.get(QUEUE);
        final int qos = options.get(QOS);
        final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
                DeserializationFormatFactory.class,
                FactoryUtil.FORMAT);
        final DataType dataType =
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();
        return new RabbitmqDynamicTableSource(hosts, port, virtualHost, useName, password, queue, exchangeName, qos, decodingFormat, dataType);
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTS);
        options.add(PORT);
        options.add(QUEUE);
        options.add(VIRTUAL_HOST);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(EXCHANGE_NAME);
        options.add(FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(QOS);
        return options;
    }
}
