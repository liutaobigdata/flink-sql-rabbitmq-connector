package org.apache.flink.sql.rabbitmq.connector.source;


import org.apache.flink.sql.rabbitmq.connector.functions.RabbitmqSourceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;

import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;

import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

public class RabbitmqDynamicTableSource implements ScanTableSource {

    private final String hostname;
    private final String virtualHost;
    private final String userName;
    private final String password;
    private final String queue;
    private final String exchangeName;
    private final int qos;
    private final DataType dataType;


    private final int port;

    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;


    public RabbitmqDynamicTableSource(String hostname,
                                      int port,
                                      String virtualHost,
                                      String useName,
                                      String password,
                                      String queue,
                                      String exchangeName,
                                      int qos,
                                      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
                                      DataType dataType) {
        this.hostname = hostname;
        this.port = port;
        this.virtualHost = virtualHost;
        this.userName = useName;
        this.password = password;
        this.queue = queue;
        this.exchangeName = exchangeName;
        this.qos = qos;
        this.decodingFormat = decodingFormat;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // define that this format can produce INSERT and DELETE rows
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {

        final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(scanContext, dataType);
        final SourceFunction<RowData> sourceFunction = new RabbitmqSourceFunction(
                hostname,
                port,
                virtualHost,
                userName,
                password,
                queue,
                exchangeName,
                qos, deserializer);


        return SourceFunctionProvider.of(sourceFunction, false);
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "rabbitmq table source";
    }
}
