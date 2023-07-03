package org.apache.flink.sql.rabbitmq.connector.functions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;


public class RabbitmqSourceFunction extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData> {
    private transient Connection conn;
    private String userName;
    private String password;
    private String virtualHost;
    private String hostName;
    private int portNumber;
    private String queueName;
    private String exchangeName;
    private int qos;

    private volatile boolean isRunning = true;
    private DeserializationSchema<RowData> deserializationSchema;

    public RabbitmqSourceFunction(String hostName, int port, String virtualHost, String userName, String password, String queueName, String exchangeName, int qos, DeserializationSchema<RowData> deserializationSchema) {
        this.userName = userName;
        this.password = password;
        this.virtualHost = virtualHost;
        this.hostName = hostName;
        this.portNumber = port;
        this.queueName = queueName;
        this.exchangeName = exchangeName;
        this.qos = qos;
        this.deserializationSchema = deserializationSchema;

    }


    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        int subtaskId = 0;
        subtaskId = getRuntimeContext().getIndexOfThisSubtask() + 1;
        Channel channel = conn.createChannel(Math.abs(subtaskId));
        channel.queueBind(queueName, exchangeName, "#");
        channel.basicQos(qos);


        while (true) {
            GetResponse getResponse = channel.basicGet(queueName, true);
            if (getResponse != null) {

                RowData rowData = deserializationSchema.deserialize(getResponse.getBody());
                ctx.collect(rowData);
            } else {
                String json = "{\n" +
                        "  \"col1\": \"000000000\",\n" +
                        "  \"col2\": \"000000000\"\n" +
                        "}";
                RowData rowData = deserializationSchema.deserialize(json.getBytes());
                ctx.collect(rowData);
            }

        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(userName);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        factory.setHost(hostName);
        factory.setPort(portNumber);
        conn = factory.newConnection();


    }

    @Override
    public void cancel() {
        isRunning = false;

    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}
