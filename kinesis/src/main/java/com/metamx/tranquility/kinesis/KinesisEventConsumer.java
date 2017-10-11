package com.metamx.tranquility.kinesis;

import java.util.List;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.Record;

public class KinesisEventConsumer implements IRecordProcessorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisEventConsumer.class);
    private Worker.Builder builder;

    public KinesisEventConsumer(String region, String streamName, String appName, String initialPosition) {

        InitialPositionInStream position = InitialPositionInStream.valueOf(initialPosition);
        
        KinesisClientLibConfiguration clientConfig = new KinesisClientLibConfiguration(appName, streamName,
                new DefaultAWSCredentialsProviderChain(), appName)
                        .withRegionName(region)
                        .withInitialPositionInStream(position);
        
        this.builder = new Worker.Builder().recordProcessorFactory(this).config(clientConfig);
    }
    
    public void start(){
        this.builder.build().run();
    }

    @Override
    public IRecordProcessor createProcessor() {
        LOGGER.debug("Creating recordProcessor.");
        return new IRecordProcessor() {
            @Override
            public void initialize(String shardId) {}

            @Override
            public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {
                for (Record record : records){ 
                    byte[] bytes = new byte[record.getData().remaining()]; 
                    record.getData().get(bytes);
                    String data = new String(bytes);
                    LOGGER.debug("Received [{}]", data);
                }
            }

            @Override
            public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
                LOGGER.debug("Shutting down [{}]");
            }
        };

    }
}
