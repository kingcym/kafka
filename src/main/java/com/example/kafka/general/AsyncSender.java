package com.example.kafka.general;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

/**
 * @Author: Kingcym
 * @Description: 异步发送
 * @Date: 2018/4/17 0:37
 */
@Slf4j
public class AsyncSender {

    public static void main(String[] args) {
        Properties properties = initProp();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        IntStream.range(0, 10).forEach(i -> {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("FireAndForgetSender", ""+i, "hello" + i);
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception==null){
                        log.info("=========发送成功key:{},partition:{},offset:{}================",i,metadata.partition(),metadata.offset());
                    }
                }
            });
        });
        producer.flush();
        System.out.println();
        producer.close();
    }

    /**
     * 1.指定server
     * 2.key和value序列化
     */
    private static Properties initProp() {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "47.98.37.251:9092");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return prop;
    }


}
