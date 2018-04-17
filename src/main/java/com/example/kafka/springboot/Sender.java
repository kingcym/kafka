package com.example.kafka.springboot;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

/**
 * @Author: Kingcym
 * @Description:
 * @Date: 2018/4/17 16:17
 */
@Slf4j
@RestController
public class Sender {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    @GetMapping("/send")
    public void kafkaSender(){
        kafkaTemplate.send("FireAndForgetSender","2","hhhhhh");
        log.info("=======发送成功==========");
    }

    @GetMapping("/send2")
    public void kafkaSender2(){
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("FireAndForgetSender","2","hhhhhh");
        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("=======发送成功:{}==========",result.getProducerRecord().toString());
                log.info("=======发送成功:{}==========",result.getRecordMetadata().toString());

            }

            @Override
            public void onFailure(Throwable ex) {
                log.info("=======发送失败==========");

            }

        });
    }

    @GetMapping("/send3")
    public void kafkaSender3() throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("FireAndForgetSender","2","hhhhhh");
        SendResult<String, String> sendResult = future.get();
        log.info("======================发送成功:{}=============",sendResult.getRecordMetadata().offset());

    }


    @KafkaListener(topics = "FireAndForgetSender")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        log.info("======================接收:{}=============",cr.toString());
    }

}
