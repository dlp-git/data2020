package com.dlp.www.dwlogger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Random;

@RestController
public class LogJsonController {


//    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger logger = LoggerFactory.getLogger(LogJsonController.class);

    @PostMapping("/log")
    public void getLog(@RequestParam("log") String log) {
        JSONObject logJsonObj = JSON.parseObject(log);
        int randomInt = new Random().nextInt(3600 * 1000 * 5);
        logJsonObj.put("ts", System.currentTimeMillis() + randomInt);
//        sendKafka(logJsonObj);
        String logNew = logJsonObj.toJSONString();
        System.out.println(logNew);
        logger.warn(logNew);

    }


    /*public void sendKafka(JSONObject logJsonObj) {
        if ("startup".equals(logJsonObj.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, logJsonObj.toJSONString());
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, logJsonObj.toJSONString());
        }
    }*/

}
