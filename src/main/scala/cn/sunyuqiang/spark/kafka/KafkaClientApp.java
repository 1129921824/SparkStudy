package cn.sunyuqiang.spark.kafka;


public class KafkaClientApp {
    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.Topic).start();
        new KafkaConsumer(KafkaProperties.Topic).start();
    }
}
