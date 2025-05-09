package com.hanu.sec04;

public class ConsumerGroup {
    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.startConsumer("1");
        }
    }
    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.startConsumer("2");
        }
    }
    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.startConsumer("3");
        }
    }
    private static class Consumer4 {
        public static void main(String[] args) {
            KafkaConsumer.startConsumer("4");
        }
    }
}
