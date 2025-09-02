package com.compartamos;

public class AppKafkaProducerL {
    public static void main(String[] args) {
        String topic = "test-topic";

        // Configuración del producer
        java.util.Properties props = new java.util.Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Seguridad SASL
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user1\" password=\"pass1\";");

        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props)) {
            for (int i = 1; i <= 5; i++) {
                String message = "Mensaje Kafka #" + i;
                org.apache.kafka.clients.producer.ProducerRecord<String, String> record = new org.apache.kafka.clients.producer.ProducerRecord<>(topic, message);
                producer.send(record);
                System.out.println("Enviado: " + message);
            }
            System.out.println("Todos los mensajes fueron enviados.");
        }
    }
}