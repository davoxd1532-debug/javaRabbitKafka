package com.compartamos.controller;

import com.compartamos.conf.ConfigServer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.w3c.dom.*;
import org.json.JSONObject;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConnectKafka {

    private final ConfigServer configServer;
    private final KafkaProducer<String, String> producer;

    public ConnectKafka(ConfigServer configServer) {
        this.configServer = configServer;
        this.producer = createProducer();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();

        // Construir bootstrapServers dinámicamente
        String bootstrapServers = configServer.getHost() + ":" + configServer.getPort();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Si hay usuario/contraseña → habilitar SASL
        if (configServer.getUser() != null && !configServer.getUser().isEmpty()
                && configServer.getPass() != null && !configServer.getPass().isEmpty()) {

            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("sasl.mechanism", "PLAIN");
            props.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"" + configServer.getUser() + "\" "
                            + "password=\"" + configServer.getPass() + "\";");
        }

        return new KafkaProducer<>(props);
    }

    /**
     * Envía un SDT representado como List<Map<String, String>> al topic indicado,
     * pero lo convierte en un único JSON tipo {"campo":"valor", ...}
     */
    public void sendSDT(String topic, List<Map<String, String>> sdt) {
        try {
            // Convertir la lista de mapas [{"Nombre":"X","Valor":"Y"}, ...]
            // en un único mapa { "X":"Y", ... }
            Map<String, String> flatMap = new java.util.HashMap<>();
            for (Map<String, String> item : sdt) {
                String key = item.get("Nombre");
                String value = item.get("Valor");
                if (key != null && value != null) {
                    flatMap.put(key, value);
                }
            }

            // Serializar como un único objeto JSON
            ObjectMapper mapper = new ObjectMapper();
            String jsonMessage = mapper.writeValueAsString(flatMap);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonMessage);
            producer.send(record);

            System.out.println("Enviado al topic [" + topic + "]: " + jsonMessage);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Envía un mensaje de texto simple
     */
    public void sendMessage(String topic, String message) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            producer.send(record);
            System.out.println("Enviado al topic [" + topic + "]: " + message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Envía un XML en formato String al topic, transformándolo en JSON plano.
     * Ejemplo XML esperado:
     *
     * <root>
     *   <RngParm.it>
     *     <Nombre>Canal</Nombre>
     *     <Valor>AGENCIA</Valor>
     *   </RngParm.it>
     *   <RngParm.it>
     *     <Nombre>Monto</Nombre>
     *     <Valor>1000</Valor>
     *   </RngParm.it>
     * </root>
     *
     * Se convertirá en:
     * {"Canal":"AGENCIA","Monto":"1000"}
     */
    public void sendFromXml(String topic, String xml) {
        try {
            JSONObject jsonObject = new JSONObject();

            // Parsear el XML
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(new java.io.ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
            doc.getDocumentElement().normalize();

            NodeList items = doc.getElementsByTagName("RngParm.it");

            for (int i = 0; i < items.getLength(); i++) {
                Node node = items.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    String nombre = element.getElementsByTagName("Nombre").item(0).getTextContent();
                    String valor  = element.getElementsByTagName("Valor").item(0).getTextContent();
                    jsonObject.put(nombre, valor);
                }
            }

            // Enviar al topic como JSON
            String jsonMessage = jsonObject.toString();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonMessage);
            producer.send(record);

            System.out.println("XML convertido y enviado al topic [" + topic + "]: " + jsonMessage);

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error al procesar y enviar XML: " + e.getMessage());
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
