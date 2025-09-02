package com.compartamos.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.compartamos.conf.ConfigServer;
import com.compartamos.conf.RabbitConfig;
import com.compartamos.model.Estructura;

public class ConnectRabbitmq {

    // Método para crear la conexión
    public static Connection createConnection(ConfigServer config) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.getHost());
        factory.setPort(config.getPort());
        factory.setUsername(config.getUser());
        factory.setPassword(config.getPass());

        // Crear y devolver la conexión
        return factory.newConnection();
    }

    public static Connection createConnectionSSL(String host, int port, String user, String password) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(password);
        
        // SSL configuration
        factory.useSslProtocol();

        // Create and return the SSL connection
        return factory.newConnection();
    }

    public static Connection createConnectionSSLCert(String host, int port, String user, String password, String trustStorePath, String trustStorePassword) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(password);
        
        // SSL configuration
        factory.useSslProtocol();
        
        // Optional: Configure trust store if using custom SSL certificates
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
    
        // Create and return the SSL connection
        return factory.newConnection();
    }

    public static Connection createConnection2(String host, int port, String user, String password) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(password);

        // Crear y devolver la conexión
        return factory.newConnection();
    }
    
    //Método para cerrar la conexión de forma segura
    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                System.out.println("Conexión cerrada con éxito.");
            } catch (Exception e) {
                System.err.println("Error al cerrar la conexión: " + e.getMessage());
            }
        }
    }

    // Método para publicar un mensaje en formato JSON usando RabbitConfig
    public static void publishMessage(Connection connection, RabbitConfig config, Estructura estructura) throws Exception {
        Channel channel = null;
        try {
            // Crear un canal
            channel = connection.createChannel();
            // Declarar la cola (si no existe, se creará)
            channel.queueDeclare(config.getQueue(), true, false, false, null);
            // Declarar el Exchange (si no existe, se creará)
            channel.exchangeDeclare(config.getExchange(), "direct",true);
            // Hacemos el Binding con el routing key
            channel.queueBind(config.getQueue(), config.getExchange(), config.getRoutingKey());

            // Convertir el objeto Estructura a JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonMessage = objectMapper.writeValueAsString(estructura);

            // Publicar el mensaje en el exchange con la routingKey
            channel.basicPublish(config.getExchange(), config.getRoutingKey(), null, jsonMessage.getBytes("UTF-8"));
            System.out.println("Mensaje JSON enviado a la cola: " + config.getQueue());
        } catch (Exception e) {
            System.err.println("Error al publicar el mensaje: " + e.getMessage());
            throw e;
        } finally {
            // Cerrar el canal después de publicar
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    System.err.println("Error al cerrar el canal: " + e.getMessage());
                }
            }
        }
    }
    
    // ====================================
    // NUEVOS MÉTODOS GENÉRICOS 31.08.2025
    // ====================================

    // 1) Publicar desde un SDT dinámico (Nombre, Valor)
    public static void publishFromDynamicSDT(Connection connection, RabbitConfig config, List<Map<String, String>> sdtData) throws Exception {
        Map<String, Object> jsonMap = new HashMap<>();
        for (Map<String, String> item : sdtData) {
            String key = item.get("Nombre");
            String value = item.get("Valor");
            jsonMap.put(key, value);
        }
        publishAsJson(connection, config, jsonMap);
    }

    // 2) Publicar un objeto cualquiera (Estructura, DTO, etc.)
    public static void publishObject(Connection connection, RabbitConfig config, Object data) throws Exception {
        publishAsJson(connection, config, data);
    }

    // 3) Método genérico interno que convierte a JSON y publica
    private static void publishAsJson(Connection connection, RabbitConfig config, Object data) throws Exception {
        Channel channel = null;
        try {
            channel = connection.createChannel();
            channel.queueDeclare(config.getQueue(), true, false, false, null);
            channel.exchangeDeclare(config.getExchange(), "direct", true);
            channel.queueBind(config.getQueue(), config.getExchange(), config.getRoutingKey());

            ObjectMapper objectMapper = new ObjectMapper();
            String jsonMessage = objectMapper.writeValueAsString(data);

            channel.basicPublish(config.getExchange(), config.getRoutingKey(), null, jsonMessage.getBytes("UTF-8"));
            System.out.println("Mensaje JSON enviado a la cola " + config.getQueue() + ": " + jsonMessage);
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    System.err.println("Error al cerrar el canal: " + e.getMessage());
                }
            }
        }
    }

    //4) Método para probar envio de mensajes transformando el SDT a JSON
    public static void publishFromGenexusJson(Connection connection, RabbitConfig config, String sdtJson) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        // Indicamos explícitamente que queremos Map<String,Object>
        Map<String, Object> jsonMap = objectMapper.readValue(
            sdtJson, 
            new TypeReference<Map<String, Object>>() {}
        );
        publishAsJson(connection, config, jsonMap);
    }

    public static void publishFromGenexusXml(Connection connection, RabbitConfig config, String sdtXml) throws Exception {
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode root = xmlMapper.readTree(sdtXml.getBytes("UTF-8"));

        // ⚡ Mapa donde construiremos el JSON plano
        Map<String, Object> jsonMap = new HashMap<>();

        // 1) Navegar hasta el nodo "Cliente"
        JsonNode clienteNode = root.findPath("Cliente");

        // 2) Iterar cada "RngParm.it"
        Iterator<JsonNode> elements = clienteNode.elements();
        while (elements.hasNext()) {
            JsonNode rngNode = elements.next();

            String key = rngNode.findPath("Nombre").asText();
            String value = rngNode.findPath("Valor").asText();

            if (!key.isEmpty()) {
                jsonMap.put(key, value);
            }
        }

        // 3) Publicar en Rabbit como JSON plano
        publishAsJson(connection, config, jsonMap);
    }
}
