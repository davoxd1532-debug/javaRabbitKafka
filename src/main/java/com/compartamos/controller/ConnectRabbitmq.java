package com.compartamos.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import java.nio.charset.StandardCharsets;

import com.compartamos.conf.ConfigServer;
import com.compartamos.conf.RabbitConfig;
import com.compartamos.model.Estructura;

import org.json.JSONArray;

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
    
    // ===================== MÉTODOS DE PUBLICACIÓN =====================

    // Publicar texto plano
    public static void publishText(Connection connection, RabbitConfig config, String message) throws Exception {
        publish(connection, config, message);
    }

    // Publicar JSON (cualquier objeto se serializa a JSON)
    public static void publishJson(Connection connection, RabbitConfig config, Object data) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonMessage = objectMapper.writeValueAsString(data);
        publish(connection, config, jsonMessage);
    }

    // Publicar XML (cualquier objeto se serializa a XML)
    public static void publishXml(Connection connection, RabbitConfig config, Object data) throws Exception {
        XmlMapper xmlMapper = new XmlMapper();
        String xmlMessage = xmlMapper.writeValueAsString(data);
        publish(connection, config, xmlMessage);
    }

    // Publicar JSONArray (org.json)
    public static void publishJsonArray(Connection connection, RabbitConfig config, JSONArray jsonArray) throws Exception {
        String jsonArrayMessage = jsonArray.toString(); // convierte a JSON string
        publish(connection, config, jsonArrayMessage);
    }

    // Publicar JSONArray desde String (se convierte a JSONArray internamente)
    public static void publishJsonArrayFromString(Connection connection, RabbitConfig config, String jsonString) throws Exception {
        JSONArray jsonArray = new JSONArray(jsonString);
        publishJsonArray(connection, config, jsonArray);
    }

    // Publicar Objeto (simplemente lo convierte a String usando toString())
    public static void publishObject(Connection connection, RabbitConfig config, Object obj) throws Exception {
        String objMessage = obj.toString();
        publish(connection, config, objMessage);
    }

    // ===================== MÉTODO BASE =====================
    private static void publish(Connection connection, RabbitConfig config, String message) throws Exception {
        Channel channel = null;
        try {
            channel = connection.createChannel();

            // Declarar recursos de RabbitMQ
            channel.queueDeclare(config.getQueue(), true, false, false, null);
            channel.exchangeDeclare(config.getExchange(), "direct", true);
            channel.queueBind(config.getQueue(), config.getExchange(), config.getRoutingKey());

            // Publicar mensaje
            channel.basicPublish(config.getExchange(), config.getRoutingKey(), null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("Mensaje publicado: " + message);

        } catch (Exception e) {
            System.err.println("Error al publicar mensaje: " + e.getMessage());
            throw e;
        } finally {
            if (channel != null) {
                try {
                    channel.close();
                } catch (Exception e) {
                    System.err.println("Error al cerrar canal: " + e.getMessage());
                }
            }
        }
    }
}
