package com.compartamos;

import java.util.*;

public class AppRabbitObjL {
    public static void main(String[] args) {
        try {
            String host = "localhost";
            int port = 5672;
            String user = "guest";
            String password = "guest";

            try (com.rabbitmq.client.Connection connection = com.compartamos.controller.ConnectRabbitmq.createConnection2(host, port, user, password)) {
                System.out.println("Conexión establecida con éxito.");

                com.compartamos.conf.RabbitConfig config = new com.compartamos.conf.RabbitConfig("biometria.queue", "biometria.exchange", "biometria.routingkey");

                // Aquí llega tu SDT de GeneXus transformado a lista de pares (Descripcion, Valor)
                java.util.List<Map<String, String>> sdt = new java.util.ArrayList<>();

                // Ejemplo de prueba (solo si quieres simular en Java)
                java.util.Map<String, String> campo1 = new java.util.HashMap<>();
                campo1.put("Descripcion", "Canal");
                campo1.put("Valor", "AGENCIA");
                sdt.add(campo1);

                // Llamada directa al método que publica dinámicamente en JSON
                com.compartamos.controller.ConnectRabbitmq.publishFromDynamicSDT(connection, config, sdt);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
