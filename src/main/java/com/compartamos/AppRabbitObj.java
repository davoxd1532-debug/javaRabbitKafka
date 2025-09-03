package com.compartamos;

import java.util.*;

import com.compartamos.controller.ConnectRabbitmq;
import com.compartamos.model.Estructura;

public class AppRabbitObj {
    public static void main(String[] args) {
        try {
            String host = "localhost";
            int port = 5672;
            String user = "guest";
            String password = "guest";

            try (com.rabbitmq.client.Connection connection = com.compartamos.controller.ConnectRabbitmq.createConnection2(host, port, user, password)) {
                System.out.println("Conexión establecida con éxito.");

                com.compartamos.conf.RabbitConfig config = new com.compartamos.conf.RabbitConfig("biometria.queue", "biometria.exchange", "biometria.routingkey");
                
                //Definir Estructura
                com.compartamos.model.Estructura estructura = new com.compartamos.model.Estructura();
                estructura.setCanal("AGENCIA");
                estructura.setEmpresa(123);
                estructura.setSucursal(456);
                estructura.setModulo(789);
                estructura.setTransaccion(1011);
                estructura.setRelacion(2021);
                //estructura.setFecha("2024-09-19");
                estructura.setPais(604);
                estructura.setTipoDocumento(1);
                estructura.setNumeroDocumento("ABC123");
                estructura.setTipo("Factura");
                estructura.setSubTipo("Electronica");
                estructura.setHora("12:00:00");
                estructura.setHoraFirma("12:00:05");
                estructura.setWorkstation("WS01");
                estructura.setUsuario("usuario1");
                estructura.setUsuarioSucursal(555);
                estructura.setTrace("20240606180115222-RPUMA-SG2");
                estructura.setAux1("Auxiliar1");
                estructura.setAux2("Auxiliar2");
                estructura.setAux3("Auxiliar3");
                estructura.setAux4(99.99);

                // Publicar diferentes tipos de mensajes
                //Text
                ConnectRabbitmq.publishText(connection, config, "Hola RabbitMQ!");
                // JSON
                ConnectRabbitmq.publishJson(connection, config, estructura);
                // XML
                ConnectRabbitmq.publishXml(connection, config, estructura);
                // Objeto genérico (usa toString())
                ConnectRabbitmq.publishObject(connection, config, estructura);

            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
