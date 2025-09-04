package com.compartamos;

public class AppKafka {
    public static void main(String[] args) {
        String topic = "test-topic";

        // Caso con usuario y password
        com.compartamos.conf.ConfigServer config = new com.compartamos.conf.ConfigServer("localhost", 29092, "user1", "pass1");

        // Caso sin autenticación
        // ConfigServer config = new ConfigServer("localhost", 29092, "", "");

        com.compartamos.controller.ConnectKafka kafka = new com.compartamos.controller.ConnectKafka(config);

        try {
            // Enviar mensajes simples
            for (int i = 1; i <= 3; i++) {
                kafka.sendMessage(topic, "Mensaje simple #" + i);
            }

            // Enviar un SDT simulado (List<Map<String, String>>)
            java.util.Map<String, String> campo1 = new java.util.HashMap<>();
            campo1.put("Nombre", "Canal");
            campo1.put("Valor", "AGENCIA");

            java.util.Map<String, String> campo2 = new java.util.HashMap<>();
            campo2.put("Nombre", "Monto");
            campo2.put("Valor", "1000");

            kafka.sendSDT(topic, java.util.Arrays.asList(campo1, campo2));
            
            // Enviar un mensaje con formato XML
            String sdtXml = "<RngParm xmlns=\"Microfinanzas\"><RngParm.it><Nombre>empresa</Nombre><Valor>1</Valor></RngParm.it><RngParm.it><Nombre>Canal</Nombre><Valor>Bantotal</Valor></RngParm.it><RngParm.it><Nombre>Monto</Nombre><Valor>1000</Valor></RngParm.it><RngParm.it><Nombre>Sucursal</Nombre><Valor>423</Valor></RngParm.it></RngParm>";
            kafka.sendFromXml(topic, sdtXml);

        } finally {
            kafka.close();
        }
    }
}