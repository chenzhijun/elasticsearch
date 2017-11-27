package me.chenzhijun.elasticsearch.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Configuration
public class ESConfig {

    @Bean
    public TransportClient client() throws UnknownHostException {
        InetSocketTransportAddress node = new InetSocketTransportAddress(
                InetAddress.getByName("localhost"),
                9300
        );
        //多集群
//        InetSocketTransportAddress node1 = new InetSocketTransportAddress(
//                InetAddress.getByName("localhost"),
//                8200
//        );
//        InetSocketTransportAddress node2 = new InetSocketTransportAddress(
//                InetAddress.getByName("localhost"),
//                7200
//        );

        Settings settings = Settings.builder().put("cluster.name", "wali").build();
        TransportClient client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(node);
//        client.addTransportAddress(node1);
//        client.addTransportAddress(node2);
        return client;
    }
}
