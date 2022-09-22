package com.lkd;

import com.lkd.config.GatewayConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@EnableDiscoveryClient
@EnableFeignClients
@EnableConfigurationProperties(GatewayConfig.class)
@SpringBootApplication
public class GatewayApplication{
    public static void main(String[] args) {
        SpringApplication.run( GatewayApplication.class, args);
    }
}
