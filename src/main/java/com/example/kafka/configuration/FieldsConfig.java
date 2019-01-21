package com.example.kafka.configuration;

import static java.util.Objects.nonNull;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

import lombok.Getter;


/**
 *
 * @author macbook
 */
@Getter
@Configuration
public class FieldsConfig {

    private Map<String, String> fieldsMap = new HashMap();

    @PostConstruct
    private void loadFieldsProperties() {
        try {

            final Properties properties = new Properties();

            final InputStream input = getClass().getClassLoader().getResourceAsStream("fields.properties");

            if (nonNull(input)) {
                properties.load(input);
                properties.forEach((k, v) -> fieldsMap.put(k.toString(), v.toString()));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
