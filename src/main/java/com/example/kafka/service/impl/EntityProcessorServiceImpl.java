package com.example.kafka.service.impl;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.lang.reflect.Field;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.example.kafka.configuration.FieldsConfig;

import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
//import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.stereotype.Service;

import com.example.kafka.service.EntityProcessorService;

import org.springframework.beans.factory.annotation.Autowired;

@Service
public class EntityProcessorServiceImpl implements EntityProcessorService {

    @Autowired
    private FieldsConfig aggregateConfig;

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public String getJsonCollectionsString(Integer id) {

        Map<String, String> collectionMap = getJsonCollectionMap(id);

        final StringBuilder json = new StringBuilder("[");

        collectionMap.entrySet().forEach(entry -> {
            json.append("{\n\"")
                    .append(entry.getKey())
                    .append("\":")
                    .append(entry.getValue())
                    .append("},\n");
        });

        json.replace(json.lastIndexOf(","), json.lastIndexOf(",") + 1, "]");

        return json.toString();

    }
    
    @Override
    public String getJsonCollectionString(Integer id, String collectionName, String prefix) {

        Map<String, String> collectionMap = getJsonCollectionMap(id);
        
        String jsonString = collectionMap.get(collectionName);
        
        if(nonNull(jsonString)) {
            
            final StringBuilder json = new StringBuilder("{\n\"message\":\"");
            
            json.append(jsonString.replaceAll("\n", "")).append("\",\n\"topic\": \"")
                    .append(prefix)
                    .append(collectionName.toLowerCase())
                    .append("\"\n}");
            
            return json.toString();
        }

        return "{\"error\" : \"Collection not found! Maybe you need to change the shared package\"}";
    }

    private Map getJsonCollectionMap(Integer id) {

        Map<String, String> collectionsMap = new HashMap();

        final HashSet classes = getEntityClasses();

        final Iterator iterator = classes.iterator();

        /*while (iterator.hasNext()) {

            final Class classe = (Class) iterator.next();

            final Document documentAnnotation = (Document) classe.getAnnotation(Document.class);

            if (nonNull(documentAnnotation)) {

                final StringBuilder jsonString = new StringBuilder("{\n");

                for (Field field : classe.getDeclaredFields()) {

                    org.springframework.data.mongodb.core.mapping.Field mongoField = field.getAnnotation(org.springframework.data.mongodb.core.mapping.Field.class);

                    if (nonNull(mongoField)) {
                        final String jsonField = formatField(documentAnnotation.collection(), mongoField.value(), id);

                        if (!jsonField.isEmpty()) {
                            jsonString.append(jsonField);
                            jsonString.append(", \n");
                        }
                    }
                }

                jsonString.replace(jsonString.lastIndexOf(","), jsonString.lastIndexOf(",") + 1, "}\n");
                collectionsMap.put(documentAnnotation.collection(), jsonString.toString());
            }
        }*/

        return collectionsMap;
    }

    private HashSet getEntityClasses() {

        final List<ClassLoader> classLoadersList = new LinkedList();

        classLoadersList.add(ClasspathHelper.contextClassLoader());
        classLoadersList.add(ClasspathHelper.staticClassLoader());

        final Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setScanners(new SubTypesScanner(false), new ResourcesScanner())
                .setUrls(ClasspathHelper.forClassLoader(classLoadersList.toArray(new ClassLoader[0])))
                .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix("br.com.integracaolegado.shared.entity.receiver"))));

        return null; //(HashSet) reflections.getSubTypesOf(Entity.class);
    }

    private String formatField(String collectionName, String fieldName, Integer id) {

        final StringBuilder field = new StringBuilder("");

        if (!fieldName.equals("_id")) {

            String fieldValue = getAggregateField(collectionName, fieldName);

            if (isNull(fieldValue)) {

                fieldValue = isDateField(fieldName) ? LocalDate.now()
                        .format(dateFormatter) : id.toString();
            }
            
            field.append("\"").append(fieldName).append("\":").append("\"").append(fieldValue).append("\"");
        //    field.append(fieldName).append(":").append("'").append(fieldValue).append("'");
        }

        return field.toString();
    }

    private String getAggregateField(String collectionName, String fieldName) {
        return aggregateConfig.getFieldsMap()
                .get(collectionName.concat(".")
                        .concat(fieldName));
    }

    private boolean isDateField(String fieldName) {
        return fieldName.toUpperCase().contains("DT_")
                || fieldName.toUpperCase().contains("DATA")
                || fieldName.equals("AUD_APPLY_TIMESTAMP");
    }
}
