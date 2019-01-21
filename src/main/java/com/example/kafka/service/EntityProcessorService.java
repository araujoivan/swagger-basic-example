package com.example.kafka.service;

public interface EntityProcessorService {    
    public String getJsonCollectionsString(Integer id);
    public String getJsonCollectionString(Integer id, String collectionName, String prefix);
}
