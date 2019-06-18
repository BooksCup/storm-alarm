package com.bc.alarm.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * MongoDB操作工具类
 *
 * @author zhou
 */
public class MongoDBUtils {
    MongoClient mongoClient;
    MongoDatabase mongoDatabase;

    private MongoDBUtils() {
        String host = ConfigUtils.getProperty("host", "");
        int port = Integer.valueOf(ConfigUtils.getProperty("port", "27017"));
        String database = ConfigUtils.getProperty("database", "");
        String userName = ConfigUtils.getProperty("userName", "");
        String password = ConfigUtils.getProperty("password", "");

        List<ServerAddress> serverAddrList = new ArrayList<>();
        ServerAddress serverAddress = new ServerAddress(host, port);
        serverAddrList.add(serverAddress);
//        List<MongoCredential> credentialList = new ArrayList<>();
//        MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
//        credentialList.add(credential);
//        mongoClient = new MongoClient(serverAddrList, credentialList);
        mongoClient = new MongoClient(serverAddrList);
        mongoDatabase = mongoClient.getDatabase(database);
    }

    private volatile static MongoDBUtils instance = null;

    public static MongoDBUtils getInstance() {
        if (null == instance) {
            synchronized (MongoDBUtils.class) {
                if (null == instance) {
                    instance = new MongoDBUtils();
                }
            }
        }
        return instance;
    }

    public <T> void insertOne(String collectionName, T object) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        try {
            collection.insertOne(BsonUtil.toBson(object));
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public <T> void insertMany(String collectionName, List<T> objectList) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        try {
            List<Document> documentList = beanListToDocList(objectList);
            collection.insertMany(documentList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public long updateOne(String collectionName, Bson bson, Object obj) {
        try {
            UpdateResult result = mongoDatabase.getCollection(collectionName).updateOne(bson,
                    new Document("$set", BsonUtil.toBson(obj)));
            return result.getMatchedCount();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public <T> List<T> find(String collectionName, Bson bson, Class<T> clazz) {
        FindIterable<Document> doIterable = mongoDatabase.getCollection(collectionName).find(bson);
        MongoCursor<Document> cursor = doIterable.iterator();
        List<T> objects = docListToBeanList(cursor, clazz);
        return objects;
    }

    public <T> List<T> find(String collectionName, Bson query, Class<T> clazz, Bson sort,
                            Integer pageNum, Integer pageSize) {
        pageNum = (null == pageNum || 0 == pageNum) ? 1 : pageNum;
        pageSize = (null == pageSize || 0 == pageSize) ? 1 : pageSize;
        sort = null == sort ? new BasicDBObject() : sort;
        int skip = (pageNum - 1) * pageSize;

        FindIterable<Document> doIterable = mongoDatabase.getCollection(collectionName).find(query).
                sort(sort).limit(pageSize).skip(skip);
        MongoCursor<Document> cursor = doIterable.iterator();
        List<T> objects = docListToBeanList(cursor, clazz);
        return objects;
    }

    private <T> List<T> docListToBeanList(MongoCursor<Document> cursor, Class<T> clazz) {
        List<T> objects = new ArrayList<>();
        while (cursor.hasNext()) {
            Document document = cursor.next();
            try {
                objects.add(BsonUtil.toBean(document, clazz));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return objects;
    }

    private <T> List<Document> beanListToDocList(List<T> beanList) {
        List<Document> documents = new ArrayList<>();
        try {
            for (int i = 0; null != beanList && i < beanList.size(); i++) {
                documents.add(BsonUtil.toBson(beanList.get(i)));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return documents;
    }


    public long count(String collectionName, Bson query) {
        return mongoDatabase.getCollection(collectionName).count(query);
    }

    public long count(String collectionName) {
        return mongoDatabase.getCollection(collectionName).count();
    }

    /**
     * 删除collection
     *
     * @param collectionName 集合名
     */
    public void dropCollection(String collectionName) {
        mongoDatabase.getCollection(collectionName).drop();
    }

    /**
     * 聚合计算
     *
     * @param collectionName 集合名
     * @param aggregateList  聚合条件
     * @return 聚合结果
     */
    public AggregateIterable<Document> aggregate(String collectionName, List<Document> aggregateList) {
        return mongoDatabase.getCollection(collectionName).aggregate(aggregateList);

    }
}
