package com.bc.alarm.utils;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class MongoDbUtils {

    /**
     * 日志
     */
    private static final Log logger = LogFactory.getLog(MongoDbUtils.class);

    MongoClient mongoClient;
    MongoDatabase mongoDatabase;

    private MongoDbUtils() {
        String host = ConfigUtils.getProperty("host", "");
        int port = Integer.valueOf(ConfigUtils.getProperty("port", "27017"));
        String database = ConfigUtils.getProperty("database", "");
        String userName = ConfigUtils.getProperty("userName", "");
        String password = ConfigUtils.getProperty("password", "");
        Boolean auth = ConfigUtils.getBooleanProperty("auth", false);
        logger.info("MongoDb auth: " + auth);

        List<ServerAddress> serverAddrList = new ArrayList<>();
        ServerAddress serverAddress = new ServerAddress(host, port);
        serverAddrList.add(serverAddress);

        if (auth) {
            // 开启认证
            List<MongoCredential> credentialList = new ArrayList<>();
            MongoCredential credential = MongoCredential.createCredential(userName, database, password.toCharArray());
            credentialList.add(credential);
            mongoClient = new MongoClient(serverAddrList, credentialList);
        } else {

            mongoClient = new MongoClient(serverAddrList);
        }
        mongoDatabase = mongoClient.getDatabase(database);
    }

    private volatile static MongoDbUtils instance = null;

    public static MongoDbUtils getInstance() {
        if (null == instance) {
            synchronized (MongoDbUtils.class) {
                if (null == instance) {
                    instance = new MongoDbUtils();
                }
            }
        }
        return instance;
    }

    /**
     * 插入单个对象
     *
     * @param collectionName 集合名
     * @param object         对象
     * @param <T>            泛型
     */
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

    /**
     * 批量插入对象
     *
     * @param collectionName 集合名
     * @param objectList     对象列表
     * @param <T>            泛型
     */
    public <T> void insertMany(String collectionName, List<T> objectList) {
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        try {
            List<Document> documentList = beanListToDocList(objectList);
            collection.insertMany(documentList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 修改单个对象
     *
     * @param collectionName 集合名
     * @param filter         查询过滤
     * @param obj            修改后的对象
     * @return 修改文档数
     */
    public long updateOne(String collectionName, Bson filter, Object obj) {
        try {
            UpdateResult result = mongoDatabase.getCollection(collectionName).updateOne(filter,
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

    /**
     * 查询数据
     *
     * @param collectionName 集合名
     * @param filter         查询过滤
     * @param clazz          clazz
     * @param <T>            泛型
     * @return 查询出的数据列表
     */
    public <T> List<T> find(String collectionName, Bson filter, Class<T> clazz) {
        FindIterable<Document> doIterable = mongoDatabase.getCollection(collectionName).find(filter);
        MongoCursor<Document> cursor = doIterable.iterator();
        List<T> objects = docListToBeanList(cursor, clazz);
        return objects;
    }

    /**
     * 查询数据
     *
     * @param collectionName 集合名
     * @param filter         查询过滤
     * @param clazz          clazz
     * @param sort           排序条件
     * @param pageNum        当前页数
     * @param pageSize       分页大小
     * @param <T>            泛型
     * @return 查询出的数据列表
     */
    public <T> List<T> find(String collectionName, Bson filter, Class<T> clazz, Bson sort,
                            Integer pageNum, Integer pageSize) {
        pageNum = (null == pageNum || 0 == pageNum) ? 1 : pageNum;
        pageSize = (null == pageSize || 0 == pageSize) ? 1 : pageSize;
        sort = null == sort ? new BasicDBObject() : sort;
        int skip = (pageNum - 1) * pageSize;

        FindIterable<Document> doIterable = mongoDatabase.getCollection(collectionName).find(filter).
                sort(sort).limit(pageSize).skip(skip);
        MongoCursor<Document> cursor = doIterable.iterator();
        List<T> objects = docListToBeanList(cursor, clazz);
        return objects;
    }

    /**
     * 文档列表转成对象列表
     *
     * @param cursor Mongo游标
     * @param clazz  clazz
     * @param <T>    泛型
     * @return 对象列表
     */
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

    /**
     * 对象列表转成文档列表
     *
     * @param beanList 对象列表
     * @param <T>      泛型
     * @return 文档列表
     */
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

    /**
     * 查询文档数量
     *
     * @param collectionName 集合名
     * @param filter         查询过滤
     * @return 集合内符合查询条件的文档数量
     */
    public long count(String collectionName, Bson filter) {
        return mongoDatabase.getCollection(collectionName).count(filter);
    }

    /**
     * 查询文档数量
     *
     * @param collectionName 集合名
     * @return 集合内所有文档的数量
     */
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
