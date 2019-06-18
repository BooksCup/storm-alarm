package com.bc.alarm.utils;


import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * 将mongo的文档转化为对象将对象转化为mongo文档
 * @author zhou
 */
public class BsonUtil {
    public static <T> List<T> toBeans(List<Document> documents, Class<T> clazz)
            throws IllegalArgumentException, InstantiationException,
            IllegalAccessException, InvocationTargetException {
        List<T> list = new ArrayList<>();
        for (int i = 0; null != documents && i < documents.size(); i++) {
            list.add(toBean(documents.get(i), clazz));
        }
        return list;
    }

    /**
     *
     * @param document
     * @param clazz
     * @param <T>
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    public static <T> T toBean(Document document, Class<T> clazz)
            throws InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        // 声明一个对象
        T obj = clazz.newInstance();
        // 获取所有属性
        Field[] fields = clazz.getDeclaredFields();
        // 获取所有的方法
        Method[] methods = clazz.getMethods();
        /*
         * 查找所有的属性，并通过属性名和数据库字段名通过相等映射
         */
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].getName();
            Column column = fields[i].getAnnotation(Column.class);
            Object bson;
            if (null != column && null != column.name()) {
                bson = document.get(column.name());
            } else if ("id".equals(fieldName)) {
                bson = document.get("_id");
            } else {
                bson = document.get(fieldName);
            }
            if (null == bson) {
                continue;
            } else if (bson instanceof Document) {
                // 如果字段是文档了递归调用
                // 如果是HashMap,这段代码是有问题的,这种写法完全不符合基本法
                // 补一段无可奉告的代码
                if (fields[i].getType().getSimpleName().equalsIgnoreCase("HashMap")) {
                    bson = toMap((Document) bson);
                } else {
                    bson = toBean((Document) bson, fields[i].getType());
                }

            } else if (bson instanceof MongoCollection) {
                // 如果字段是文档集了调用colTOList方法
                bson = colToList(bson, fields[i]);
            }
            for (int j = 0; j < methods.length; j++) {
                // 为对象赋值
                String metdName = methods[j].getName();
                if (equalFieldAndSet(fieldName, metdName)) {
                    methods[j].invoke(obj, bson);
                    break;
                }
            }
        }
        return obj;
    }

    /**
     *
     * @param objs
     * @return
     * @throws IllegalArgumentException
     * @throws SecurityException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws NoSuchFieldException
     */
    public static List<Document> toBsons(List<Object> objs)
            throws IllegalArgumentException, SecurityException,
            IllegalAccessException, InvocationTargetException,
            NoSuchFieldException {
        List<Document> documents = new ArrayList<>();
        for (int i = 0; null != objs && i < objs.size(); i++) {
            documents.add(toBson(objs.get(i)));
        }
        return documents;
    }

    /**
     *
     * @param obj
     * @return
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     * @throws SecurityException
     * @throws NoSuchFieldException
     */
    public static Document toBson(Object obj) throws IllegalArgumentException,
            IllegalAccessException, InvocationTargetException,
            SecurityException, NoSuchFieldException {
        if (null == obj) {
            return null;
        }
        Class<? extends Object> clazz = obj.getClass();
        Document document = new Document();
        Method[] methods = clazz.getDeclaredMethods();
        Field[] fields = clazz.getDeclaredFields();
        for (int i = 0; null != fields && i < fields.length; i++) {
            // 获取列注解内容
            Column column = fields[i].getAnnotation(Column.class);
            // 获取否列
            NotColumn notColumn = fields[i].getAnnotation(NotColumn.class);
            // 对应的文档键值
            String key;
            if (null != column && null != column.name()) {
                // 存在列映射取值
                key = column.name();
            } else if (null != notColumn) {
                // 不是列的情况
                continue;
            } else {
                // 默认情况通过属性名映射
                key = fields[i].getName();
                // 替换id为_id
                if ("id".equals(key)) {
                    key = "_id";
                }
            }
            String fieldName = fields[i].getName();
            /*
             * 获取对象属性值并映射到Document中
             */
            for (int j = 0; null != methods && j < methods.length; j++) {
                String methdName = methods[j].getName();
                if (null != fieldName && equalFieldAndGet(fieldName, methdName)) {
                    // 得到值
                    Object val = methods[j].invoke(obj);
                    if (null == val) {
                        continue;
                    }
                    if (isJavaClass(methods[j].getReturnType())) {
                        if (methods[j].getReturnType().getName()
                                .equals("java.util.List")) {
                            // 列表处理
                            @SuppressWarnings("unchecked")
                            List<Object> list = (List<Object>) val;
                            List<Document> documents = new ArrayList<Document>();
                            for (Object obj1 : list) {
                                documents.add(toBson(obj1));
                            }
                            document.append(key, documents);
                        } else {// 其它对象处理，基本类型
                            document.append(key, val);
                        }
                    } else {// 自定义类型
                        document.append(key, toBson(val));
                    }
                }
            }
        }
        return document;
    }

    /**
     *
     * @param clz
     * @return
     */
    private static boolean isJavaClass(Class<?> clz) {
        return clz != null && clz.getClassLoader() == null;
    }

    /**
     *
     * @param bson
     * @param field
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    private static List<Object> colToList(Object bson, Field field)
            throws InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException {
        // 获取列表的类型
        ParameterizedType pt = (ParameterizedType) field.getGenericType();
        List<Object> objs = new ArrayList<>();
        @SuppressWarnings("unchecked")
        MongoCollection<Document> cols = (MongoCollection<Document>) bson;
        MongoCursor<Document> cursor = cols.find().iterator();
        while (cursor.hasNext()) {
            Document child = cursor.next();
            @SuppressWarnings("rawtypes")
            // 获取元素类型
            Class clz = (Class) pt.getActualTypeArguments()[0];
            @SuppressWarnings("unchecked")
            Object obj = toBean(child, clz);
            System.out.println(child);
            objs.add(obj);

        }
        return objs;
    }

    /**
     *
     * @param field
     * @param name
     * @return
     */
    private static boolean equalFieldAndSet(String field, String name) {
        if (name.toLowerCase().matches("set" + field.toLowerCase())) {
            return true;
        } else {
            return false;
        }
    }

    /*
     * 比较getter方法和属性相等
     */
    private static boolean equalFieldAndGet(String field, String name) {
        if (name.toLowerCase().matches("get" + field.toLowerCase())) {
            return true;
        } else {
            return false;
        }
    }

    public static HashMap toMap(Document bson) {
        HashMap hashMap = new HashMap();
        for (String key : bson.keySet()) {
            hashMap.put(key, bson.get(key));
        }
        return hashMap;
    }
}
