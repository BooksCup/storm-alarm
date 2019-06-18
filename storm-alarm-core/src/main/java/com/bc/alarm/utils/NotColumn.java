package com.bc.alarm.utils;

/**
 * Created by steven on 2018/5/21.
 */
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NotColumn {
    String text() default "不是属性字段";
}
