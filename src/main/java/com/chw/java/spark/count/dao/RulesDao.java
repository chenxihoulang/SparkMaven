package com.chw.java.spark.count.dao;

import java.util.List;

import com.chw.java.spark.count.entity.Rule;
import com.chw.java.spark.count.util.MysqlUtils;

public class RulesDao {
    public static List<Rule> getGameRules() {
        return MysqlUtils.queryByBeanListHandler("select * from rules where state=0;", Rule.class);
    }
}
