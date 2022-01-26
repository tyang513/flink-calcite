package com.talkingdata.framework.flink;

import com.talkingdata.calcite.sql.parser.impl.CustomSqlParserImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * @author tao.yang
 * @date 2021-12-21
 */
public class SqlCreateFunctionMain {

    public static void main(String[] args) throws SqlParseException {
        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig = SqlParser.configBuilder()
                // 定义解析工厂
                .setParserFactory(CustomSqlParserImpl.FACTORY)
                .setLex(Lex.MYSQL)
                .build();
        // Sql语句
        String sql = "create function " +
                "hr.custom_function as 'com.talkingdata.calcite.function.CustomFunction' " +
                "method 'eval'  " +
                "property ('a'='b','c'='1') comment 'asdfadfs'";
        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);

        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql);
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
