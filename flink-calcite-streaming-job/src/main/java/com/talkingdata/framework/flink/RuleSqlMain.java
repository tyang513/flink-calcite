package com.talkingdata.framework.flink;

import com.talkingdata.flink.calcite.sql.parser.impl.RuleSqlParserImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.OracleSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * @author tao.yang
 * @date 2022-01-26
 */
public class RuleSqlMain {
    public static void main(String[] args) throws SqlParseException {
        // 解析配置 - mysql设置
        SqlParser.Config mysqlConfig = SqlParser.configBuilder()
                .setParserFactory(RuleSqlParserImpl.FACTORY)
                .setLex(Lex.MYSQL)
                .build();
        // Sql语句
        String sql = "rule name '网关-每分钟5XX占比大于10%告警'" +
                "namespace 'gateway' " +
                "event 'gateway'";

        // 创建解析器
        SqlParser parser = SqlParser.create(sql, mysqlConfig);
        // 解析sql
        SqlNode sqlNode = parser.parseQuery(sql);
        // 还原某个方言的SQL
        System.out.println(sqlNode.toSqlString(OracleSqlDialect.DEFAULT));
    }
}
