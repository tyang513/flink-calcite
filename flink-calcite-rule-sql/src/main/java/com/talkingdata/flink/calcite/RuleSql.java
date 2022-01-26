package com.talkingdata.flink.calcite;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author tao.yang
 * @date 2022-01-26
 */
public class RuleSql extends SqlCall {

    private static final Logger logger = LoggerFactory.getLogger(RuleSql.class);

    private SqlNode name;

    private SqlNode namespace;

    private SqlNode event;

    public RuleSql(SqlParserPos pos, SqlNode name, SqlNode namespace, SqlNode event) {
        super(pos);
        logger.info("=========");
        this.name = name;
        this.namespace = namespace;
        this.event = event;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("RULE");
        if (name != null) {
            writer.newlineAndIndent();
            writer.keyword("NAME");
            String v = ((SqlCharStringLiteral) name).toValue();
            writer.print("'" + v + "'");
        }
        if (namespace != null){
            writer.newlineAndIndent();
            writer.keyword("NAMESPACE");
            String v = ((SqlCharStringLiteral) namespace).toValue();
            writer.print("'" + v + "'");
        }
        if (event != null){
            writer.newlineAndIndent();
            writer.keyword("EVENT");
            String v = ((SqlCharStringLiteral) event).toValue();
            writer.print("'" + v + "'");
        }
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.OTHER_DDL;
    }

    @Override
    public SqlOperator getOperator() {
        return null;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return null;
    }

    public SqlNode getName() {
        return name;
    }

    public void setName(SqlNode name) {
        this.name = name;
    }

    public SqlNode getNamespace() {
        return namespace;
    }

    public void setNamespace(SqlNode namespace) {
        this.namespace = namespace;
    }

    public SqlNode getEvent() {
        return event;
    }

    public void setEvent(SqlNode event) {
        this.event = event;
    }
}
