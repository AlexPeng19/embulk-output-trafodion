package org.embulk.output.trafodion;

import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.embulk.output.jdbc.JdbcColumn;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.MergeConfig;

public class TrafodionOutputConnection
        extends JdbcOutputConnection
{
    public TrafodionOutputConnection(Connection connection, boolean autoCommit)
            throws SQLException
    {
        super(connection,null);
	System.out.println("connection------------------------------------:"+connection);
		//setSearchPath(schema);
        connection.setAutoCommit(autoCommit);
    }
    /*@Override
    public void createTableIfNotExists(String tableName, JdbcSchema schema) throws SQLException
    {
        if (!tableExists(tableName)) {
            createTable(tableName, schema);
        }
    }*/
    /*@Override
    protected void setSearchPath(String schema) throws SQLException {
	Statement stmt = connection.createStatement();
        try {
            String sql = "SET SCHEMA " + schema;
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } finally {
            stmt.close();
        }
    }*/
    @Override
    protected String buildPreparedInsertSql(String toTable, JdbcSchema toTableSchema) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("UPSERT USING LOAD INTO ");
        quoteIdentifierString(sb, toTable);

        sb.append(" (");
        for (int i=0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(") VALUES (");
        for(int i=0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            sb.append("?");
        }
        sb.append(")");

        return sb.toString();
    }
    @Override
    protected String buildCollectInsertSql(List<String> fromTables, JdbcSchema schema, String toTable)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("UPSERT USING LOAD INTO ");
        quoteIdentifierString(sb, toTable);
        sb.append(" (");
        for (int i=0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        for (int i=0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for (int j=0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteIdentifierString(sb, fromTables.get(i));
        }

        return sb.toString();
    }
    protected int executeUpdate(Statement stmt, String sql) throws SQLException
    {
        System.out.println("SQL: " + sql);
        long startTime = System.currentTimeMillis();
        int count = stmt.executeUpdate(sql);
        double seconds = (System.currentTimeMillis() - startTime) / 1000.0;
        if (count == 0) {
            System.out.println(String.format("> %.2f seconds", seconds));
        } else {
            System.out.println(String.format("> %.2f seconds (%,d rows)", seconds, count));
        }
        return count;
    }

    /*public void createTable(String tableName, JdbcSchema schema) throws SQLException
    {
        Statement stmt = connection.createStatement();
        try {
            String sql = buildCreateTableSql(tableName, schema);
            executeUpdate(stmt, sql);
            commitIfNecessary(connection);
        } catch (SQLException ex) {
            throw safeRollback(connection, ex);
        } finally {
            stmt.close();
        }
    }

    protected String buildCreateTableSql(String name, JdbcSchema schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        quoteIdentifierString(sb, name);
        sb.append(buildCreateTableSchemaSql(schema));
        return sb.toString();
    }*/
    @Override
    protected String buildPreparedMergeSql(String toTable, JdbcSchema toTableSchema, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("UPSERT USING LOAD INTO ");
        quoteIdentifierString(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, toTableSchema.getColumnName(i));
        }
        sb.append(") VALUES (");
        for(int i = 0; i < toTableSchema.getCount(); i++) {
            if(i != 0) { sb.append(", "); }
            sb.append("?");
        }
        sb.append(")");
        sb.append(" ON DUPLICATE KEY UPDATE ");
        if (mergeConfig.getMergeRule().isPresent()) {
            List<String> rule = mergeConfig.getMergeRule().get();
            for (int i = 0; i < rule.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(rule.get(i));
            }
        } else {
            for (int i = 0; i < toTableSchema.getCount(); i++) {
                if(i != 0) { sb.append(", "); }
                String columnName = quoteIdentifierString(toTableSchema.getColumnName(i));
                sb.append(columnName).append(" = VALUES(").append(columnName).append(")");
            }
        }

        return sb.toString();
    }

    @Override
    protected String buildCollectMergeSql(List<String> fromTables, JdbcSchema schema, String toTable, MergeConfig mergeConfig) throws SQLException
    {
        StringBuilder sb = new StringBuilder();

        sb.append("UPSERT USING LOAD INTO ");
        quoteIdentifierString(sb, toTable);
        sb.append(" (");
        for (int i = 0; i < schema.getCount(); i++) {
            if (i != 0) { sb.append(", "); }
            quoteIdentifierString(sb, schema.getColumnName(i));
        }
        sb.append(") ");
        for (int i = 0; i < fromTables.size(); i++) {
            if (i != 0) { sb.append(" UNION ALL "); }
            sb.append("SELECT ");
            for (int j = 0; j < schema.getCount(); j++) {
                if (j != 0) { sb.append(", "); }
                quoteIdentifierString(sb, schema.getColumnName(j));
            }
            sb.append(" FROM ");
            quoteIdentifierString(sb, fromTables.get(i));
        }
        sb.append(" ON DUPLICATE KEY UPDATE ");
        if (mergeConfig.getMergeRule().isPresent()) {
            List<String> rule = mergeConfig.getMergeRule().get();
            for (int i = 0; i < rule.size(); i++) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(rule.get(i));
            }
        } else {
            for (int i = 0; i < schema.getCount(); i++) {
                if(i != 0) { sb.append(", "); }
                String columnName = quoteIdentifierString(schema.getColumnName(i));
                sb.append(columnName).append(" = VALUES(").append(columnName).append(")");
            }
        }

        return sb.toString();
    }
    /*@Override
    protected String buildPreparedInsertSql(String toTable, JdbcSchema toTableSchema) throws SQLException
    {
        String sql = super.buildPreparedInsertSql(toTable, toTableSchema);
        return sql;
    }*/
    @Override
    protected String buildColumnTypeName(JdbcColumn c)
    {
        switch(c.getSimpleTypeName()) {
        case "CLOB":
            return "varchar(1000)";
        default:
            return super.buildColumnTypeName(c);
        }
    }
}
