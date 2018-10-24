package com.glodon.safe.stream.jdbc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import java.sql.Types;
import java.util.List;
import java.util.Map;

public class StromJdbcMapper {
    public ConnectionProvider createConnection () {
        Map hikariConfig = Maps.newHashMap();
        hikariConfig.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfig.put("dataSource.url", "jdbc:mysql://localhost/test");
        hikariConfig.put("dataSource.user","root");
        hikariConfig.put("dataSource.password","password");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfig);
        return connectionProvider;
        /*String tableName = "user_details";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        return simpleJdbcMapper;*/
    }

    public JdbcInsertBolt createInsertBolt() {
        ConnectionProvider conn = createConnection();
        List<Column> columnSchema = Lists.newArrayList(
                new Column("user_id", Types.INTEGER),
                new Column("user_name", Types.VARCHAR)
        );
        JdbcMapper jdbcMapper = new SimpleJdbcMapper(columnSchema);
        JdbcInsertBolt persistBolt = new JdbcInsertBolt(conn, jdbcMapper);
        return persistBolt;
    }
}
