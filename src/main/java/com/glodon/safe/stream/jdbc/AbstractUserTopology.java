package com.glodon.safe.stream.jdbc;

import com.glodon.safe.stream.spout.UserSpout;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.tuple.Fields;

import java.sql.Types;
import java.util.List;
import java.util.Map;

public abstract class AbstractUserTopology {
    private static final List<String> setupSqls = Lists.newArrayList(
            "drop table if exists t_user",
            "drop table if exists t_department",
            "drop table if exists t_user_department",
            "create table if not exists t_user(user_id integer, user_name varchar(100), dept_name varchar(100), create_date date)",
            "create table if not exists t_department(dept_id integer, dept_name varchar(100))",
            "create table if not exists t_user_department(user_id integer, dept_id integer)",
            "insert into t_department values (1, '财务部')",
            "insert into t_department values(2,'技术部')",
            "insert into t_department values(3,'行政部')",
            "insert into t_department values(4,'HR部')",
            "insert into t_user_department values(1,2)",
            "insert into t_user_department values(2,1)",
            "insert into t_user_department values(3,4)",
            "insert into t_user_department values(4,3)"
    );

    protected UserSpout userSpout;
    protected JdbcMapper jdbcMapper;
    protected JdbcLookupMapper jdbcLookupMapper;
    protected ConnectionProvider connectionProvider;

    protected static final String TABLE_NAME = "t_user";
    protected static final String JDBC_CONF = "jdbc.conf";
    protected static final String SELECT_QUERY = "" +
            "select d.dept_name from t_department d , t_user_department ud where d.dept_id = ud.dept_id" +
            " and ud.user_id = ?";
    public void execute(String[] args) throws Exception {
        if (args.length != 4 && args.length != 5) {
            System.out.println("Usage: " + this.getClass().getSimpleName() + "<dataSourceClassName> <dataSource.url> " +
                    "<user> <password> [<topology name>]");
            System.exit(-1);
        }
        Map map = Maps.newHashMap();
        map.put("dataSourceClassName", args[0]);//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
        map.put("dataSource.url", args[1]);//jdbc:mysql://localhost/test
        map.put("dataSource.user", args[2]);//root
        if (args.length == 4) {
            map.put("dataSource.password", args[3]);//password
        }

        Config config = new Config();
        config.put(JDBC_CONF, map);
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(map);
        connectionProvider.prepare();
        int queryTimeoutSecs = 60;
        JdbcClient jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
        for (String sql : setupSqls) {
            jdbcClient.executeSql(sql);
        }

        this.userSpout = new UserSpout();
        this.jdbcMapper = new SimpleJdbcMapper(TABLE_NAME, connectionProvider);
        connectionProvider.cleanup();

        Fields outputFields = new Fields("user_id","user_name","dept_name","create_date");
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.INTEGER));
        this.jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        this.connectionProvider = new HikariCPConnectionProvider(map);

        if (args.length == 4) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("jdbc_test", config, getTopology());
            Thread.sleep(30000);
            cluster.killTopology("jdbc_test");
            cluster.shutdown();
            System.exit(0);
        } else {
            StormSubmitter.submitTopology(args[4], config, getTopology());
        }


    }

    protected abstract StormTopology getTopology();

}
