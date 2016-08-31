package org.embulk.output.trafodion;

import java.util.Properties;
import java.sql.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import org.embulk.output.jdbc.JdbcOutputConnector;

public class TrafodionOutputConnector
        implements JdbcOutputConnector
{
    private final Driver driver;
    private final String url;
    private final Properties properties;

    public TrafodionOutputConnector(String url, Properties properties)
    {
        this.driver=new org.trafodion.jdbc.t4.T4Driver();
        this.url = url;
        this.properties = properties;
    }

    public TrafodionOutputConnection connect(boolean autoCommit) throws SQLException
    {
        Connection c = driver.connect(url, properties);
        try {
            TrafodionOutputConnection con = new TrafodionOutputConnection(c,autoCommit);
            c = null;
            return con;
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }
}
