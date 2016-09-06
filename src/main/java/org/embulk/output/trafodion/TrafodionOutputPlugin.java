package org.embulk.output.trafodion;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin.Features;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin.PluginTask;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.MergeConfig;

import com.google.common.base.Optional;

import org.embulk.output.trafodion.TrafodionOutputPlugin.TrafodionPluginTask;
public class TrafodionOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    public interface TrafodionPluginTask
            extends PluginTask
    {
        /*@Config("host")
	@ConfigDefault("null")
        public String getHost();

        @Config("port")
        @ConfigDefault("23400")
        public int getPort();*/
		
	@Config("url")
        @ConfigDefault("null")
        public Optional<String> getUrl();


        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        //@Config("database")
		//@ConfigDefault("trafodion")
        //public String getDatabase();
        
       //@Config("schema")
       //public String getSchema();
        
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return TrafodionPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(64)
            .setIgnoreMergeKeys(true);
    }

    @Override
    protected TrafodionOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
    	TrafodionPluginTask trafodionTask = (TrafodionPluginTask) task;

	String url=new String();
       if (trafodionTask.getUrl().isPresent()) {
            url = trafodionTask.getUrl().get();
        } else {
           // url = String.format("jdbc:t4jdbc://%s:%d/:",
            //    trafodionTask.getHost(), trafodionTask.getPort());
        }
	System.out.println("-----------------------------------"+ url);
	Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("useCompression", "true");
	props.setProperty("connectTimeout", "300000"); // milliseconds
        props.setProperty("socketTimeout", "1800000");
	props.setProperty("tcpKeepAlive", "true");
	if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
                         props.setProperty("connectTimeout",  "300000");  // milliseconds
                                     props.setProperty("socketTimeout", "2700000");   // milliseconds
                                             } 
	props.putAll(trafodionTask.getOptions());

        props.setProperty("user", trafodionTask.getUser());
        logger.info("Connecting to {} options {}", url, props);
        props.setProperty("password", trafodionTask.getPassword());
        return new TrafodionOutputConnector(url, props);
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        return new TrafodionBatchInsert(getConnector(task, true), mergeConfig);
    }


    @Override
    protected boolean isRetryableException(String sqlState, int errorCode)
    {
        switch (errorCode) {
            case 1213: // ER_LOCK_DEADLOCK (Message: Deadlock found when trying to get lock; try restarting transaction)
                return true;
            case 1205: // ER_LOCK_WAIT_TIMEOUT (Message: Lock wait timeout exceeded; try restarting transaction)
                return true;
            default:
                return false;
        }
    }
}
