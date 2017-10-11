package com.metamx.tranquility.kinesis;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.config.TranquilityConfig;
import com.metamx.tranquility.kinesis.model.PropertiesBasedKinesisConfig;
import io.airlift.airline.Help;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;
import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class KinesisMain
{

  private static final Logger log = new Logger(KinesisMain.class);

  @Inject
  public HelpOption helpOption;

  @Option(name = {"-f", "-configFile"}, description = "Path to configuration property file")
  public String propertiesFile;

  public static void main(String[] args) throws Exception
  {
    KinesisMain main;
    try {
      main = SingleCommand.singleCommand(KinesisMain.class).parse(args);
    }
    catch (Exception e) {
      log.error(e, "Exception parsing arguments");
      Help.help(SingleCommand.singleCommand(KinesisMain.class).getCommandMetadata());
      return;
    }

    if (main.helpOption.showHelpIfRequested()) {
      return;
    }

    main.run();
  }

  public void run() throws InterruptedException
  {
    if (propertiesFile == null || propertiesFile.isEmpty()) {
      helpOption.help = true;
      helpOption.showHelpIfRequested();

      log.warn("Missing required parameters, aborting.");
      return;
    }

    TranquilityConfig<PropertiesBasedKinesisConfig> config = null;
    try (InputStream in = new FileInputStream(propertiesFile)) {
      config = TranquilityConfig.read(in, PropertiesBasedKinesisConfig.class);
    }
    catch (IOException e) {
      log.error("Could not read config file: %s, aborting.", propertiesFile);
      Throwables.propagate(e);
    }

    PropertiesBasedKinesisConfig globalConfig = config.globalConfig();
    Map<String, DataSourceConfig<PropertiesBasedKinesisConfig>> dataSourceConfigs = Maps.newHashMap();
    for (String dataSource : config.getDataSources()) {
      dataSourceConfigs.put(dataSource, config.getDataSource(dataSource));
    }

    // find all properties that start with 'kafka.' and pass them on to Kafka
    final Properties kafkaProperties = new Properties();
    for (String propertyName : config.globalConfig().properties().stringPropertyNames()) {
      if (propertyName.startsWith("kafka.")) {
        kafkaProperties.setProperty(
            propertyName.replaceFirst("kafka\\.", ""),
            config.globalConfig().properties().getProperty(propertyName)
        );
      }
    }

    // set the critical Kafka configs again from TranquilityKafkaConfig so it picks up the defaults
    kafkaProperties.setProperty("group.id", globalConfig.getKafkaGroupId());
    kafkaProperties.setProperty("zookeeper.connect", globalConfig.getKafkaZookeeperConnect());
    if (kafkaProperties.setProperty(
        "zookeeper.session.timeout.ms",
        Long.toString(globalConfig.zookeeperTimeout().toStandardDuration().getMillis())
    ) != null) {
      throw new IllegalArgumentException(
          "Set zookeeper.timeout instead of setting kafka.zookeeper.session.timeout.ms"
      );
    }

    Runtime.getRuntime().addShutdownHook(
        new Thread(
            new Runnable()
            {
              @Override
              public void run()
              {
                log.info("Initiating shutdown...");
              }
            }
        )
    );
  }
}
