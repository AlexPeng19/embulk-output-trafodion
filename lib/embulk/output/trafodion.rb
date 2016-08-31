Embulk::JavaPlugin.register_output(
  "trafodion", "org.embulk.output.trafodion.TrafodionOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
