import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.logging.config._
import com.twitter.ostrich.admin.config._
import net.lag.kestrel.config._

new KestrelConfig {
  listenAddress = "0.0.0.0"
  memcacheListenPort = 22133
  textListenPort = 2222

  queuePath = "db/kestrel"

  clientTimeout = 30.seconds

  expirationTimerFrequency = 1.second

  maxOpenTransactions = 100

  // default queue settings:
  default.defaultJournalSize = 16.megabytes
  default.maxMemorySize = 128.megabytes
  default.maxJournalSize = 1.gigabyte
  default.syncJournal = 1.second

  admin.httpPort = 2223

  admin.statsNodes = new StatsConfig {
    reporters = new JsonStatsLoggerConfig {
      loggerName = "stats"
      serviceName = "kestrel"
    } :: new TimeSeriesCollectorConfig
  }

  queues = new QueueBuilder {
    name = "count_event"
  } :: new QueueBuilder {
    name = "raw_event"
  } :: new QueueBuilder {
    // don't keep a journal file for this queue. when kestrel exits, any
    // remaining contents will be lost.
    name               = "source_updated"
    keepJournal        = false
    fanoutOnly         = true
    discardOldWhenFull = true
    maxItems           = 2
    maxAge             = 1.hour
  }

  loggers = new LoggerConfig {
    level = Level.DEBUG
    handlers = new FileHandlerConfig {
      filename = "log/kestrel.log"
      roll = Policy.Never
    }
  }
}
