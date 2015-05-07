package org.elasticsearch.river.kafka;

import com.codahale.metrics.*;
import com.codahale.metrics.ganglia.GangliaReporter;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

import info.ganglia.gmetric4j.gmetric.GMetric;
import org.elasticsearch.river.kafka.RiverConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GangliaMetricsFactory {
    public static final MetricRegistry METRIC = SharedMetricRegistries.getOrCreate("usergrid");
    private static final Logger LOG = LoggerFactory.getLogger(GangliaMetricsFactory.class);

    private ScheduledReporter reporter;
    private boolean writeMetricsEnable = false;
    private static final String badHost = "badhost";

    private ConcurrentHashMap<String, Metric> hashMap = new ConcurrentHashMap<String, Metric>();

    public GangliaMetricsFactory(final RiverConfig riverConfig) {
	  String metricsHost = riverConfig.getMetricsHost();
	  String metricsPort = riverConfig.getMetricsPort();
	  writeMetricsEnable = riverConfig.isWriteMetricsEnable();
      if (!metricsHost.equals(badHost) && writeMetricsEnable) {
          try {
              METRIC.registerAll(new MemoryUsageGaugeSet());
              METRIC.registerAll(new ThreadStatesGaugeSet());
              METRIC.registerAll(new GarbageCollectorMetricSet());
              METRIC.registerAll(new JvmAttributeGaugeSet());
              GMetric gMetric = new GMetric(metricsHost, Integer.valueOf(metricsPort), GMetric.UDPAddressingMode.UNICAST, 0);
              reporter = GangliaReporter.forRegistry(METRIC)
                      .convertDurationsTo(TimeUnit.SECONDS)
                      .convertRatesTo(TimeUnit.SECONDS)
                      .prefixedWith("usergrid")
                      .build(gMetric);
          } catch (IOException e) {
              LOG.warn("MetricsService:Logger not started.", e);
          }
      }
      if (reporter == null) {
          reporter = Slf4jReporter.forRegistry(METRIC).convertDurationsTo(TimeUnit.SECONDS).convertRatesTo(TimeUnit.SECONDS).build();
      }
      reporter.start(30, TimeUnit.SECONDS);
    }
    
    public MetricRegistry getRegistry() {
        return METRIC;
    }

    public Timer getTimer(Class<?> klass, String name) {
        return getMetric(Timer.class, klass, name);
    }

    public Histogram getHistogram(Class<?> klass, String name) {
        return getMetric(Histogram.class, klass, name);
    }

    public Counter getCounter(Class<?> klass, String name) {
        return getMetric(Counter.class, klass, name);
    }

    public Meter getMeter(Class<?> klass, String name) {
        return getMetric(Meter.class, klass, name);
    }

    @SuppressWarnings("unchecked")
	private <T> T getMetric(Class<T> metricClass, Class<?> klass, String name) {
        String key = metricClass.getName() + klass.getName() + name;
        Metric metric = hashMap.get(key);
        if (metric == null) {
            if (metricClass == Histogram.class) {
                metric = this.getRegistry().histogram(MetricRegistry.name(klass, name));
            }
            if (metricClass == Timer.class) {
                metric = this.getRegistry().timer(MetricRegistry.name(klass, name));
            }
            if (metricClass == Meter.class) {
                metric = this.getRegistry().meter(MetricRegistry.name(klass, name));
            }
            if (metricClass == Counter.class) {
                metric = this.getRegistry().counter(MetricRegistry.name(klass, name));
            }
            hashMap.put(key, metric);
        }
        return (T) metric;
    }

}