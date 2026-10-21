// Copyright 2021-2026 The Buildfarm Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.metrics.prometheus;

import static java.util.logging.Level.SEVERE;

import build.buildfarm.common.redis.RedisHashtags;
import build.buildfarm.v1test.BackplaneStatus;
import build.buildfarm.v1test.QueueStatus;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.java.Log;

@Log
public class PrometheusPublisher implements Runnable {
  private final Supplier<BackplaneStatus> getMetrics;
  private HTTPServer server;
  private Thread metricsThread;
  private final Gauge preQueueSize =
      Gauge.build().name("pre_queue_size").help("Pre queue size.").register();
  private final Gauge workerPoolSize =
      Gauge.build().name("worker_pool_size").help("Active worker pool size.").register();
  private final Gauge storageWorkerPoolSize =
      Gauge.build()
          .name("storage_worker_pool_size")
          .help("Active storage worker pool size.")
          .register();
  private final Gauge executeWorkerPoolSize =
      Gauge.build()
          .name("execute_worker_pool_size")
          .help("Active execute worker pool size.")
          .register();
  private final Gauge queueSize =
      Gauge.build().name("queue_size").labelNames("queue_name").help("Queue size.").register();
  private final Gauge dispatchedOperationsSize =
      Gauge.build()
          .name("dispatched_operations_size")
          .help("Dispatched operations size.")
          .register();

  public PrometheusPublisher(Supplier<BackplaneStatus> getMetrics) {
    this.getMetrics = getMetrics;
  }

  public void startHttpServer(int port) {
    try {
      if (port > 0) {
        DefaultExports.initialize();
        server = new HTTPServer(port);
        log.info("Started Prometheus HTTP Server on port " + port);
        startMetricsThread();
      } else {
        log.info("Prometheus port is not configured. HTTP Server will not be started");
      }
    } catch (IOException e) {
      log.severe("Could not start Prometheus HTTP Server on port " + port);
    }
  }

  private void updateQueueSizes(List<QueueStatus> queues) {
    if (queueSize != null) {
      for (QueueStatus queueStatus : queues) {
        queueSize
            .labels(RedisHashtags.unhashedName(queueStatus.getName()))
            .set(queueStatus.getSize());
      }
    }
  }

  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        TimeUnit.SECONDS.sleep(30);
        BackplaneStatus backplaneStatus = getMetrics.get();
        workerPoolSize.set(backplaneStatus.getActiveWorkersCount());
        executeWorkerPoolSize.set(backplaneStatus.getActiveExecuteWorkersCount());
        storageWorkerPoolSize.set(backplaneStatus.getActiveStorageWorkersCount());
        dispatchedOperationsSize.set(backplaneStatus.getDispatchedSize());
        preQueueSize.set(backplaneStatus.getPrequeue().getSize());
        updateQueueSizes(backplaneStatus.getOperationQueue().getProvisionsList());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        log.log(SEVERE, "Could not update Backplane metrics", e);
      }
    }
  }

  private void startMetricsThread() {
    metricsThread = new Thread(this, "Prometheus Metrics Collector");
    metricsThread.start();
  }

  public void stopHttpServer() throws InterruptedException {
    if (server != null) {
      server.stop();
    }
    if (metricsThread != null) {
      metricsThread.interrupt();
      metricsThread.join();
    }
  }
}
