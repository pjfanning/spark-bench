/**
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */
package com.ibm.sparktc.sparkbench.metrics

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.scheduler._

class MetricsSparkListener extends SparkListener {

  val stageCount: AtomicLong = new AtomicLong()
  val taskCount: AtomicLong = new AtomicLong()
  val jobCount: AtomicLong = new AtomicLong()
  val executorAddCount: AtomicLong = new AtomicLong()
  val executorRemoveCount: AtomicLong = new AtomicLong()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = stageCount.incrementAndGet()

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = taskCount.incrementAndGet()

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = jobCount.incrementAndGet()

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = executorAddCount.incrementAndGet()

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = executorRemoveCount.incrementAndGet()

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("**** MetricsSparkListener ****")
    println(s"stageCount=${stageCount.get()}")
    println(s"taskCount=${taskCount.get()}")
    println(s"jobCount=${jobCount.get()}")
    println(s"executorAddCount=${executorAddCount.get()}")
    println(s"executorRemoveCount=${executorRemoveCount.get()}")
  }
}
