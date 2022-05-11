/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.blaze

import scala.annotation.tailrec
import scala.collection.immutable.TreeMap

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.adaptive.CustomShuffleReaderExec
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.Partition
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.blaze.protobuf.PartitionId
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.TaskDefinition

trait NativeSupports extends SparkPlan {
  def doExecuteNative(): NativeRDD

  protected override def doExecute(): RDD[InternalRow] = doExecuteNative()
  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = doExecuteNative().toColumnar

  // native to native plans are not columnar executed
  var columnarEnabled = false
  override def supportsColumnar: Boolean = columnarEnabled
}

object NativeSupports extends Logging {
  private var nativeInitialized: Boolean = false

  @tailrec def isNative(plan: SparkPlan): Boolean =
    plan match {
      case _: NativeSupports => true
      case plan: CustomShuffleReaderExec => isNative(plan.child)
      case plan: QueryStageExec => isNative(plan.plan)
      case plan: ReusedExchangeExec => isNative(plan.child)
      case _ => false
    }

  @tailrec def getUnderlyingNativePlan(plan: SparkPlan): NativeSupports =
    plan match {
      case plan: NativeSupports => plan
      case plan: CustomShuffleReaderExec => getUnderlyingNativePlan(plan.child)
      case plan: QueryStageExec => getUnderlyingNativePlan(plan.plan)
      case plan: ReusedExchangeExec => getUnderlyingNativePlan(plan.child)
      case _ => throw new RuntimeException("unreachable: plan is not native")
    }

  @tailrec def executeNative(plan: SparkPlan): NativeRDD =
    plan match {
      case plan: NativeSupports => plan.doExecuteNative()
      case plan: CustomShuffleReaderExec => executeNative(plan.child)
      case plan: QueryStageExec => executeNative(plan.plan)
      case plan: ReusedExchangeExec => executeNative(plan.child)
      case _ => throw new SparkException(s"Underlying plan is not NativeSupports: ${plan}")
    }

  def executeNativePlan(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: TaskContext): Iterator[InternalRow] = {

    val iterPtr = executeNativePlanInternal(nativePlan, partition, context)
    if (iterPtr < 0) {
      logWarning("Error occurred while call physical_plan.execute")
      return Iterator.empty
    }
    FFIHelper.fromBlazeIter(iterPtr, context, metrics)
  }

  def executeNativePlanColumnar(
      nativePlan: PhysicalPlanNode,
      metrics: MetricNode,
      partition: Partition,
      context: TaskContext): Iterator[ColumnarBatch] = {

    val iterPtr = executeNativePlanInternal(nativePlan, partition, context)
    if (iterPtr < 0) {
      logWarning("Error occurred while call physical_plan.execute")
      return Iterator.empty
    }
    FFIHelper.fromBlazeIterColumnar(iterPtr, context, metrics)
  }

  def getDefaultNativeMetrics(sc: SparkContext): Map[String, SQLMetric] =
    TreeMap(
      "output_rows" -> SQLMetrics.createMetric(sc, "Native.output_rows"),
      "output_batches" -> SQLMetrics.createMetric(sc, "Native.output_batches"),
      "input_rows" -> SQLMetrics.createMetric(sc, "Native.input_rows"),
      "input_batches" -> SQLMetrics.createMetric(sc, "Native.input_batches"),
      "elapsed_compute" -> SQLMetrics.createNanoTimingMetric(sc, "Native.elapsed_compute"),
      "join_time" -> SQLMetrics.createNanoTimingMetric(sc, "Native.join_time"))

  def executeNativePlanInternal(
      nativePlan: PhysicalPlanNode,
      partition: Partition,
      context: TaskContext): Long = {

    // do not use context.partitionId since it is not correct in Union plans.
    val partitionId = PartitionId
      .newBuilder()
      .setPartitionId(partition.index)
      .setStageId(context.stageId())
      .setJobId(partition.index.toString)
      .build()

    val taskDefinition = TaskDefinition
      .newBuilder()
      .setTaskId(partitionId)
      .setPlan(nativePlan)
      .build()

    val conf = SparkEnv.get.conf
    val batchSize = conf.getLong("spark.blaze.batchSize", 16384);
    val nativeMemory = conf.getLong("spark.executor.memoryOverhead", Long.MaxValue) * 1024 * 1024;
    val memoryFraction = conf.getDouble("spark.blaze.memoryFraction", 0.75);
    val tmpDirs = SparkEnv.get.blockManager.diskBlockManager.localDirsString.mkString(",")

    NativeSupports.synchronized {
      if (!NativeSupports.nativeInitialized) {
        logInfo(s"Initializing native environment ...")
        System.loadLibrary("blaze")
        JniBridge.initNative(batchSize, nativeMemory, memoryFraction, tmpDirs)
        NativeSupports.nativeInitialized = true
      }
    }

    if (SparkEnv.get.conf.getBoolean("spark.blaze.dumpNativePlanBeforeExecuting", false)) {
      logInfo(s"Start executing native plan: ${taskDefinition.toString}")
    } else {
      logInfo(s"Start executing native plan")
    }

    JniBridge.callNative(taskDefinition.toByteArray);
  }
}

case class MetricNode(metrics: Map[String, SQLMetric], children: Seq[MetricNode])
    extends Logging {
  def getChild(i: Int): MetricNode =
    children(i)

  def add(metricName: String, v: Long): Unit = {
    metrics.get(metricName) match {
      case Some(metric) => metric.add(v)
      case None =>
        logWarning(s"Ignore non-exist metric: ${metricName}")
    }
  }
}
