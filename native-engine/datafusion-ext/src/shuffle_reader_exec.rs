// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::Partitioning::UnknownPartitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::Stream;
use jni::objects::{GlobalRef, JObject};
use jni::sys::{jboolean, jint, jlong, JNI_TRUE};

use crate::jni_bridge::JavaClasses;
use crate::jni_bridge_call_static_method;
use crate::jni_global_ref;
use crate::jni_map_error;
use crate::{jni_bridge_call_method, ResultExt};

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    pub num_partitions: usize,
    pub native_shuffle_id: String,
    pub schema: SchemaRef,
    pub metrics: ExecutionPlanMetricsSet,
}
impl ShuffleReaderExec {
    pub fn new(
        num_partitions: usize,
        native_shuffle_id: String,
        schema: SchemaRef,
    ) -> ShuffleReaderExec {
        ShuffleReaderExec {
            num_partitions,
            native_shuffle_id,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        UnknownPartitioning(self.num_partitions)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Blaze ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let baseline_metrics = BaselineMetrics::new(&self.metrics, 0);
        let elapsed_compute = baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        let env = JavaClasses::get_thread_jnienv();
        let segments_provider = jni_bridge_call_static_method!(
            env,
            JniBridge.getResource -> JObject,
            jni_map_error!(env.new_string(&self.native_shuffle_id))?
        )?;
        let segments = jni_global_ref!(
            env,
            jni_bridge_call_method!(
                env,
                ScalaFunction0.apply -> JObject,
                segments_provider
            )?
        )?;

        let schema = self.schema.clone();
        Ok(Box::pin(ShuffleReaderStream::new(
            schema,
            segments,
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct ShuffleReaderStream {
    schema: SchemaRef,
    segments: GlobalRef,
    current_segment: Option<GlobalRef>,
    arrow_file_reader: Option<FileReader<SeekableByteChannelReader>>,
    baseline_metrics: BaselineMetrics,
}
unsafe impl Sync for ShuffleReaderStream {} // safety: segments is safe to be shared
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ShuffleReaderStream {}

impl ShuffleReaderStream {
    pub fn new(
        schema: SchemaRef,
        segments: GlobalRef,
        baseline_metrics: BaselineMetrics,
    ) -> ShuffleReaderStream {
        ShuffleReaderStream {
            schema,
            segments,
            current_segment: None,
            arrow_file_reader: None,
            baseline_metrics,
        }
    }

    fn next_segment(&mut self) -> Result<bool> {
        let env = JavaClasses::get_thread_jnienv();

        if jni_bridge_call_method!(
            env,
            ScalaIterator.hasNext -> jboolean,
            self.segments.as_obj()
        )? != JNI_TRUE
        {
            self.current_segment = None;
            self.arrow_file_reader = None;
            return Ok(false);
        }

        self.current_segment = Some(jni_global_ref!(
            env,
            jni_bridge_call_method!(
                env,
                ScalaIterator.next -> JObject,
                self.segments.as_obj()
            )?
        )?);

        if let Some(current_segment) = &self.current_segment {
            self.arrow_file_reader = Some(FileReader::try_new(
                // safety:
                // the lifetime of SeekableByteChannelReader is exactly the same as self.arrow_file_reader
                SeekableByteChannelReader(unsafe {
                    std::mem::transmute::<_, JObject<'static>>(current_segment.as_obj())
                }),
                None,
            )?);
            return Ok(true);
        }
        Ok(false)
    }
}

impl Stream for ShuffleReaderStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        if let Some(arrow_file_reader) = &mut self.arrow_file_reader {
            if let Some(record_batch) = arrow_file_reader.next() {
                return self
                    .baseline_metrics
                    .record_poll(Poll::Ready(Some(record_batch)));
            }
        }

        // current arrow file reader reaches EOF, try next ipc
        if self.next_segment()? {
            return self.poll_next(cx);
        }
        Poll::Ready(None)
    }
}
impl RecordBatchStream for ShuffleReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct SeekableByteChannelReader(JObject<'static>);
impl Read for SeekableByteChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let env = JavaClasses::get_thread_jnienv();
        Ok(jni_bridge_call_method!(
            env,
            JavaNioSeekableByteChannel.read -> jint,
            self.0,
            jni_map_error!(env.new_direct_byte_buffer(buf)).to_io_result()?
        )
        .to_io_result()? as usize)
    }
}

impl Seek for SeekableByteChannelReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let env = JavaClasses::get_thread_jnienv();
        match pos {
            SeekFrom::Start(position) => {
                jni_bridge_call_method!(
                    env,
                    JavaNioSeekableByteChannel.setPosition -> JObject,
                    self.0,
                    position as i64
                )
                .to_io_result()?;
                Ok(position)
            }

            SeekFrom::End(offset) => {
                let size = jni_bridge_call_method!(
                    env,
                    JavaNioSeekableByteChannel.size -> jlong,
                    self.0
                )
                .to_io_result()? as u64;

                let position = size + offset as u64;
                jni_bridge_call_method!(
                    env,
                    JavaNioSeekableByteChannel.setPosition -> JObject,
                    self.0,
                    position as i64
                )
                .to_io_result()?;
                Ok(position)
            }

            SeekFrom::Current(_) => unimplemented!(),
        }
    }
}
