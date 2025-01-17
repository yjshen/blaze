// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::ErrorKind::InvalidData;

use std::io::{Cursor, Read};
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

use crate::jni_call;
use crate::jni_call_static;
use crate::jni_delete_local_ref;
use crate::jni_new_direct_byte_buffer;
use crate::jni_new_global_ref;
use crate::jni_new_string;

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

        let segments_provider = jni_call_static!(
            JniBridge.getResource(
                jni_new_string!(&self.native_shuffle_id)?
            ) -> JObject
        )?;
        let segments = jni_new_global_ref!(
            jni_call!(ScalaFunction0(segments_provider).apply() -> JObject)?
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
    arrow_file_reader: Option<FileReader<Cursor<Vec<u8>>>>,
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
            arrow_file_reader: None,
            baseline_metrics,
        }
    }

    fn next_segment(&mut self) -> Result<bool> {
        if jni_call!(
            ScalaIterator(self.segments.as_obj()).hasNext() -> jboolean
        )? != JNI_TRUE
        {
            self.arrow_file_reader = None;
            return Ok(false);
        }

        let channel = jni_call!(ScalaIterator(self.segments.as_obj()).next() -> JObject)?;
        let len = jni_call!(JavaSeekableByteChannel(channel).size() -> jlong)? as u64;

        // read compressed data
        let mut zdata = vec![0; len as usize];
        let mut zdata_read_bytes = 0;
        while zdata_read_bytes < len as usize {
            let buf = jni_new_direct_byte_buffer!(&mut zdata[zdata_read_bytes..])?;
            let read_bytes = jni_call!(
                JavaSeekableByteChannel(channel).read(buf) -> jint
            )?;
            if read_bytes < 0 {
                return Err(DataFusionError::IoError(std::io::Error::new(
                    InvalidData,
                    "unexpected EOF",
                )));
            }
            zdata_read_bytes += read_bytes as usize;
        }

        // decompress one segment of IPC into memory
        let mut arrow_data = vec![];
        let mut zreader = zstd::stream::Decoder::new(&zdata[..])?;
        zreader.read_to_end(&mut arrow_data)?;

        self.arrow_file_reader =
            Some(FileReader::try_new(Cursor::new(arrow_data), None)?);

        // channel ref must be explicitly deleted to avoid OOM
        jni_delete_local_ref!(channel)?;
        Ok(true)
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
