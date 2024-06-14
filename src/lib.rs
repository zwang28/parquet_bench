use arrow::array::{Array, ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::async_reader::ParquetObjectReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::fs::File;
use std::sync::Arc;

pub fn write(path: &str, row_num: usize, col_num: usize, set_max_row_group_size: usize) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(set_max_row_group_size)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(4).unwrap()))
        .build();

    let empty_data = (0..col_num)
        .into_iter()
        .map(|i| {
            (
                format!("v{i}").to_string(),
                Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
            )
        })
        .collect::<Vec<_>>();
    let empty_batch = RecordBatch::try_from_iter(empty_data).unwrap();
    let mut writer = ArrowWriter::try_new(file, empty_batch.schema(), Some(props)).unwrap();
    let mut rng = rand::thread_rng();
    for _ in 0..row_num {
        let data = (0..col_num)
            .into_iter()
            .map(|i| {
                (
                    format!("v{i}").to_string(),
                    Arc::new(Int32Array::from(vec![rng.gen_range(i32::MIN..=i32::MAX)]))
                        as ArrayRef,
                )
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(data).unwrap();
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

pub fn read_metadata(path: &str) {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let metadata = reader.metadata();
    let num_row_groups = metadata.num_row_groups();
    println!("row group num {num_row_groups}");
}

pub fn read(path: &str, col_num: usize) {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = builder.parquet_schema();
    println!("{}/{}", col_num, schema.num_columns());
    let mask = ProjectionMask::leaves(schema, 0..col_num);
    let mut reader = builder.with_projection(mask).build().unwrap();

    let mut num_rows = 0;
    let mut num_batch = 0;
    let mut sum: f64 = 0.0;
    while let Some(record_batch) = reader.next() {
        let record_batch = record_batch.unwrap();
        for c_id in 0..record_batch.num_columns() {
            let c = record_batch
                .column(c_id)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            for r_id in 0..record_batch.num_rows() {
                sum += c.value(r_id) as f64;
            }
        }
        num_rows += record_batch.num_rows();
        num_batch += 1;
    }
    println!("Read {} records in {} batch.", num_rows, num_batch);
    println!("Sum {}", sum);
}

pub async fn read_object_store(path: &str, col_num: usize) {
    let storage_container = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(std::env::var("AWS_S3_BUCKET").unwrap())
            .build()
            .unwrap(),
    );
    let location = Path::from(path);
    let meta = storage_container.head(&location).await.unwrap();
    println!("Found Blob with {}B at {}", meta.size, meta.location);
    // Show Parquet metadata
    let reader = ParquetObjectReader::new(storage_container, meta);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
    let schema = builder.parquet_schema();
    println!("{}/{}", col_num, schema.num_columns());
    let mask = ProjectionMask::leaves(schema, 0..col_num);
    let mut reader = builder.with_projection(mask).build().unwrap();

    let mut num_rows = 0;
    let mut num_batch = 0;
    let mut sum: f64 = 0.0;
    while let Some(record_batch) = reader.next().await {
        let record_batch = record_batch.unwrap();
        for c_id in 0..record_batch.num_columns() {
            let c = record_batch
                .column(c_id)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap();
            for r_id in 0..record_batch.num_rows() {
                sum += c.value(r_id) as f64;
            }
        }
        num_rows += record_batch.num_rows();
        num_batch += 1;
    }
    println!("Read {} records in {} batch.", num_rows, num_batch);
    println!("Sum {}", sum);
}
