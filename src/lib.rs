use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use std::fs::File;
use std::sync::Arc;

pub fn write(path: &str, row_num: usize, col_num: usize) {
    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::LZ4)
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

pub fn read(path: &str) {
    let file = File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    println!("Converted arrow schema is: {}", builder.schema());

    let mut reader = builder.build().unwrap();

    let record_batch = reader.next().unwrap().unwrap();

    println!("Read {} records.", record_batch.num_rows());
}
