use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let path = std::env::var("READ_PATH").unwrap_or_else(|_| panic!("expect env var READ_PATH"));
    let read_bytes = std::env::var("READ_BYTES")
        .unwrap_or_else(|_| panic!("expect env var READ_BYTES"))
        .parse()
        .unwrap();
    let storage_container = Arc::new(
        object_store::aws::AmazonS3Builder::from_env()
            .with_bucket_name(std::env::var("AWS_S3_BUCKET").unwrap())
            .build()
            .unwrap(),
    );
    let location = Path::from(path);

    let start = Instant::now();
    // Fetch just the file metadata
    let meta = storage_container.head(&location).await.unwrap();
    println!("{meta:?}");
    println!("e1: {}", start.elapsed().as_secs());

    // Fetch the object including metadata
    let _result = storage_container
        .get_range(&location, 0..read_bytes)
        .await
        .unwrap();
    println!("e2: {}", start.elapsed().as_secs());
}
