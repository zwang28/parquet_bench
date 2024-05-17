use parquet_bench::read_object_store;

#[tokio::main]
async fn main() {
    let path = std::env::var("READ_PATH").unwrap_or_else(|_| panic!("expect env var READ_PATH"));
    let col_num = std::env::var("READ_COL_NUM")
        .unwrap_or_else(|_| panic!("expect env var READ_COL_NUM"))
        .parse()
        .unwrap();
    read_object_store(&path, col_num).await;
}
