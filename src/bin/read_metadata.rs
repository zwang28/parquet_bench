use parquet_bench::read_metadata;

fn main() {
    let path = std::env::var("READ_PATH").unwrap_or_else(|_| panic!("expect env var READ_PATH"));
    read_metadata(&path);
}
