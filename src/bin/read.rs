use parquet_bench::read;

fn main() {
    let path = std::env::var("READ_PATH").unwrap_or_else(|_| panic!("expect env var READ_PATH"));
    let col_num = std::env::var("READ_COL_NUM")
        .unwrap_or_else(|_| panic!("expect env var READ_COL_NUM"))
        .parse()
        .unwrap();
    read(&path, col_num);
}
