use parquet_bench::write;

fn main() {
    let path = std::env::var("WRITE_PATH").unwrap_or_else(|_| panic!("expect env var WRITE_PATH"));
    let row_num = std::env::var("WRITE_ROW_NUM")
        .unwrap_or_else(|_| panic!("expect env var WRITE_ROW_NUM"))
        .parse()
        .unwrap();
    let col_num = std::env::var("WRITE_COL_NUM")
        .unwrap_or_else(|_| panic!("expect env var WRITE_COL_NUM"))
        .parse()
        .unwrap();
    let set_max_row_group_size = std::env::var("WRITE_MAX_ROW_GROUP_SIZE")
        .unwrap_or_else(|_| "1000000".into())
        .parse()
        .unwrap();
    write(&path, row_num, col_num, set_max_row_group_size);
}
