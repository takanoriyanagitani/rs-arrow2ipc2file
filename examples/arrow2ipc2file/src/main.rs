use std::io;

use rs_arrow2ipc2file::arrow_array;
use rs_arrow2ipc2file::arrow_schema;

use arrow_array::RecordBatch;
use arrow_array::record_batch;

use arrow_schema::Schema;

use rs_arrow2ipc2file::rows2ipc_filename_default;

fn sample_row() -> Result<RecordBatch, io::Error> {
    record_batch!(
        ("timestamp_us", Int64, [0, 1, 2]),
        ("status", Int16, [200, 404, 500]),
        ("body", Utf8, ["ok", "ng", "ng"])
    )
    .map_err(io::Error::other)
}

fn main() -> Result<(), io::Error> {
    let row: RecordBatch = sample_row()?;
    let sch: &Schema = &row.schema();

    let outname = std::env::var("ENV_OUT_IPC_NAME").unwrap_or_default();

    let rows = vec![Ok(row)].into_iter();

    rows2ipc_filename_default(rows, outname, sch)?;
    Ok(())
}
