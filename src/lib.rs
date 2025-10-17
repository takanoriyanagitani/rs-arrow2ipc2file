pub use arrow_array;
pub use arrow_ipc;
pub use arrow_schema;

use std::io;
use std::path::Path;

use io::BufWriter;
use io::Write;

use arrow_array::RecordBatch;
use arrow_ipc::writer::FileWriter;
use arrow_schema::Schema;

pub fn write_all<I, W>(rows: I, mut wtr: FileWriter<BufWriter<W>>) -> Result<(), io::Error>
where
    W: Write,
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
{
    for r in rows {
        let rbat: RecordBatch = r?;
        wtr.write(&rbat).map_err(io::Error::other)?;
        wtr.flush().map_err(io::Error::other)?;
    }
    wtr.finish().map_err(io::Error::other)
}

pub fn rows2ipc_file<I, S>(
    rows: I,
    mut out_ipc_file: std::fs::File,
    sch: &Schema,
    fsync: S,
) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    S: Fn(&mut std::fs::File) -> Result<(), io::Error>,
{
    let wtr = FileWriter::try_new_buffered(&mut out_ipc_file, sch).map_err(io::Error::other)?;
    write_all(rows, wtr)?;
    out_ipc_file.flush()?;
    fsync(&mut out_ipc_file)?;
    Ok(())
}

pub fn rows2ipc_filename<I, P, S>(
    rows: I,
    out_ipc_filename: P,
    sch: &Schema,
    fsync: S,
) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    P: AsRef<Path>,
    S: Fn(&mut std::fs::File) -> Result<(), io::Error>,
{
    let outfile = std::fs::File::create(out_ipc_filename)?;
    rows2ipc_file(rows, outfile, sch, fsync)
}

pub fn fsync_all(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_all()
}

pub fn fsync_dat(f: &mut std::fs::File) -> Result<(), io::Error> {
    f.sync_data()
}

pub fn fsync_nop(_: &mut std::fs::File) -> Result<(), io::Error> {
    Ok(())
}

pub fn rows2ipc_filename_default<I, P>(
    rows: I,
    out_ipc_filename: P,
    sch: &Schema,
) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    P: AsRef<Path>,
{
    rows2ipc_filename(rows, out_ipc_filename, sch, fsync_all)
}
