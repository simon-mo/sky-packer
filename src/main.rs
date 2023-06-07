use clap::Parser;
use log::info;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::{
    collections::HashSet,
    fs::File,
    io::{Read, Write},
    path::Path,
    sync::{Arc, Mutex},
};
use tar::Header;

#[derive(Parser)]
struct Cli {
    /// The compressoin type, either zstd or gzip, required
    #[clap(short, long)]
    compression: String,

    /// Compute hash
    #[clap(long, default_value = "false")]
    hash: bool,

    /// Split the tar archive into multiple files, prefixed with this path.
    /// The output will be compressed with the same compression type as the input.
    #[clap(long)]
    split_to: Option<String>,
    /// Split size, either a number of bytes or a human readable string like 1GB.
    #[clap(long)]
    split_size: Option<String>,

    /// Unpack from directory, if specified, this is the only thing we do
    #[clap(long)]
    unpack_from: Option<String>,
    #[clap(long)]
    unpack_to: Option<String>,

    #[clap(long)]
    tar_source_from: Option<String>,
}

struct WriterState {
    split_size: u64,
    split_to: String,
    compression: String,

    current_split_file: Option<tar::Builder<Box<dyn Write>>>,
    current_split_file_size: u64,
    current_file_contains_path: HashSet<String>,

    num_split_files_completed: usize,
    split_file_name: String,

    tar_source_from: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SplitMetadata {
    path: String,
    start_offset: u32,
    chunk_size: u32,
    total_size: u64,
}

struct PassThroughHashWriter<T: std::io::Write> {
    hash_context: Context,
    inner: T,
    hash_write_to_path: String,
}

impl<T: std::io::Write> PassThroughHashWriter<T> {
    fn new(inner: T, hash_write_to_path: String) -> Self {
        Self {
            hash_context: Context::new(&SHA256),
            inner,
            hash_write_to_path,
        }
    }
}

impl<T: std::io::Write> Drop for PassThroughHashWriter<T> {
    fn drop(&mut self) {
        let hash = data_encoding::HEXLOWER.encode(self.hash_context.clone().finish().as_ref());
        std::fs::write(&self.hash_write_to_path, hash).unwrap();
    }
}

impl<T: std::io::Write> std::io::Write for PassThroughHashWriter<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.hash_context.update(buf);
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl WriterState {
    fn new(
        split_size: u64,
        split_to: String,
        compression: String,
        tar_source_from: Option<String>,
    ) -> Self {
        Self {
            current_split_file: None,
            split_file_name: "".to_string(),
            current_split_file_size: 0,
            num_split_files_completed: 0,
            current_file_contains_path: HashSet::new(),
            split_size,
            split_to,
            compression,
            tar_source_from,
        }
    }

    fn finish_current_file(&mut self) {
        if self.current_split_file.is_some() {
            self.current_split_file.as_mut().unwrap().finish().unwrap();
        }
        self.current_split_file = None;
        self.current_split_file_size = 0;
        self.current_file_contains_path.clear();
        self.num_split_files_completed += 1;
    }

    fn ensure_new_file(&mut self) {
        if self.current_split_file.is_none() {
            self.split_file_name =
                format!("{}.{:03}", self.split_to, self.num_split_files_completed);
            let split_file = std::fs::File::create(&self.split_file_name).unwrap();
            // let split_file: Box<dyn Write> = match self.compression.as_str() {
            //     "zstd" => Box::new(zstd::stream::write::Encoder::new(split_file, 3).unwrap()),
            //     "gzip" => Box::new(flate2::write::GzEncoder::new(
            //         split_file,
            //         flate2::Compression::fast(),
            //     )),
            //     "none" => Box::new(split_file),
            //     _ => {
            //         panic!("Unknown compression type, must be either zstd or gzip");
            //     }
            // };
            let hash_writer = PassThroughHashWriter::new(
                split_file,
                format!("{}.compressed.sha256", self.split_file_name),
            );
            let encoder = zstd::stream::write::Encoder::new(hash_writer, 3)
                .unwrap()
                .auto_finish();
            let writer = PassThroughHashWriter::new(
                encoder,
                format!("{}.uncompressed.sha256", self.split_file_name),
            );
            self.current_split_file = Some(tar::Builder::new(Box::new(writer)));
        }
    }

    fn write(&mut self, mut entry: tar::Entry<'_, Box<dyn Read>>) {
        let mut file_size = entry.header().size().unwrap().clone();
        // https://github.com/alexcrichton/tar-rs/issues/286
        if let Some(mut pax) = entry.pax_extensions().unwrap() {
            if let Some(Ok(size)) = pax.find(|p| {
                let key = p.as_ref().unwrap().key().unwrap();
                let val = p.as_ref().unwrap().value().unwrap();
                info!("PAX extension: {} = {}", key, val);
                key.ends_with("size")
            }) {
                let real_size = size.value().unwrap().parse::<u64>().unwrap();
                info!(
                    "Sparse file detected, header claim size is {}, real size is {}, using real size",
                    file_size, real_size
                );
                file_size = real_size;
            }
        };

        if self.tar_source_from.is_some() && entry.header().entry_type().is_file() {
            let tar_source_from = self.tar_source_from.as_ref().unwrap();
            let path = entry.path().unwrap().display().to_string();
            let path = Path::new(tar_source_from).join(path);
            let metadata = std::fs::metadata(&path).unwrap();
            if metadata.len() != file_size {
                info!(
                    "File size mismatch, tar header says {}, actual file size is {}",
                    file_size,
                    metadata.len()
                );
                file_size = metadata.len();
            }
        }

        if self.current_split_file_size >= self.split_size && file_size > 0 {
            info!(
                "Advancing to next file because {} > {}",
                self.current_split_file_size, self.split_size
            );
            self.finish_current_file();
        }
        self.ensure_new_file();

        let path = entry.path().unwrap().display().to_string();

        // Check links
        {
            if entry.header().entry_type().is_hard_link() {
                let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                assert!(
                self.current_file_contains_path.contains(&target_path),
                "Current file {} is a hard link to {}, but the target file isn't in this archive. This will cause trouble during extraction",
                &path,
                &target_path
            );
            }
            self.current_file_contains_path.insert(path.clone());
        }

        // Write the file!
        if file_size > self.split_size {
            // we will split this file to multiple archives
            // first, we will fill up the current archive, then break the rest, the last archive can be less than split_size (for now).
            let current_archive_remaining_size = self.split_size - self.current_split_file_size;

            let mut segment_idx: usize = 0;
            let mut start_offset: u32 = 0;
            let mut remaining_size = file_size.clone();
            let mut entry = entry;
            while remaining_size > 0 {
                let mut chunk_size = std::cmp::min(remaining_size, self.split_size);
                if segment_idx == 0 {
                    chunk_size = std::cmp::min(chunk_size, current_archive_remaining_size);
                }

                if self.current_split_file_size >= self.split_size {
                    self.finish_current_file();
                }
                self.ensure_new_file();
                let current_split_file = self.current_split_file.as_mut().unwrap();

                // Write the metadata for this split
                {
                    let split_metadata = SplitMetadata {
                        path: path.clone(),
                        start_offset: start_offset,
                        chunk_size: chunk_size as u32,
                        total_size: file_size,
                    };
                    let metadata_json = serde_json::to_string(&split_metadata).unwrap();
                    let metadata_json_bytes = metadata_json.as_bytes();

                    let mut metadata_header = Header::new_gnu();
                    metadata_header.set_size(metadata_json_bytes.len() as u64);
                    metadata_header.set_cksum();

                    current_split_file
                        .append_data(
                            &mut metadata_header,
                            format!("{}.split-metadata.{}.json", path, segment_idx),
                            metadata_json_bytes,
                        )
                        .unwrap();
                }

                // Write the actual data
                {
                    let mut chunk_header = Header::new_gnu();
                    let old_header = entry.header().clone();
                    chunk_header.set_size(chunk_size);
                    chunk_header.set_entry_type(tar::EntryType::Regular);
                    chunk_header.set_path(&path).unwrap();
                    chunk_header.set_uid(old_header.uid().unwrap());
                    chunk_header.set_gid(old_header.gid().unwrap());
                    chunk_header.set_cksum();
                    let mut chunk_data = entry.take(chunk_size as u64);
                    current_split_file
                        .append(&chunk_header, &mut chunk_data)
                        .unwrap();
                    entry = chunk_data.into_inner();
                }

                info!(
                    "Writing {} (size {}) to {} (segment {})",
                    &path, &chunk_size, self.split_file_name, segment_idx
                );

                segment_idx += 1;
                start_offset += chunk_size as u32;
                remaining_size -= chunk_size;
                self.current_split_file_size += chunk_size;
                self.current_file_contains_path.insert(path.clone());
            }
        } else {
            if file_size > 1000000 {
                info!(
                    "Writing {} (size {}) to {}",
                    &path, &file_size, self.split_file_name
                );
            }
            let current_split_file = self.current_split_file.as_mut().unwrap();
            current_split_file
                .append_data(&mut entry.header().clone(), path, entry)
                .unwrap();
            self.current_split_file_size += file_size;
        }
    }
}

fn ensure_parent_dir_exists(path: &std::path::PathBuf) {
    let mut dir_path = path.clone();
    dir_path.pop();
    std::fs::create_dir_all(&dir_path).unwrap();
}

fn unpack_one_tar(path: std::path::PathBuf, unpack_to: String, fallocate_lock: Arc<Mutex<()>>) {
    let mut tar = tar::Archive::new(File::open(&path).unwrap());
    let mut maybe_split_metadata: Option<SplitMetadata> = None;
    for entry in tar.entries().unwrap() {
        let mut entry = entry.unwrap();
        let path = entry.path().unwrap().display().to_string();

        match entry.header().entry_type() {
            tar::EntryType::Directory => {
                info!("Creating directory {}", &path);
                let path = Path::new(&unpack_to).join(path.clone());
                std::fs::create_dir_all(&path).unwrap();
                continue;
            }
            tar::EntryType::Symlink => {
                let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                let path = Path::new(&unpack_to).join(path.clone());
                let target_path = Path::new(&unpack_to).join(target_path);
                info!("Creating symlink {:?} -> {:?}", &path, &target_path);
                std::os::unix::fs::symlink(&target_path, &path).unwrap();
                continue;
            }
            tar::EntryType::Link => {
                let target_path = entry.link_name().unwrap().unwrap().display().to_string();
                let path = Path::new(&unpack_to).join(path.clone());
                let target_path = Path::new(&unpack_to).join(target_path);
                // the target_path should already exist in the same archive.
                assert!(target_path.is_file());
                info!("Creating hard link {:?} -> {:?}", &path, &target_path);
                std::fs::hard_link(&target_path, &path).unwrap();
                continue;
            }
            _ => {}
        }

        if path.contains("split-metadata") {
            // load the data
            let mut buf = String::new();
            entry.read_to_string(&mut buf).unwrap();
            let split_metadata: SplitMetadata = serde_json::from_str(&buf).unwrap();

            assert!(maybe_split_metadata.is_none());
            maybe_split_metadata = Some(split_metadata.clone());

            let path = Path::new(&unpack_to).join(split_metadata.path);
            ensure_parent_dir_exists(&path);

            {
                let _lock = fallocate_lock.lock().unwrap();
                if !path.is_file() {
                    let f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open(&path)
                        .unwrap();
                    vmm_sys_util::fallocate::fallocate(
                        &f,
                        vmm_sys_util::fallocate::FallocateMode::ZeroRange,
                        false,
                        0,
                        split_metadata.total_size as u64,
                    )
                    .unwrap();
                    info!("Fallocated file {:?}", &path);
                } else {
                    let current_size = std::fs::metadata(&path).unwrap().len();
                    info!(
                        "File {:?} (size {}) already exists, skip falllocate",
                        &path, current_size
                    );
                }
            }

            continue;
        }

        info!("Handling regular file {:?}", &path);

        let path = Path::new(&unpack_to).join(path);
        // create directory
        ensure_parent_dir_exists(&path);
        // create and write the file
        match maybe_split_metadata {
            Some(metadata) => {
                assert!(path.is_file());
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .unwrap();
                let offset = file
                    .seek(SeekFrom::Start(metadata.start_offset as u64))
                    .unwrap();
                let num_bytes_written = std::io::copy(&mut entry, &mut file).unwrap();
                let finished_offset = file.seek(SeekFrom::Current(0)).unwrap();
                info!(
                    "Writing {:?} (size {}) to {:?} (offset {}, physical_offset {}), {} written, finished_offset {}",
                    &path,
                    &entry.header().size().unwrap(),
                    &path,
                    &metadata.start_offset,
                    &offset,
                    &num_bytes_written,
                    &finished_offset,
                );
                maybe_split_metadata = None;
            }
            None => {
                let mut file = File::create(&path).unwrap();
                info!(
                    "Writing {:?} (size {}) to {:?}",
                    &path,
                    &entry.header().size().unwrap(),
                    &path
                );
                std::io::copy(&mut entry, &mut file).unwrap();
            }
        };
    }
}

use rayon::prelude::*;

fn unpack_split_tars(location: String, unpack_to: String) {
    let mut tar_files = Path::new(&location)
        .read_dir()
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_file())
        .collect::<Vec<_>>();
    tar_files.sort();

    let fallocate_lock = Arc::new(Mutex::new(()));

    // multi-thread this later
    tar_files.into_par_iter().for_each(|path| {
        unpack_one_tar(path.clone(), unpack_to.clone(), fallocate_lock.clone());
    });
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Cli::parse();

    if args.unpack_from.is_some() {
        unpack_split_tars(args.unpack_from.unwrap(), args.unpack_to.unwrap());
        return;
    }

    let mut writer_state = match args.split_to {
        Some(split_to) => {
            assert!(
                args.split_size.is_some(),
                "Must specify split size when split_to is specified"
            );

            let parsed_size = parse_size::parse_size(args.split_size.as_ref().unwrap()).unwrap();
            info!("Splitting into files of size {:?}", parsed_size);

            Some(WriterState::new(
                parsed_size,
                split_to,
                args.compression.clone(),
                args.tar_source_from,
            ))
        }
        None => None,
    };
    // let decoder: Box<dyn Read> = match args.compression.as_str() {
    //     "zstd" => Box::new(zstd::stream::read::Decoder::new(std::io::stdin()).unwrap()),
    //     "gzip" => Box::new(flate2::bufread::GzDecoder::new(std::io::BufReader::new(
    //         std::io::stdin(),
    //     ))),
    //     "none" => Box::new(std::io::stdin()),
    //     _ => {
    //         panic!("Unknown compression type, must be either zstd or gzip");
    //     }
    // };
    let decoder: Box<dyn Read> = Box::new(std::io::stdin());
    let mut tar = tar::Archive::new(decoder);

    for file in tar.entries().unwrap() {
        let mut file = file.unwrap();

        // Write it to output tar
        if let Some(writer_state) = &mut writer_state {
            writer_state.write(file);
        }
    }

    writer_state.map(|mut writer_state| writer_state.finish_current_file());
}
