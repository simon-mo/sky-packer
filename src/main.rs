use log::info;
use std::{
    collections::HashSet,
    io::{Read, Write},
};

use clap::Parser;

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
}

impl WriterState {
    fn new(split_size: u64, split_to: String, compression: String) -> Self {
        Self {
            current_split_file: None,
            split_file_name: "".to_string(),
            current_split_file_size: 0,
            num_split_files_completed: 0,
            current_file_contains_path: HashSet::new(),
            split_size,
            split_to,
            compression,
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
                key == "size"
            }) {
                let real_size = size.value().unwrap().parse::<u64>().unwrap();
                info!(
                    "Sparse file detected, header claim size is {}, real size is {}, using real size",
                    file_size, real_size
                );
                file_size = real_size;
            }
        }
        if self.current_split_file_size + file_size > self.split_size && file_size > 0 {
            info!(
                "Advancing to next file because {} + {} > {}",
                self.current_split_file_size, file_size, self.split_size
            );
            if self.current_split_file.is_some() {
                self.current_split_file.as_mut().unwrap().finish().unwrap();
            }
            self.current_split_file = None;
            self.current_file_contains_path.clear();
            self.current_split_file_size = 0;
            self.num_split_files_completed += 1;
        }

        if self.current_split_file.is_none() {
            self.split_file_name =
                format!("{}.{:03}", self.split_to, self.num_split_files_completed);
            let split_file = std::fs::File::create(&self.split_file_name).unwrap();
            let split_file: Box<dyn Write> = match self.compression.as_str() {
                "zstd" => Box::new(zstd::stream::write::Encoder::new(split_file, 3).unwrap()),
                "gzip" => Box::new(flate2::write::GzEncoder::new(
                    split_file,
                    flate2::Compression::fast(),
                )),
                "none" => Box::new(split_file),
                _ => {
                    panic!("Unknown compression type, must be either zstd or gzip");
                }
            };
            self.current_split_file = Some(tar::Builder::new(split_file));
        }

        let current_split_file = self.current_split_file.as_mut().unwrap();
        let path = entry.path().unwrap().display().to_string();
        info!(
            "Writing {} (size {}) to {}",
            &path, &file_size, self.split_file_name
        );

        if entry.header().entry_type().is_hard_link() {
            let target_path = entry.link_name().unwrap().unwrap().display().to_string();
            assert!(
                    self.current_file_contains_path.contains(&target_path),
                    "Current file {} is a hard link to {}, but the target file isn't in this archive. This will cause trouble during extraction",
                    &path,
                    &target_path
                );
        }
        self.current_file_contains_path.insert(path);

        current_split_file
            .append(&entry.header().clone(), entry)
            .unwrap();
        self.current_split_file_size += file_size;
    }
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let args = Cli::parse();

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
            ))
        }
        None => None,
    };
    let decoder: Box<dyn Read> = match args.compression.as_str() {
        "zstd" => Box::new(zstd::stream::read::Decoder::new(std::io::stdin()).unwrap()),
        "gzip" => Box::new(flate2::bufread::GzDecoder::new(std::io::BufReader::new(
            std::io::stdin(),
        ))),
        "none" => Box::new(std::io::stdin()),
        _ => {
            panic!("Unknown compression type, must be either zstd or gzip");
        }
    };
    let mut tar = tar::Archive::new(decoder);

    for file in tar.entries().unwrap() {
        let mut file = file.unwrap();

        // Log and hash
        let data: json::JsonValue = match file.header().entry_type() {
            tar::EntryType::Regular => {
                let path = file.path().unwrap().display().to_string();
                // For some reason the size sometimes return 0. It might be due to a hardlink (?)
                // We default to the value in header, but then also compute the size ourselves
                // (if we are hasing)
                let (size, file_digest) = if args.hash {
                    let mut size_computed = 0;

                    let mut hasher = crc32fast::Hasher::new();

                    loop {
                        let mut buf = [0; 4194304]; // 4MB
                        let n = file.read(&mut buf).unwrap();
                        size_computed += n;

                        if n == 0 {
                            break;
                        }
                        hasher.update(&buf[..n]);
                    }

                    let file_digest: u32 = hasher.finalize();

                    (size_computed, file_digest)
                } else {
                    (file.header().size().unwrap() as usize, 0 as u32)
                };

                json::object! {
                    path: path,
                    size: size,
                    file_type: "regular",
                    file_digest: file_digest,
                }
            }
            tar::EntryType::Directory => {
                json::object! {
                    path: file.path().unwrap().display().to_string(),
                    size: 0,
                    file_type: "directory",
                    file_digest: 0,
                }
            }
            tar::EntryType::Symlink => {
                json::object! {
                    path: file.path().unwrap().display().to_string(),
                    size: 0,
                    file_type: "symlink",
                    file_digest: 0,
                    link_name: file.link_name().unwrap().unwrap().display().to_string(),
                }
            }
            tar::EntryType::Link => {
                json::object! {
                    path: file.path().unwrap().display().to_string(),
                    size: 0,
                    file_type: "link",
                    file_digest: 0,
                    link_name: file.link_name().unwrap().unwrap().display().to_string(),
                }
            }
            _ => {
                panic!(
                    "Unknown file type {:?} {:?}",
                    file.header().entry_type(),
                    file.path().unwrap()
                );
            }
        };
        println!("\n{}", data);

        // Write it to output tar
        if let Some(writer_state) = &mut writer_state {
            writer_state.write(file);
        }
    }
}
