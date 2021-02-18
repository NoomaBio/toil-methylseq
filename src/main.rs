mod bam_sort;
mod fastq_split;

use clap::{App, AppSettings, Arg, SubCommand};
use fastq_split::run_fastq_split;
use log::{error, info};
use std::io::{stdout, Write};
use std::process::exit;

fn main() {
    match std::env::var("TOIL_METHYLSEQ_UTILS_LOG") {
        Ok(v) => std::env::set_var("RUST_LOG", v),
        Err(_) => std::env::set_var("RUST_LOG", "info"),
    }
    env_logger::init();

    let matches = App::new("toil-methylseq-utils")
        .about("utility operations for toil-methylseq")
        .setting(AppSettings::SubcommandRequired)
        .subcommand(
            SubCommand::with_name("fastq-split")
                .arg(
                    Arg::with_name("input")
                        .long("input")
                        .short("i")
                        .help("fastq reads")
                        .multiple(true)
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("bins")
                        .long("bins")
                        .short("b")
                        .help("number of bins to shard into")
                        .takes_value(true)
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("bam-sort").arg(
                Arg::with_name("input")
                    .long("input")
                    .short("i")
                    .help("path to bam")
                    .takes_value(true)
                    .required(true),
            ),
        )
        .get_matches();

    match matches.subcommand_name() {
        Some("fastq-split") => {
            info!("running fastq-split");
            let sub_matches = matches.subcommand_matches("fastq-split").unwrap();

            let fastq_paths = sub_matches
                .values_of("input")
                .unwrap()
                .collect::<Vec<&str>>();
            let bins = sub_matches
                .value_of("bins")
                .unwrap()
                .parse::<u32>()
                .expect("failed to parse bins into valid i128");
            match run_fastq_split(&fastq_paths, bins) {
                Ok(written) => {
                    let stdout = stdout();
                    let mut handle = stdout.lock();
                    writeln!(handle, "{}", serde_json::to_string(&written).unwrap())
                        .expect("failed to return to stdout");
                    exit(0);
                }
                Err(s) => {
                    error!("fastq-split failed, {}", s);
                    exit(1);
                }
            }
        }
        Some("bam-sort") => {
            info!("running bam sort");
            let sub_matches = matches.subcommand_matches("bam-sort").unwrap();
            let bam_path = sub_matches.value_of("input").unwrap();
            match bam_sort::run_bam_sort(bam_path) {
                Ok(bam_paths) => {
                    let stdout = stdout();
                    let mut handle = stdout.lock();
                    writeln!(handle, "{}", serde_json::to_string(&bam_paths).unwrap())
                        .expect("failed to return to stdout");
                    exit(0);
                }
                Err(e) => {
                    error!("bam-sort failed, {}", e);
                    exit(1);
                }
            }
        }
        _ => {
            error!("unknown command");
            exit(1);
        }
    }
}
