use std::collections::{HashMap, HashSet};
use std::path::Path;

use rust_htslib::{
    bam, bam::Read, bam::Reader as BamReader, bam::Record, bam::Writer as BamWriter,
};

const KEEP_CHROMS: &'static [&'static str] = &[
    "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11",
    "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21",
    "chr22",
];

pub fn run_bam_sort(p: &str) -> Result<Vec<(String, String)>, String> {
    let mut bam_reader = BamReader::from_path(p).map_err(|e| e.to_string())?;
    let mut record = Record::new();

    let keep_chroms = KEEP_CHROMS.iter().collect::<HashSet<_>>();
    let path = Path::new(p);
    let parent_path = path.parent().ok_or("should not be root".to_string())?;
    let filename = path.file_stem().ok_or("should get filename".to_string())?;
    let filename = filename.to_str().ok_or("should make string".to_string())?;

    let header = bam::Header::from_template(bam_reader.header());
    let targets = bam_reader
        .header()
        .target_names()
        .iter()
        .map(|t| String::from_utf8_lossy(t).to_string())
        .collect::<Vec<String>>();

    let mut writers = HashMap::new();
    let writer_maker = |chrom: &str| -> (String, BamWriter) {
        let chrom_filename = format!("{}_{}.bam", chrom, filename);
        let writer_path = Path::new(parent_path).join(&chrom_filename);
        (
            chrom_filename,
            BamWriter::from_path(writer_path, &header, bam::Format::BAM)
                .expect("should make writer"),
        )
    };

    let mut writer_paths = Vec::new();
    while let Some(result) = bam_reader.read(&mut record) {
        match result {
            Ok(_) => {
                let tid = record.tid() as usize;
                assert!(tid < targets.len());
                let target_name = targets[tid].as_str();
                if keep_chroms.contains(&target_name) {
                    let writer = writers.entry(target_name).or_insert_with(|| {
                        let (file_path, writer) = writer_maker(target_name);
                        writer_paths.push((target_name.to_string(), file_path));
                        writer
                    });
                    writer.write(&record).map_err(|e| e.to_string())?;
                }
            }
            Err(_) => panic!("should work!"),
        }
    }
    Ok(writer_paths)
}

#[cfg(test)]
mod bam_sort_tests {
    use std::collections::HashMap;

    use rust_htslib::{bam, bam::Read, bam::Reader as BamReader, bam::Record};

    #[test]
    fn test_bam_sort_basic() {
        let mut bam = BamReader::from_path("resources/shard_0.bam").expect("should get reader");
        let mut record = Record::new();

        let targets = bam
            .header()
            .target_names()
            .iter()
            .map(|t| String::from_utf8_lossy(t).to_string())
            .collect::<Vec<String>>();
        dbg!(targets);

        let mut i = 0;
        while let Some(result) = bam.read(&mut record) {
            match result {
                Ok(_) => {
                    // let target_name = header.tid2name(record.tid() as u32);
                    dbg!(&record);
                    i += 1;
                    if i > 10 {
                        break;
                    }
                    ()
                }
                Err(_) => panic!("should work!"),
            }
        }
    }
}
