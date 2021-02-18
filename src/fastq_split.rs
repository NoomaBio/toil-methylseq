use bio::io::fastq::Record as FqRecord;
use bio::io::fastq::{Reader as FqReader, Writer as FqWriter};
use fasthash::murmur3;
use flate2::read::MultiGzDecoder;
use log::{info, warn};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

struct FqSplitter {
    bins: u32,
}

impl FqSplitter {
    fn new(bins: u32) -> Self {
        Self { bins }
    }

    fn bin_record(&self, fq_record: &FqRecord) -> usize {
        let s = murmur3::hash32(fq_record.id());
        let bucket = s % self.bins;
        bucket as usize
    }

    fn shard_file(&self, file_path: &str) -> Result<(String, Vec<String>), String> {
        let path = Path::new(file_path);
        let parent_path = path.parent().expect("should not be root");
        let filename = path
            .file_name()
            .ok_or("Failed to get input filename".to_string())?;
        let filename = filename.to_str().expect("should make string").to_string();

        let mut file_shards = Vec::with_capacity(self.bins as usize);
        let mut writers = (0..self.bins)
            .map(|bin| {
                let shard_filename = format!("{}-{:?}", &filename, bin);
                let outpath = parent_path.join(&shard_filename);
                let writer = File::create(outpath).expect("should make file");
                file_shards.push(shard_filename);
                Ok(FqWriter::new(writer))
            })
            .collect::<Result<Vec<FqWriter<File>>, String>>()?;

        let path = Path::new(file_path);
        let f = File::open(path).expect("file should be there");
        let reader = MultiGzDecoder::new(f);
        let reader = FqReader::new(reader);

        let mut i = 0u32;
        for record in reader.records() {
            let rec = record.expect("should be ok");
            let bin = self.bin_record(&rec);
            assert!(bin < writers.len());
            writers[bin].write_record(&rec).map_err(|e| e.to_string())?;
            i += 1;
        }

        info!("wrote {:?} records for {}", i, file_path);
        Ok((filename, file_shards))
    }
}

pub fn run_fastq_split(
    fastq_files: &[&str],
    bins: u32,
) -> Result<HashMap<String, Vec<String>>, String> {
    if bins % 2 != 0 {
        warn!("bins is not a power of 2..")
    }

    let mut aggregator = HashMap::new();
    for file in fastq_files {
        let (filename, file_shards) = FqSplitter::new(bins).shard_file(file)?;
        aggregator.insert(filename, file_shards);
    }
    Ok(aggregator)
}

#[cfg(test)]
mod fastq_split_tests {
    use crate::fastq_split::FqSplitter;
    use bio::io::fastq;
    use flate2::read::GzDecoder;
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::Path;

    #[test]
    fn test_fastq_splits_consistently() {
        let file_path = Path::new("resources/reads_1.fastq.gz");
        let f = File::open(file_path).expect("file should be there");
        let reader = GzDecoder::new(f);
        let fq = fastq::Reader::new(reader);
        let mut counter = HashMap::<String, usize>::new();

        let splitter = FqSplitter::new(16);

        for record in fq.records() {
            let rec = record.expect("should get record");
            let bin = splitter.bin_record(&rec);
            let added = counter.insert(rec.id().to_string(), bin);
            assert!(added.is_none());
        }

        let file_path = Path::new("resources/reads_2.fastq.gz");
        let f = File::open(file_path).expect("file should be there");
        let reader = GzDecoder::new(f);
        let fq = fastq::Reader::new(reader);

        let splitter_b = FqSplitter::new(16);
        for record in fq.records() {
            let rec = record.expect("should get record");
            let bin = splitter_b.bin_record(&rec);
            let expected = counter.get(rec.id()).expect("should have bin for record");
            assert_eq!(&bin, expected);
        }
    }
}
