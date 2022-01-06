use byte_unit::Byte;

use tezos_context::{working_tree::working_tree::WorkingTreeStatistics, ObjectHash};

pub struct DebugWorkingTreeStatistics(pub WorkingTreeStatistics);

impl std::fmt::Debug for DebugWorkingTreeStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut blobs_stats = Vec::with_capacity(self.0.blobs_by_length.len());
        for stats in self.0.blobs_by_length.values() {
            blobs_stats.push(stats);
        }
        blobs_stats.sort_by_key(|stats| stats.size);

        let hashes = format!(
            "{{ total: {:>8}, unique: {:>9} }}",
            &self.0.nhashes,
            &self.0.unique_hash.len()
        );
        let objects = format!(
            "{{ total: {:>8}, inlined: {:>8}, not inlined: {:>8} }}",
            &self.0.nobjects,
            &self.0.nobjects_inlined,
            self.0.nobjects - self.0.nobjects_inlined
        );

        let total_bytes = self.0.nhashes * std::mem::size_of::<ObjectHash>()
            + self.0.strings_total_bytes
            + self.0.objects_total_bytes;

        struct BytesDisplay {
            bytes: usize,
        }

        impl std::fmt::Debug for BytesDisplay {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                let bytes_str = Byte::from_bytes(self.bytes as u64)
                    .get_appropriate_unit(false)
                    .to_string();

                f.write_fmt(format_args!("{:>8} ({:>8})", self.bytes, bytes_str,))
            }
        }

        f.debug_struct("WorkingTreeStatistics")
            .field("blobs_by_length", &blobs_stats)
            .field("max_depth", &self.0.max_depth)
            .field(
                "oldest_reference (offset in data.db file) ",
                &self.0.lowest_offset,
            )
            .field("number_of_objects", &objects)
            .field("number_of_hashes ", &hashes)
            .field("number_of_shapes ", &self.0.nshapes)
            .field(
                "hashes_total_bytes (hashes.db file)",
                &BytesDisplay {
                    bytes: self.0.nhashes * std::mem::size_of::<ObjectHash>(),
                },
            )
            .field(
                "strings_total_bytes (small & big strings)",
                &BytesDisplay {
                    bytes: self.0.strings_total_bytes,
                },
            )
            .field(
                "objects_total_bytes (data.db file)",
                &BytesDisplay {
                    bytes: self.0.objects_total_bytes,
                },
            )
            .field(
                "shapes_total_bytes (shape_directories.db file)",
                &BytesDisplay {
                    bytes: self.0.shapes_total_bytes,
                },
            )
            .field("total_bytes", &BytesDisplay { bytes: total_bytes })
            .finish()
    }
}
