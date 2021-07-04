use std::{collections::{HashMap, hash_map::DefaultHasher}, convert::TryInto, hash::Hasher};

use static_assertions::const_assert;

pub(crate) const STRING_INTERN_THRESHOLD: usize = 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StringId {
    /// [1 bit] => is_local

    /// Not local:
    /// [26 bits] => index in all_strings
    /// [5 bits] => length

    /// Local:
    /// [31 bits] => index in strings_local
    bits: u32
}

// The number of bits for the string length in the
// the bitfield is 5
const_assert!(STRING_INTERN_THRESHOLD < (1 << 5));

impl StringId {
    pub(crate) fn new(bits: u32) -> Self {
        Self { bits }
    }

    pub(crate) fn as_u32(self) -> u32 {
        self.bits
    }

    fn is_local(self) -> bool {
        (self.bits >> 31) != 0
    }

    fn get_local_index(self) -> usize {
        (self.bits & 0x7FFFFFFF) as usize
    }

    fn get_start_end(self) -> (usize, usize) {
        let start = (self.bits >> 5) as usize;
        let length = (self.bits & 0b11111) as usize;

        (start, start + length)
    }
}

#[derive(Debug)]
pub struct StringsMemoryUsage {
    map_cap: usize,
    all_cap: usize,
    local_len: usize,
    local_cap: usize,
}

#[derive(Default)]
struct BigStrings {
    strings: String,
    offsets: Vec<(u32, u32)>,
}

impl BigStrings {
    fn push(&mut self, s: &str) -> u32 {
        let start = self.strings.len();
        self.strings.push_str(s);
        let end = self.strings.len();

        let index = self.offsets.len();
        self.offsets.push((start.try_into().unwrap(), end.try_into().unwrap()));

        index.try_into().unwrap()
    }

    fn get_str(&self, index: usize) -> Option<&str> {
        let index: u32 = index.try_into().unwrap();

        let (start, end) = self.offsets.get(index as usize).copied()?;
        let start: usize = start.try_into().unwrap();
        let end: usize = end.try_into().unwrap();

        self.strings.get(start..end)
    }

    fn clear(&mut self) {
        self.strings.clear();
        self.offsets.clear();
    }
}

#[derive(Default)]
pub struct StringInterner {
    string_to_offset: HashMap<u64, StringId>,
    all_strings: String,
    big_strings: BigStrings,
}

impl StringInterner {
    pub fn memory_usage(&self) -> StringsMemoryUsage {
        StringsMemoryUsage {
            map_cap: self.string_to_offset.capacity(),
            all_cap: self.all_strings.capacity(),
            local_len: self.big_strings.strings.len(),
            local_cap: self.big_strings.strings.capacity(),
        }
    }

    pub fn get_string_id(&mut self, s: &str) -> StringId {
        if s.len() >= STRING_INTERN_THRESHOLD {
            // let index: u32 = self.strings_local.len().try_into().unwrap();
            // self.strings_local.push(Box::from(s));

            let index = self.big_strings.push(s);

            return StringId {
                bits: 1 << 31 | index
            };
        }

        let mut hasher = DefaultHasher::new();
        hasher.write(s.as_bytes());
        let hashed = hasher.finish();

        if let Some(string_id) = self.string_to_offset.get(&hashed) {
            return *string_id
        }

        let index: u32 = self.all_strings.len().try_into().unwrap();
        let length: u32 = s.len().try_into().unwrap();

        assert_eq!(index & !0x3FFFFFF, 0);
        assert_eq!(length >> 5, 0);

        self.all_strings.push_str(s);

        let string_id = StringId {
            bits: index << 5 | length
        };

        self.string_to_offset.insert(hashed, string_id);

        debug_assert_eq!(s, self.get(string_id).unwrap());

        string_id
    }

    pub fn get(&self, string_id: StringId) -> Option<&str> {
        if string_id.is_local() {
            return self.big_strings.get_str(string_id.get_local_index());
            // return self.strings_local.get(string_id.get_local_index()).map(|s| &**s)
        }

        let (start, end) = string_id.get_start_end();
        self.all_strings.get(start..end)
    }

    pub fn clear(&mut self) {
        self.big_strings.clear();
        // self.strings_local.clear();
        //self.strings_local = Vec::with_capacity(32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_interner() {
        let mut interner = StringInterner::default();

        let a = interner.get_string_id("a");
        let b = interner.get_string_id("a");

        assert_eq!(a, b);
        assert!(!a.is_local());
        assert_eq!(interner.get(a), Some("a"));
        assert_eq!(interner.get(a), interner.get(b));

        let long_str = std::iter::repeat("a").take(STRING_INTERN_THRESHOLD).collect::<String>();

        let a = interner.get_string_id(&long_str);
        let b = interner.get_string_id(&long_str);
        assert_ne!(a, b);
        assert!(a.is_local());
        assert_eq!(interner.get(a).unwrap(), long_str);
        assert_eq!(interner.get(b).unwrap(), long_str);
    }
}




// #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
// pub struct TreeStorageId {
//     /// [30 bits] => start,
//     /// [20 bits] => length,
//     bits: u64,
// }

// impl Default for TreeStorageId {
//     fn default() -> Self {
//         Self::empty()
//     }
// }

// impl TreeStorageId {
//     fn new(start: usize, end: usize) -> Self {
//         let length = end.checked_sub(start).unwrap();

//         assert_eq!(start & !0x3FFFFFFF, 0);
//         assert_eq!(length & !0xFFFFF, 0);

//         let bits = (start as u64) << 20 | length as u64;

//         Self {
//             bits,
//         }
//     }

//     fn get(self) -> (usize, usize) {
//         let start = (self.bits >> 20) as usize;
//         let length = (self.bits & 0xFFFFF) as usize;

//         (start, start + length)
//     }

//     pub fn empty() -> Self {
//         Self::new(0, 0)
//     }

//     pub fn is_empty(&self) -> bool {
//         let length = self.bits | 0xFFFFF;
//         length == 0
//     }
// }

// impl Into<u64> for TreeStorageId {
//     fn into(self) -> u64 {
//         self.bits
//     }
// }

// impl From<NodeEntry> for TreeStorageId {
//     fn from(entry: NodeEntry) -> Self {
//         Self { bits: entry.entry_id() }
//     }
// }



// #[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
// pub struct BlobStorageId {
//     /// [28 bits] => start,
//     /// [22 bits] => length,
//     bits: u64,
//     // start: u32,
//     // end: NonZeroU32,
// }

// impl Into<u64> for BlobStorageId {
//     fn into(self) -> u64 {
//         self.bits
//         // (self.start as u64) << 32 | self.end.get() as u64
//     }
// }

// impl From<NodeEntry> for BlobStorageId {
//     fn from(entry: NodeEntry) -> Self {
//         Self { bits: entry.entry_id() }
//         // let blob_id = entry.entry_id() as usize;
//         // Self {
//         //     start: (blob_id >> 32) as u32,
//         //     end: NonZeroU32::new((blob_id & 0xFFFFFFFF) as u32).unwrap(),
//         // }
//     }
// }

// impl BlobStorageId {
//     fn new(start: usize, end: usize) -> Self {
//         let length = end.checked_sub(start).unwrap();

//         assert_eq!(start & !0xFFFFFFF, 0);
//         assert_eq!(length & !0x3FFFFF, 0);

//         let bits = ((start as u64) << 22) | length as u64;

//         let res = Self {
//             bits,
//         };

//         assert_eq!(res.get(), (start, end));

//         res

//         // Self {
//         //     start: start.try_into().unwrap(),
//         //     end: NonZeroU32::new(end.checked_add(1).unwrap().try_into().unwrap()).unwrap(),
//         // }
//     }

//     fn get(self) -> (usize, usize) {
//         let start = (self.bits >> 22) as usize;
//         let length = (self.bits & 0x3FFFFF) as usize;

//         (start, start + length)
//         // (
//         //     self.start as usize,
//         //     self.end.get().checked_sub(1).unwrap() as usize,
//         // )
//     }
// }
