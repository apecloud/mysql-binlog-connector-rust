use std::{collections::HashMap, fmt::Display};

use crate::binlog_error::BinlogError;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Interval {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GtidSet {
    /// Keyed by transaction source ID: `uuid` or `uuid:tag`, in lowercase.
    pub map: HashMap<String, UuidSet>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UuidSet {
    /// Transaction source ID: `uuid` for untagged GTIDs, `uuid:tag` for tagged GTIDs.
    pub uuid: String,
    pub intervals: Vec<Interval>,
}

impl Interval {
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    pub fn is_contained_within(&self, other: &Interval) -> bool {
        self.start >= other.start && self.end <= other.end
    }
}

impl UuidSet {
    pub fn new(uuid: String, intervals: Vec<Interval>) -> Self {
        let uuid = uuid
            .split(':')
            .map(str::trim)
            .collect::<Vec<_>>()
            .join(":")
            .to_ascii_lowercase();
        let mut me = Self { uuid, intervals };
        me.intervals.sort_unstable_by_key(|interval| interval.start);
        if me.intervals.len() > 1 {
            me.join_adjacent_intervals(0)
        }
        me
    }

    pub(crate) fn source_id_parts(source_id: &str) -> Result<(&str, Option<&str>), BinlogError> {
        let invalid = || BinlogError::InvalidGtid(source_id.to_string());
        let (uuid, tag) = match source_id.split_once(':') {
            Some((uuid, tag)) => (uuid.trim(), Some(tag.trim())),
            None => (source_id.trim(), None),
        };
        if uuid.len() != 36
            || !uuid.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
            || tag.is_some_and(|tag| !GtidSet::is_tag(tag))
        {
            return Err(invalid());
        }
        Ok((uuid, tag))
    }

    pub fn add(&mut self, transaction_id: u64) -> bool {
        let index = self.find_interval(transaction_id);
        let mut added_to_existing = false;

        if index < self.intervals.len() {
            let interval = &mut self.intervals[index];
            if interval.start.checked_sub(1) == Some(transaction_id) {
                interval.start = transaction_id;
                added_to_existing = true;
            } else if interval.end.checked_add(1) == Some(transaction_id) {
                interval.end = transaction_id;
                added_to_existing = true;
            } else if interval.start <= transaction_id && transaction_id <= interval.end {
                return false;
            }
        }

        if !added_to_existing {
            self.intervals.insert(
                index,
                Interval {
                    start: transaction_id,
                    end: transaction_id,
                },
            );
        }

        if self.intervals.len() > 1 {
            self.join_adjacent_intervals(index.saturating_sub(1));
        }
        true
    }

    fn is_contained_within(&self, other: &UuidSet) -> bool {
        if self.uuid != other.uuid {
            return false;
        }

        // every interval in this must be within an interval of the other
        for i in self.intervals.iter() {
            let mut found = false;
            for o in other.intervals.iter() {
                if i.is_contained_within(o) {
                    found = true;
                    break;
                }
            }
            if !found {
                return false;
            }
        }
        true
    }

    fn join_adjacent_intervals(&mut self, mut index: usize) {
        while index + 1 < self.intervals.len() {
            if self.intervals[index + 1].start <= self.intervals[index].end.saturating_add(1) {
                self.intervals[index].end =
                    self.intervals[index].end.max(self.intervals[index + 1].end);
                self.intervals.remove(index + 1);
            } else {
                index += 1;
            }
        }
    }

    fn find_interval(&self, v: u64) -> usize {
        let mut l = 0;
        let mut r = self.intervals.len();
        let mut p = 0;

        while l < r {
            p = (l + r) / 2;
            let i = &self.intervals[p];
            if i.end < v {
                l = p + 1;
            } else if v < i.start {
                r = p;
            } else {
                return p;
            }
        }

        if !self.intervals.is_empty() && self.intervals[p].end < v {
            p += 1;
        }
        p
    }
}

impl Display for UuidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = self.uuid.clone();
        for interval in &self.intervals {
            result += &format!(":{}-{}", interval.start, interval.end);
        }
        write!(f, "{}", result)
    }
}

// refer to: https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html
impl GtidSet {
    pub fn new(gtid_set: &str) -> Result<Self, BinlogError> {
        let mut map: HashMap<String, UuidSet> = HashMap::new();
        let lines = gtid_set.replace('\n', "");
        // MySQL allows tag switches within one UUID: uuid:1-2:admin:1:biz:1.
        // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/sql/rpl_gtid_set.cc#L440-L577
        for entry in lines
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let invalid = || BinlogError::InvalidGtid(entry.to_string());
            let mut parts = entry.split(':').map(str::trim);
            let uuid = parts.next().ok_or_else(invalid)?;
            UuidSet::source_id_parts(uuid)?;
            let uuid = uuid.to_ascii_lowercase();
            let mut source_id = uuid.clone();
            let mut has_interval = false;
            for part in parts {
                if Self::is_tag(part) {
                    if !has_interval && source_id != uuid {
                        return Err(invalid());
                    }
                    source_id = format!("{}:{}", uuid, part.to_ascii_lowercase());
                    has_interval = false;
                } else {
                    let mut bounds = part.split('-').map(str::trim);
                    let start =
                        Self::parse_interval_num(bounds.next().ok_or_else(invalid)?, entry)?;
                    let end = bounds
                        .next()
                        .map(|number| Self::parse_interval_num(number, entry))
                        .transpose()?
                        .unwrap_or(start);
                    if bounds.next().is_some() || end < start || (source_id != uuid && start == 0) {
                        return Err(invalid());
                    }
                    map.entry(source_id.clone())
                        .or_insert_with(|| UuidSet {
                            uuid: source_id.clone(),
                            intervals: Vec::new(),
                        })
                        .intervals
                        .push(Interval { start, end });
                    has_interval = true;
                }
            }
            if !has_interval {
                return Err(invalid());
            }
        }
        for set in map.values_mut() {
            *set = UuidSet::new(set.uuid.clone(), std::mem::take(&mut set.intervals));
        }
        Ok(GtidSet { map })
    }

    pub fn add(&mut self, gtid: &str) -> Result<bool, BinlogError> {
        let invalid = || BinlogError::InvalidGtid(gtid.to_string());
        let (source_id, number) = gtid.rsplit_once(':').ok_or_else(invalid)?;
        let (uuid, tag) = UuidSet::source_id_parts(source_id)?;
        let transaction_id = Self::parse_interval_num(number.trim(), gtid)?;
        if tag.is_some() && transaction_id == 0 {
            return Err(invalid());
        }
        let source_id = match tag {
            Some(tag) => format!("{}:{}", uuid.to_ascii_lowercase(), tag.to_ascii_lowercase()),
            None => uuid.to_ascii_lowercase(),
        };
        let uuid_set = self
            .map
            .entry(source_id.clone())
            .or_insert_with(|| UuidSet::new(source_id, Vec::new()));
        Ok(uuid_set.add(transaction_id))
    }

    fn is_tag(tag: &str) -> bool {
        // Tags are case-insensitive ASCII identifiers, up to 32 bytes.
        // https://github.com/mysql/mysql-server/blob/mysql-8.4.0/libs/mysql/gtid/tag.h#L45-L51
        !tag.is_empty()
            && tag.len() <= 32
            && (tag.as_bytes()[0].is_ascii_alphabetic() || tag.as_bytes()[0] == b'_')
            && tag
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    }

    pub fn get_uuid_sets(&self) -> Vec<&UuidSet> {
        self.map.values().collect()
    }

    pub fn put_uuid_set(&mut self, uuid_set: UuidSet) {
        let uuid_set = UuidSet::new(uuid_set.uuid, uuid_set.intervals);
        self.map.insert(uuid_set.uuid.clone(), uuid_set);
    }

    pub fn is_contained_within(&self, other: &GtidSet) -> bool {
        for (uuid, i_set) in self.map.iter() {
            if let Some(o_set) = other.map.get(uuid) {
                if !i_set.is_contained_within(o_set) {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    fn parse_interval_num(number: &str, gtid: &str) -> Result<u64, BinlogError> {
        let invalid = || BinlogError::InvalidGtid(gtid.to_string());
        let value = number.parse::<u64>().map_err(|_| invalid())?;
        // MySQL reserves i64::MAX as GNO_END. Keep the legacy untagged GNO 0 API.
        if value >= i64::MAX as u64 {
            return Err(invalid());
        }
        Ok(value)
    }
}

impl Display for GtidSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut gtids = vec![];
        let mut uuids: Vec<_> = self.map.keys().collect();
        uuids.sort();

        for key in uuids {
            let uuid_set = self.map.get(key).unwrap();
            gtids.push(uuid_set.to_string());
        }
        write!(f, "{}", gtids.join(","))
    }
}

#[cfg(test)]
mod tests {

    use crate::command::gtid_set::Interval;

    use super::GtidSet;

    const UUID: &'static str = "24bc7850-2c16-11e6-a073-0242ac110002";

    #[test]
    fn test_tagged_parse_and_roundtrip() {
        let text = format!(
            "{0}:1-2:Admin:1-3:biz:1,{0}:admin:3-5,{0}:6-8",
            UUID.to_ascii_uppercase()
        );
        let set = GtidSet::new(&text).unwrap();
        assert_eq!(set.map.len(), 3);
        assert_eq!(
            set.to_string(),
            format!("{0}:1-2:6-8,{0}:admin:1-5,{0}:biz:1-1", UUID)
        );
        assert_eq!(GtidSet::new(&set.to_string()).unwrap(), set);
        for (source_id, value) in &set.map {
            assert_eq!(source_id, &value.uuid);
        }
    }

    #[test]
    fn test_tagged_add_isolates_sources() {
        let mut set = GtidSet::new("").unwrap();
        for suffix in ["1", "admin:1", "biz:1", "admin:3", "admin:2", "2", "biz:2"] {
            assert!(set.add(&format!("{}:{}", UUID, suffix)).unwrap());
        }
        assert!(!set
            .add(&format!("{}:ADMIN:1", UUID.to_ascii_uppercase()))
            .unwrap());
        assert_eq!(
            set.to_string(),
            format!("{0}:1-2,{0}:admin:1-3,{0}:biz:1-2", UUID)
        );
        assert_eq!(GtidSet::new(&set.to_string()).unwrap(), set);
    }

    #[test]
    fn test_tagged_subset_does_not_cross_tags() {
        let admin = GtidSet::new(&format!("{}:admin:1-2", UUID)).unwrap();
        let biz = GtidSet::new(&format!("{}:biz:1-2", UUID)).unwrap();
        let untagged = GtidSet::new(&format!("{}:1-2", UUID)).unwrap();
        let mixed = GtidSet::new(&format!("{}:admin:1-3:biz:1-4", UUID)).unwrap();
        assert!(admin.is_contained_within(&mixed));
        assert!(biz.is_contained_within(&mixed));
        assert!(!mixed.is_contained_within(&admin));
        assert!(!admin.is_contained_within(&biz));
        assert!(!admin.is_contained_within(&untagged));
        assert!(!untagged.is_contained_within(&mixed));
    }

    #[test]
    fn test_put_tagged_uuid_set_preserves_other_sources() {
        let mut set = GtidSet::new(&format!("{}:1:admin:1:biz:1", UUID)).unwrap();
        set.put_uuid_set(super::UuidSet::new(
            format!("{}: Admin", UUID.to_ascii_uppercase()),
            vec![Interval::new(3, 4), Interval::new(2, 3)],
        ));
        assert_eq!(set.get_uuid_sets().len(), 3);
        assert_eq!(
            set.to_string(),
            format!("{0}:1-1,{0}:admin:2-4,{0}:biz:1-1", UUID)
        );
    }

    #[test]
    fn test_repeated_sources_and_overlapping_intervals_merge() {
        let mut set = GtidSet::new(&format!(
            "{0}:admin:9-12:2-3:1-10:14-18,{0}:5-7,{0}:1-3:3-5",
            UUID
        ))
        .unwrap();
        assert!(set.add(&format!("{}:admin:13", UUID)).unwrap());
        assert!(!set.add(&format!("{}:admin:14", UUID)).unwrap());
        assert_eq!(set.to_string(), format!("{0}:1-7,{0}:admin:1-18", UUID));
    }

    #[test]
    fn test_tagged_add_rejects_invalid_input_without_mutation() {
        let mut set = GtidSet::new(&format!("{}:admin:1", UUID)).unwrap();
        let before = set.to_string();
        for suffix in [
            "admin:0",
            "admin:1-2",
            "admin:9223372036854775807",
            "admin:18446744073709551615",
            "admin:1:2",
            "bad-tag:1",
            "admin:",
        ] {
            assert!(set.add(&format!("{}:{}", UUID, suffix)).is_err());
            assert_eq!(set.to_string(), before);
        }
        let maximum = i64::MAX as u64 - 1;
        assert!(set.add(&format!("{}:admin:{}", UUID, maximum)).unwrap());
        assert_eq!(GtidSet::new(&set.to_string()).unwrap(), set);
    }

    #[test]
    fn test_leading_plus_preserves_numeric_parsing_compatibility() {
        let mut set = GtidSet::new(&format!("{}:+1-+2:admin:+1-+2", UUID)).unwrap();
        assert!(set.add(&format!("{}:+3", UUID)).unwrap());
        assert!(set.add(&format!("{}:admin:+3", UUID)).unwrap());
        assert_eq!(set.to_string(), format!("{0}:1-3,{0}:admin:1-3", UUID));
    }

    #[test]
    fn test_add() {
        let mut gtid_set = GtidSet::new("00000000-0000-0000-0000-000000000000:3-5").unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000000:2")
            .unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000000:4")
            .unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000000:5")
            .unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000000:7")
            .unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000001:9")
            .unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000000:0")
            .unwrap();
        assert_eq!(gtid_set.to_string(),
            "00000000-0000-0000-0000-000000000000:0-0:2-5:7-7,00000000-0000-0000-0000-000000000001:9-9");
    }

    #[test]
    fn test_join() {
        let mut gtid_set = GtidSet::new("00000000-0000-0000-0000-000000000000:3-4:6-7").unwrap();
        gtid_set
            .add("00000000-0000-0000-0000-000000000000:5")
            .unwrap();
        let first_interval = gtid_set
            .get_uuid_sets()
            .first()
            .unwrap()
            .intervals
            .first()
            .unwrap();
        assert_eq!(first_interval.end, 7);
        assert_eq!(
            gtid_set.to_string(),
            "00000000-0000-0000-0000-000000000000:3-7"
        );
    }

    #[test]
    fn test_empty_set() {
        assert_eq!(GtidSet::new("").unwrap().to_string(), "");
    }

    #[test]
    fn test_equals() {
        assert_eq!(GtidSet::new("").unwrap(), GtidSet::new("").unwrap());
        assert_eq!(
            GtidSet::new(&format!("{}:1-191", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-191", UUID)).unwrap()
        );
        assert_eq!(
            GtidSet::new(&format!("{}:1-191:192-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-191:192-199", UUID)).unwrap()
        );
        assert_eq!(
            GtidSet::new(&format!("{}:1-191:192-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-199", UUID)).unwrap()
        );
        assert_eq!(
            GtidSet::new(&format!("{}:1-191:193-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-191:193-199", UUID)).unwrap()
        );
        assert_ne!(
            GtidSet::new(&format!("{}:1-191:193-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-199", UUID)).unwrap()
        );
    }

    #[test]
    fn test_subset_of() {
        let set = vec![
            GtidSet::new("").unwrap(),
            GtidSet::new(&format!("{}:1-191", UUID)).unwrap(),
            GtidSet::new(&format!("{}:192-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-191:192-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-191:193-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:2-199", UUID)).unwrap(),
            GtidSet::new(&format!("{}:1-200", UUID)).unwrap(),
        ];

        let subset_matrix = &[
            &[1, 1, 1, 1, 1, 1, 1],
            &[0, 1, 0, 1, 1, 0, 1],
            &[0, 0, 1, 1, 0, 1, 1],
            &[0, 0, 0, 1, 0, 0, 1],
            &[0, 0, 0, 1, 1, 0, 1],
            &[0, 0, 0, 1, 0, 1, 1],
            &[0, 0, 0, 0, 0, 0, 1],
        ];

        for (i, subset) in subset_matrix.iter().enumerate() {
            for (j, &is_subset) in subset.iter().enumerate() {
                assert_eq!(
                    set[i].is_contained_within(&set[j]),
                    is_subset == 1,
                    "\"{:?}\" was expected to be a subset of \"{:?}\" ({},{})",
                    set[i],
                    set[j],
                    i,
                    j
                );
            }
        }
    }

    #[test]
    fn test_single_interval() {
        let gtid_set = GtidSet::new(&format!("{}:1-191", UUID)).unwrap();
        let uuid_set = gtid_set.map.get(UUID).unwrap();
        assert_eq!(uuid_set.intervals.len(), 1);
        assert!(uuid_set.intervals.contains(&Interval::new(1, 191)));
        assert_eq!(
            uuid_set.intervals.iter().next(),
            Some(&Interval::new(1, 191))
        );
        assert_eq!(uuid_set.intervals.last(), Some(&Interval::new(1, 191)));
        assert_eq!(gtid_set.to_string(), format!("{}:1-191", UUID));
    }

    #[test]
    fn test_collapse_adjacent_intervals() {
        let gtid_set = GtidSet::new(&format!("{}:1-191:192-199", UUID)).unwrap();
        let uuid_set = gtid_set.map.get(UUID).unwrap();
        assert_eq!(uuid_set.intervals.len(), 1);
        assert!(uuid_set.intervals.contains(&Interval::new(1, 199)));
        assert_eq!(
            uuid_set.intervals.iter().next(),
            Some(&Interval::new(1, 199))
        );
        assert_eq!(uuid_set.intervals.last(), Some(&Interval::new(1, 199)));
        assert_eq!(gtid_set.to_string(), format!("{}:1-199", UUID));
    }

    #[test]
    fn test_not_collapse_non_adjacent_intervals() {
        let gtid_set = GtidSet::new(&format!("{}:1-191:193-199", UUID)).unwrap();
        let uuid_set = gtid_set.map.get(UUID).unwrap();
        assert_eq!(uuid_set.intervals.len(), 2);
        assert_eq!(
            uuid_set.intervals.iter().next(),
            Some(&Interval::new(1, 191))
        );
        assert_eq!(uuid_set.intervals.last(), Some(&Interval::new(193, 199)));
        assert_eq!(gtid_set.to_string(), format!("{}:1-191:193-199", UUID));
    }

    #[test]
    fn test_multiple_intervals() {
        let gtid_set = GtidSet::new(&format!("{}:1-191:193-199:1000-1033", UUID)).unwrap();
        let uuid_set = gtid_set.map.get(UUID).unwrap();
        assert_eq!(uuid_set.intervals.len(), 3);
        assert!(uuid_set.intervals.contains(&Interval::new(193, 199)));
        assert_eq!(uuid_set.intervals.first(), Some(&Interval::new(1, 191)));
        assert_eq!(uuid_set.intervals.last(), Some(&Interval::new(1000, 1033)));
        assert_eq!(
            gtid_set.to_string(),
            format!("{}:1-191:193-199:1000-1033", UUID)
        );
    }

    #[test]
    fn test_multiple_intervals_that_may_be_adjacent() {
        let gtid_set = GtidSet::new(&format!(
            "{}:1-191:192-199:1000-1033:1035-1036:1038-1039",
            UUID
        ))
        .unwrap();
        let uuid_set = gtid_set.map.get(UUID).unwrap();
        assert_eq!(uuid_set.intervals.len(), 4);
        assert!(uuid_set.intervals.contains(&Interval::new(1000, 1033)));
        assert!(uuid_set.intervals.contains(&Interval::new(1035, 1036)));
        assert_eq!(uuid_set.intervals.first(), Some(&Interval::new(1, 199)));
        assert_eq!(uuid_set.intervals.last(), Some(&Interval::new(1038, 1039)));
        assert_eq!(
            gtid_set.to_string(),
            format!("{}:1-199:1000-1033:1035-1036:1038-1039", UUID)
        );
    }

    #[test]
    fn test_put_uuid_set() {
        let mut gtid_set = GtidSet::new(&format!("{}:1-191", UUID)).unwrap();
        let gtid_set2 = GtidSet::new(&format!("{}:1-190", UUID)).unwrap();
        let uuid_set2 = gtid_set2.map.get(UUID).unwrap();
        gtid_set.put_uuid_set(uuid_set2.clone());
        assert_eq!(gtid_set, gtid_set2);
    }
    #[test]
    fn test_tagged_set_rejects_invalid_input() {
        for suffix in [
            "admin",
            "admin:biz:1",
            "admin:0",
            "admin:2-1",
            "admin:1-2-3",
            "admin:18446744073709551615",
            "admin:9223372036854775807",
            "admin:1:",
            "admin:1:bad-tag:2",
            "admin:1:abcdefghijklmnopqrstuvwxyz_123456:2",
            "admin:1:中文:2",
        ] {
            assert!(
                GtidSet::new(&format!("{}:{}", UUID, suffix)).is_err(),
                "{}",
                suffix
            );
        }
        for uuid in [
            "not-a-uuid",
            "8f555992-d370-11f1-a239-0242ac11000é",
            "8f555992xd370-11f1-a239-0242ac110002",
        ] {
            assert!(GtidSet::new(&format!("{}:admin:1", uuid)).is_err());
        }
    }
}
