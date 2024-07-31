use std::{cmp, collections::HashMap, fmt::Display};

use crate::binlog_error::BinlogError;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Interval {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct GtidSet {
    pub map: HashMap<String, UuidSet>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UuidSet {
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
        let mut me = Self { uuid, intervals };
        if me.intervals.len() > 1 {
            me.join_adjacent_intervals(0)
        }
        me
    }

    pub fn add(&mut self, transaction_id: u64) -> bool {
        let index = self.find_interval(transaction_id);
        let mut added_to_existing = false;

        if index < self.intervals.len() {
            let interval = &mut self.intervals[index];
            if interval.start == transaction_id + 1 {
                interval.start = transaction_id;
                added_to_existing = true;
            } else if interval.end + 1 == transaction_id {
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
            self.join_adjacent_intervals(index);
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

    fn join_adjacent_intervals(&mut self, index: usize) {
        if self.intervals.is_empty() {
            return;
        }

        let mut i = cmp::min(index + 1, self.intervals.len() - 1);
        let mut end = 0;
        if index >= 1 {
            end = index - 1;
        }

        while i > end {
            if self.intervals[i - 1].end + 1 == self.intervals[i].start {
                self.intervals[i - 1].end = self.intervals[i].end;
                self.intervals.remove(i);
            }
            i -= 1;
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
        // 6d3960f6-4b36-11ef-8614-0242ac110002:1-5:7-10:12,
        // 787d08c4-4b36-11ef-8614-0242ac110006:1-5
        let lines = gtid_set.replace('\n', "");
        let uuid_sets: Vec<&str> = lines.split(',').collect();

        for uuid_set in uuid_sets {
            if uuid_set.is_empty() {
                continue;
            }

            let parts: Vec<&str> = uuid_set.split(':').collect();
            if parts.len() < 2 {
                return Err(BinlogError::InvalidGtid(uuid_set.to_string()));
            }

            let source_id = parts[0].to_string();
            let mut intervals = vec![];
            for interval_str in parts[1..].iter() {
                let interval_parts: Vec<&str> = interval_str.split('-').collect();
                if interval_parts.is_empty() {
                    return Err(BinlogError::InvalidGtid(uuid_set.to_string()));
                }

                let start = Self::parse_interval_num(interval_parts[0], uuid_set)?;
                let end = if interval_parts.len() > 1 {
                    Self::parse_interval_num(interval_parts[1], uuid_set)?
                } else {
                    start
                };
                intervals.push(Interval { start, end });
            }
            map.insert(source_id.clone(), UuidSet::new(source_id, intervals));
        }
        Ok(GtidSet { map })
    }

    pub fn add(&mut self, gtid: &str) -> Result<bool, BinlogError> {
        let split: Vec<&str> = gtid.split(':').collect();
        if split.len() != 2 {
            return Err(BinlogError::InvalidGtid(gtid.to_string()));
        }

        let source_id = split[0];
        if let Ok(transaction_id) = split[1].parse::<u64>() {
            let uuid_set = self
                .map
                .entry(source_id.to_string())
                .or_insert_with(|| UuidSet {
                    uuid: source_id.to_string(),
                    intervals: vec![],
                });
            Ok(uuid_set.add(transaction_id))
        } else {
            Err(BinlogError::InvalidGtid(gtid.to_string()))
        }
    }

    pub fn get_uuid_sets(&self) -> Vec<&UuidSet> {
        self.map.values().collect()
    }

    pub fn put_uuid_set(&mut self, uuid_set: UuidSet) {
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

    fn parse_interval_num(interval_num_str: &str, uuid_set: &str) -> Result<u64, BinlogError> {
        if let Ok(num) = interval_num_str.parse::<u64>() {
            Ok(num)
        } else {
            Err(BinlogError::InvalidGtid(uuid_set.to_string()))
        }
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
}
