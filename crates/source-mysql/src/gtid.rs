//! GTID positions: what a checkpoint carries so a resume survives a failover.
//!
//! A binlog file name and offset only mean anything on the server they were
//! read from — MySQL's own `GTID_ONLY` exists to stop persisting them — so the
//! checkpoint also records which transactions have been consumed, globally.
//!
//! The two servers agree on nothing here. MySQL identifies a transaction by
//! `source_uuid:N`, keeps a *set* of them, and takes that set as binary in
//! `COM_BINLOG_DUMP_GTID`. MariaDB identifies one by
//! `domain-server_id-sequence`, keeps only the newest per domain, and has no
//! such command at all — the position goes over as text in a session variable.
//! Both are implemented rather than one emulated: the difference is in the
//! server, and neither is a dialect of the other.

use std::collections::BTreeMap;

/// A consumed-transaction position, in whichever form its server speaks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GtidPosition {
    MySql(MySqlGtidSet),
    MariaDb(MariaGtidPos),
}

impl GtidPosition {
    /// Parse either server's textual form.
    ///
    /// The two are told apart by a colon, which only MySQL's form has: its
    /// intervals are `uuid:1-5`, while MariaDB's whole GTID is three
    /// dash-separated numbers.
    pub fn parse(text: &str) -> Option<Self> {
        let text = text.trim();
        if text.is_empty() {
            return None;
        }
        if text.contains(':') {
            MySqlGtidSet::parse(text).map(Self::MySql)
        } else {
            MariaGtidPos::parse(text).map(Self::MariaDb)
        }
    }

    pub fn to_text(&self) -> String {
        match self {
            Self::MySql(set) => set.to_text(),
            Self::MariaDb(pos) => pos.to_text(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::MySql(set) => set.is_empty(),
            Self::MariaDb(pos) => pos.is_empty(),
        }
    }
}

/// Every MySQL transaction consumed, as intervals per source uuid.
///
/// Intervals are held *inclusive* here, the way the textual form writes them.
/// The binary form on the wire is half-open, and [`MySqlGtidSet::encode`] is the
/// only place that conversion happens — one place to be wrong about, rather
/// than one per caller.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MySqlGtidSet {
    /// Sorted by uuid text, because the encoded set and the textual form both
    /// have to come out the same for the same content — a checkpoint that
    /// reordered itself between runs would look like a change that is not one.
    uuids: BTreeMap<String, Vec<(u64, u64)>>,
}

impl MySqlGtidSet {
    pub fn is_empty(&self) -> bool {
        self.uuids.is_empty()
    }

    /// Record one consumed transaction.
    ///
    /// Transactions from one server arrive in sequence, so the overwhelmingly
    /// common case is extending the last interval by one. Everything else — a
    /// gap, an earlier number arriving after a later one — still lands in the
    /// right place, because a set that silently dropped a number would resume
    /// by replaying transactions it had already written, or worse, skip them.
    pub fn add(&mut self, uuid: &str, gno: u64) {
        let intervals = self.uuids.entry(uuid.to_string()).or_default();
        // fast path: the next number after the last interval
        if let Some(last) = intervals.last_mut()
            && gno == last.1 + 1
        {
            last.1 = gno;
            return;
        }
        let at = intervals.partition_point(|(start, _)| *start <= gno);
        if at > 0 {
            let (start, end) = intervals[at - 1];
            if gno >= start && gno <= end {
                return; // already covered
            }
            if gno == end + 1 {
                intervals[at - 1].1 = gno;
                self.coalesce(uuid);
                return;
            }
        }
        if let Some((start, _)) = intervals.get(at)
            && gno + 1 == *start
        {
            intervals[at].0 = gno;
            self.coalesce(uuid);
            return;
        }
        intervals.insert(at, (gno, gno));
    }

    /// Join intervals that a newly added number made adjacent.
    fn coalesce(&mut self, uuid: &str) {
        let Some(intervals) = self.uuids.get_mut(uuid) else {
            return;
        };
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(intervals.len());
        for (start, end) in intervals.drain(..) {
            match merged.last_mut() {
                Some(prev) if start <= prev.1 + 1 => prev.1 = prev.1.max(end),
                _ => merged.push((start, end)),
            }
        }
        *intervals = merged;
    }

    /// `uuid:1-5:8,other:1-3`, which is what the server prints and accepts.
    pub fn to_text(&self) -> String {
        self.uuids
            .iter()
            .map(|(uuid, intervals)| {
                let mut out = uuid.clone();
                for (start, end) in intervals {
                    if start == end {
                        out.push_str(&format!(":{start}"));
                    } else {
                        out.push_str(&format!(":{start}-{end}"));
                    }
                }
                out
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn parse(text: &str) -> Option<Self> {
        let mut set = Self::default();
        // the server wraps long sets across lines
        for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let mut parts = entry.split(':');
            let uuid = parts.next()?.trim();
            if uuid.is_empty() {
                return None;
            }
            let intervals = set.uuids.entry(uuid.to_string()).or_default();
            for range in parts {
                let (start, end) = match range.split_once('-') {
                    Some((s, e)) => (s.trim().parse().ok()?, e.trim().parse().ok()?),
                    None => {
                        let one: u64 = range.trim().parse().ok()?;
                        (one, one)
                    }
                };
                if end < start {
                    return None;
                }
                intervals.push((start, end));
            }
            if intervals.is_empty() {
                return None;
            }
            intervals.sort_unstable();
            set.coalesce(uuid);
        }
        (!set.is_empty()).then_some(set)
    }

    /// The set as `COM_BINLOG_DUMP_GTID` carries it.
    ///
    /// From MySQL's own encoder: an 8-byte count of uuids, then per uuid its 16
    /// raw bytes, an 8-byte count of intervals, and each interval as two
    /// 8-byte numbers. The second is documented as "the first GNO *after* this
    /// interval", so an inclusive `1-5` goes out as `1, 6`.
    pub fn encode(&self) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.uuids.len() as u64).to_le_bytes());
        for (uuid, intervals) in &self.uuids {
            out.extend_from_slice(&parse_uuid(uuid)?);
            out.extend_from_slice(&(intervals.len() as u64).to_le_bytes());
            for (start, end) in intervals {
                out.extend_from_slice(&start.to_le_bytes());
                out.extend_from_slice(&(end + 1).to_le_bytes());
            }
        }
        Some(out)
    }
}

/// The 16 raw bytes of a `8-4-4-4-12` uuid.
fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

/// MariaDB's position: the newest sequence number per replication domain.
///
/// A set of intervals would be meaningless here — the sequence number is one
/// monotonic counter per domain, so "everything up to N" is the whole story,
/// and that is exactly what `gtid_slave_pos` holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MariaGtidPos {
    domains: BTreeMap<u32, (u32, u64)>,
}

impl MariaGtidPos {
    pub fn is_empty(&self) -> bool {
        self.domains.is_empty()
    }

    /// Record one consumed transaction, keeping the highest sequence seen.
    ///
    /// Never moving backwards matters on a server whose domain has more than
    /// one writer: a lower sequence arriving late must not rewind the position
    /// and have the resume replay everything above it.
    pub fn add(&mut self, domain: u32, server_id: u32, seq_no: u64) {
        let entry = self.domains.entry(domain).or_insert((server_id, seq_no));
        if seq_no >= entry.1 {
            *entry = (server_id, seq_no);
        }
    }

    pub fn to_text(&self) -> String {
        self.domains
            .iter()
            .map(|(domain, (server_id, seq_no))| format!("{domain}-{server_id}-{seq_no}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    pub fn parse(text: &str) -> Option<Self> {
        let mut pos = Self::default();
        for entry in text.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let mut parts = entry.split('-');
            let domain: u32 = parts.next()?.trim().parse().ok()?;
            let server_id: u32 = parts.next()?.trim().parse().ok()?;
            let seq_no: u64 = parts.next()?.trim().parse().ok()?;
            if parts.next().is_some() {
                return None;
            }
            pos.domains.insert(domain, (server_id, seq_no));
        }
        (!pos.is_empty()).then_some(pos)
    }
}

/// What has been consumed, tracked against the positions it was consumed at.
///
/// The checkpoint may only claim the transactions that are durably written, and
/// the token it is written for lags the stream. So each GTID is held with the
/// position of its commit and folded into the position only once a checkpoint
/// asks for that position or later — claiming the newest set instead would make
/// a resume skip everything the target had not yet taken.
#[derive(Debug)]
pub struct GtidTracker {
    position: GtidPosition,
    /// Commits not yet covered by a checkpoint, oldest first.
    pending: Vec<(u64, Consumed)>,
    /// Set once something arrived that no set can describe, after which the
    /// position is withheld rather than understated.
    incomplete: Option<&'static str>,
}

/// One consumed transaction, in whichever form its server names it.
#[derive(Debug, Clone)]
enum Consumed {
    MySql {
        uuid: String,
        gno: u64,
    },
    MariaDb {
        domain: u32,
        server_id: u32,
        seq_no: u64,
    },
}

impl GtidTracker {
    pub fn new(mariadb: bool, resume_from: Option<GtidPosition>) -> Self {
        let position = resume_from.unwrap_or(if mariadb {
            GtidPosition::MariaDb(MariaGtidPos::default())
        } else {
            GtidPosition::MySql(MySqlGtidSet::default())
        });
        Self {
            position,
            pending: Vec::new(),
            incomplete: None,
        }
    }

    pub fn record_mysql(&mut self, token: u64, uuid: String, gno: u64) {
        self.pending.push((token, Consumed::MySql { uuid, gno }));
    }

    pub fn record_mariadb(&mut self, token: u64, domain: u32, server_id: u32, seq_no: u64) {
        self.pending.push((
            token,
            Consumed::MariaDb {
                domain,
                server_id,
                seq_no,
            },
        ));
    }

    /// Say that something happened which a set cannot express, naming it once.
    ///
    /// An anonymous transaction has no GTID, and a tagged one is a shape this
    /// reader does not decode. Either way the set no longer describes
    /// everything consumed, and a checkpoint carrying it would resume by
    /// replaying what it omitted — or worse, skipping it.
    pub fn mark_incomplete(&mut self, why: &'static str) {
        if self.incomplete.is_none() {
            tracing::warn!(target: "pg2osync::source",
                "{why}, so this checkpoint cannot carry a GTID position and will not \
                 survive a failover; the binlog coordinate still resumes on this server");
            self.incomplete = Some(why);
        }
    }

    /// The position as of `token`, or nothing if it cannot be trusted.
    ///
    /// Called with a non-decreasing token by the checkpoint task, so folding
    /// is a single pass that consumes what it covers.
    pub fn position_at(&mut self, token: u64) -> Option<String> {
        let mut i = 0;
        while i < self.pending.len() {
            if self.pending[i].0 > token {
                break;
            }
            i += 1;
        }
        for (_, consumed) in self.pending.drain(..i) {
            match (&mut self.position, consumed) {
                (GtidPosition::MySql(set), Consumed::MySql { uuid, gno }) => set.add(&uuid, gno),
                (
                    GtidPosition::MariaDb(pos),
                    Consumed::MariaDb {
                        domain,
                        server_id,
                        seq_no,
                    },
                ) => pos.add(domain, server_id, seq_no),
                // A server cannot change dialect mid-stream; if it somehow did,
                // mixing the two would produce a position neither accepts.
                (position, _) => {
                    tracing::error!(target: "pg2osync::source",
                        "a GTID arrived in the other server's form; the position is \
                         no longer trustworthy");
                    *position = match position {
                        GtidPosition::MySql(_) => GtidPosition::MySql(MySqlGtidSet::default()),
                        GtidPosition::MariaDb(_) => GtidPosition::MariaDb(MariaGtidPos::default()),
                    };
                    self.incomplete = Some("a GTID arrived in the other server's form");
                    return None;
                }
            }
        }
        if self.incomplete.is_some() || self.position.is_empty() {
            return None;
        }
        Some(self.position.to_text())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consecutive_transactions_collapse_to_one_interval() {
        let mut set = MySqlGtidSet::default();
        for gno in 1..=1000 {
            set.add("3E11FA47-71CA-11E1-9E33-C80AA9429562", gno);
        }
        assert_eq!(
            set.to_text(),
            "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-1000",
            "a thousand transactions must not become a thousand intervals"
        );
    }

    #[test]
    fn a_gap_keeps_two_intervals_and_closing_it_joins_them() {
        let uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
        let mut set = MySqlGtidSet::default();
        for gno in [1, 2, 3, 5, 6] {
            set.add(uuid, gno);
        }
        assert_eq!(set.to_text(), format!("{uuid}:1-3:5-6"));
        set.add(uuid, 4);
        assert_eq!(set.to_text(), format!("{uuid}:1-6"));
    }

    #[test]
    fn a_number_already_held_changes_nothing() {
        let uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
        let mut set = MySqlGtidSet::default();
        for gno in 1..=5 {
            set.add(uuid, gno);
        }
        let before = set.clone();
        for gno in 1..=5 {
            set.add(uuid, gno);
        }
        assert_eq!(set, before);
    }

    #[test]
    fn a_single_transaction_prints_without_a_range() {
        let mut set = MySqlGtidSet::default();
        set.add("3E11FA47-71CA-11E1-9E33-C80AA9429562", 7);
        assert_eq!(set.to_text(), "3E11FA47-71CA-11E1-9E33-C80AA9429562:7");
    }

    #[test]
    fn a_mysql_set_survives_its_own_text() {
        let text = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5:8-9,\
                    5de4400a-71ca-11e1-9e33-c80aa9429562:1-3";
        let set = MySqlGtidSet::parse(text).expect("parses");
        assert_eq!(MySqlGtidSet::parse(&set.to_text()), Some(set));
    }

    #[test]
    fn the_binary_form_is_what_the_server_decodes() {
        let mut set = MySqlGtidSet::default();
        for gno in 1..=5 {
            set.add("3E11FA47-71CA-11E1-9E33-C80AA9429562", gno);
        }
        let encoded = set.encode().expect("valid uuid");
        let mut want = Vec::new();
        want.extend_from_slice(&1u64.to_le_bytes()); // one uuid
        want.extend_from_slice(&[
            0x3E, 0x11, 0xFA, 0x47, 0x71, 0xCA, 0x11, 0xE1, 0x9E, 0x33, 0xC8, 0x0A, 0xA9, 0x42,
            0x95, 0x62,
        ]);
        want.extend_from_slice(&1u64.to_le_bytes()); // one interval
        want.extend_from_slice(&1u64.to_le_bytes()); // first gno
        // exclusive end: the interval holds 1-5, so the server is told 6
        want.extend_from_slice(&6u64.to_le_bytes());
        assert_eq!(encoded, want);
    }

    #[test]
    fn a_malformed_uuid_does_not_encode() {
        let mut set = MySqlGtidSet::default();
        set.add("not-a-uuid", 1);
        assert!(
            set.encode().is_none(),
            "sending a truncated sid block would resume somewhere unrelated"
        );
    }

    #[test]
    fn mariadb_keeps_the_newest_sequence_per_domain() {
        let mut pos = MariaGtidPos::default();
        pos.add(0, 3307, 100);
        pos.add(1, 3307, 5);
        pos.add(0, 3307, 101);
        assert_eq!(pos.to_text(), "0-3307-101,1-3307-5");
    }

    #[test]
    fn a_late_lower_sequence_does_not_rewind_mariadb() {
        let mut pos = MariaGtidPos::default();
        pos.add(0, 3307, 100);
        pos.add(0, 42, 7);
        assert_eq!(
            pos.to_text(),
            "0-3307-100",
            "rewinding would replay everything above it"
        );
    }

    #[test]
    fn the_two_forms_are_told_apart_by_their_own_syntax() {
        assert!(matches!(
            GtidPosition::parse("0-3307-1431"),
            Some(GtidPosition::MariaDb(_))
        ));
        assert!(matches!(
            GtidPosition::parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"),
            Some(GtidPosition::MySql(_))
        ));
        assert!(GtidPosition::parse("").is_none());
        assert!(GtidPosition::parse("nonsense").is_none());
    }

    #[test]
    fn a_checkpoint_claims_only_what_is_written() {
        let mut tracker = GtidTracker::new(false, None);
        let uuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
        tracker.record_mysql(100, uuid.into(), 1);
        tracker.record_mysql(200, uuid.into(), 2);
        tracker.record_mysql(300, uuid.into(), 3);
        assert_eq!(
            tracker.position_at(200).as_deref(),
            Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-2"),
            "the third transaction is not durable yet, so resuming past it would skip it"
        );
        assert_eq!(
            tracker.position_at(300).as_deref(),
            Some("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-3")
        );
    }

    #[test]
    fn nothing_consumed_yet_is_no_position_rather_than_an_empty_one() {
        let mut tracker = GtidTracker::new(true, None);
        assert!(tracker.position_at(1000).is_none());
    }

    #[test]
    fn a_transaction_without_a_gtid_withholds_the_position() {
        let mut tracker = GtidTracker::new(false, None);
        tracker.record_mysql(100, "3e11fa47-71ca-11e1-9e33-c80aa9429562".into(), 1);
        tracker.mark_incomplete("a transaction was written with no GTID");
        assert!(
            tracker.position_at(100).is_none(),
            "a set that omits a transaction would resume in the wrong place"
        );
    }

    #[test]
    fn a_resumed_tracker_carries_what_it_was_given() {
        let start = GtidPosition::parse("0-3307-1431").expect("parses");
        let mut tracker = GtidTracker::new(true, Some(start));
        tracker.record_mariadb(500, 0, 3307, 1432);
        assert_eq!(tracker.position_at(499).as_deref(), Some("0-3307-1431"));
        assert_eq!(tracker.position_at(500).as_deref(), Some("0-3307-1432"));
    }

    #[test]
    fn a_mariadb_position_survives_its_own_text() {
        let pos = MariaGtidPos::parse("0-3307-1431,1-42-9").expect("parses");
        assert_eq!(MariaGtidPos::parse(&pos.to_text()), Some(pos));
    }
}
