use crate::git_process::new_tokio_git_command;
use chrono::{Datelike, Duration, Local, NaiveDate, Weekday};
use futures::StreamExt;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use tokio_util::sync::CancellationToken;
use walkdir::WalkDir;

// ── Constants ────────────────────────────────────────────────────────────────

const SKIP_DIRS: &[&str] = &[
    "target",
    "node_modules",
    ".next",
    "dist",
    "build",
    ".venv",
    "venv",
    "__pycache__",
    "__MACOSX",
    ".idea",
    ".vscode",
];

/// Default maximum directory depth for the WalkDir scanner.
pub const DEFAULT_MAX_DEPTH: usize = 6;

/// Per-repo git process timeout in seconds.
const GIT_TIMEOUT_SECS: u64 = 30;

/// Maximum parallel repository scans (capped at physical/logical core count).
const MAX_PARALLELISM: usize = 8;

/// Minimum year allowed for contribution range selection.
const MIN_SUPPORTED_YEAR: i32 = 2005;

// ── Public types ─────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
pub struct ScanResult {
    pub username: String,
    pub root_path: PathBuf,
    pub selected_year_start: i32,
    pub selected_year_end: i32,
    pub repos_scanned: usize,
    pub contributions: BTreeMap<NaiveDate, u32>,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub analytics: ScanAnalytics,
    pub repo_errors: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct ScanAnalytics {
    pub top_repositories: Vec<TopRepositoryStat>,
    pub streak_metrics: StreakMetrics,
    pub activity_days: ActivityDayMetrics,
    pub peak_productivity: PeakProductivity,
    pub weekly_trend: WeeklyTrend,
    pub day_distribution: DayDistribution,
}

#[derive(Clone, Debug)]
pub struct TopRepositoryStat {
    pub repo_path: String,
    pub commit_count: u32,
    pub percentage_of_total: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreakMetrics {
    pub current_streak: u32,
    pub longest_streak: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActivityDayMetrics {
    pub active_days: u32,
    pub inactive_days: u32,
    pub total_days: u32,
}

#[derive(Clone, Debug, Default)]
pub struct PeakProductivity {
    pub best_day: Option<PeakDay>,
    pub best_week: Option<PeakWeek>,
    pub best_month: Option<PeakMonth>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeakDay {
    pub date: NaiveDate,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeakWeek {
    pub iso_year: i32,
    pub iso_week: u32,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeakMonth {
    pub year: i32,
    pub month: u32,
    pub count: u32,
}

#[derive(Clone, Debug)]
pub struct WeeklyTrend {
    pub week_over_week_growth: Option<WeekOverWeekGrowth>,
    pub moving_average_points: Vec<MovingAveragePoint>,
    pub weekly_totals: Vec<WeeklyBucket>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WeeklyBucket {
    pub iso_year: i32,
    pub iso_week: u32,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub count: u32,
}

#[derive(Clone, Debug)]
pub struct WeekOverWeekGrowth {
    pub current_iso_year: i32,
    pub current_iso_week: u32,
    pub current_week_start: NaiveDate,
    pub current_week_end: NaiveDate,
    pub current_week_count: u32,
    pub previous_iso_year: i32,
    pub previous_iso_week: u32,
    pub previous_week_start: NaiveDate,
    pub previous_week_end: NaiveDate,
    pub previous_week_count: u32,
    pub delta: i32,
    pub percent_change: Option<f64>,
}

#[derive(Clone, Debug)]
pub struct MovingAveragePoint {
    pub date: NaiveDate,
    pub value: f64,
}

#[derive(Clone, Debug, Default)]
pub struct DayDistribution {
    pub monday: u32,
    pub tuesday: u32,
    pub wednesday: u32,
    pub thursday: u32,
    pub friday: u32,
    pub saturday: u32,
    pub sunday: u32,
    pub weekend_ratio: f64,
}

impl ScanResult {
    pub fn total_contributions(&self) -> u32 {
        self.contributions.values().sum()
    }

    pub fn max_daily_count(&self) -> u32 {
        self.contributions.values().copied().max().unwrap_or(0)
    }
}

// ── Analytics helpers ───────────────────────────────────────────────────────

pub fn compute_scan_analytics(
    contributions: &BTreeMap<NaiveDate, u32>,
    repo_commit_counts: &[(PathBuf, u32)],
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> ScanAnalytics {
    let total_contributions: u32 = contributions.values().sum();
    let weekly_totals = compute_weekly_buckets(contributions, start_date, end_date);

    ScanAnalytics {
        top_repositories: compute_top_repositories(repo_commit_counts, total_contributions, 5),
        streak_metrics: compute_streak_metrics(contributions, start_date, end_date),
        activity_days: compute_activity_day_metrics(contributions, start_date, end_date),
        peak_productivity: compute_peak_productivity(contributions, start_date, end_date),
        weekly_trend: WeeklyTrend {
            week_over_week_growth: compute_week_over_week_growth(&weekly_totals),
            moving_average_points: compute_moving_average_points(
                contributions,
                start_date,
                end_date,
                7,
            ),
            weekly_totals,
        },
        day_distribution: compute_day_distribution(contributions),
    }
}

pub fn compute_top_repositories(
    repo_commit_counts: &[(PathBuf, u32)],
    total_contributions: u32,
    limit: usize,
) -> Vec<TopRepositoryStat> {
    let mut sorted = repo_commit_counts
        .iter()
        .filter(|(_, count)| *count > 0)
        .map(|(path, count)| (path.display().to_string(), *count))
        .collect::<Vec<_>>();

    sorted.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    sorted
        .into_iter()
        .take(limit)
        .map(|(repo_path, commit_count)| {
            let percentage_of_total = if total_contributions == 0 {
                0.0
            } else {
                (commit_count as f64 * 100.0) / total_contributions as f64
            };

            TopRepositoryStat {
                repo_path,
                commit_count,
                percentage_of_total,
            }
        })
        .collect()
}

pub fn compute_streak_metrics(
    contributions: &BTreeMap<NaiveDate, u32>,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> StreakMetrics {
    if start_date > end_date {
        return StreakMetrics::default();
    }

    let mut longest_streak = 0u32;
    let mut running_streak = 0u32;

    for_each_day_inclusive(start_date, end_date, |date| {
        if contributions.get(&date).copied().unwrap_or(0) > 0 {
            running_streak += 1;
            longest_streak = longest_streak.max(running_streak);
        } else {
            running_streak = 0;
        }
    });

    let mut current_streak = 0u32;
    let mut cursor = end_date;

    loop {
        if contributions.get(&cursor).copied().unwrap_or(0) > 0 {
            current_streak += 1;
        } else {
            break;
        }

        if cursor <= start_date {
            break;
        }

        cursor -= Duration::days(1);
    }

    StreakMetrics {
        current_streak,
        longest_streak,
    }
}

pub fn compute_activity_day_metrics(
    contributions: &BTreeMap<NaiveDate, u32>,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> ActivityDayMetrics {
    if start_date > end_date {
        return ActivityDayMetrics::default();
    }

    let total_days = ((end_date - start_date).num_days() as u32).saturating_add(1);
    let mut active_days = 0u32;

    for_each_day_inclusive(start_date, end_date, |date| {
        if contributions.get(&date).copied().unwrap_or(0) > 0 {
            active_days += 1;
        }
    });

    ActivityDayMetrics {
        active_days,
        inactive_days: total_days.saturating_sub(active_days),
        total_days,
    }
}

pub fn compute_weekly_buckets(
    contributions: &BTreeMap<NaiveDate, u32>,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Vec<WeeklyBucket> {
    if start_date > end_date {
        return Vec::new();
    }

    let mut buckets: BTreeMap<(i32, u32), WeeklyBucket> = BTreeMap::new();

    for_each_day_inclusive(start_date, end_date, |date| {
        let iso_week = date.iso_week();
        let key = (iso_week.year(), iso_week.week());

        let week_start = iso_week_start(date);
        let week_end = week_start + Duration::days(6);

        let entry = buckets.entry(key).or_insert_with(|| WeeklyBucket {
            iso_year: iso_week.year(),
            iso_week: iso_week.week(),
            start_date: week_start,
            end_date: week_end,
            count: 0,
        });

        entry.count = entry
            .count
            .saturating_add(contributions.get(&date).copied().unwrap_or(0));
    });

    buckets.into_values().collect()
}

pub fn compute_week_over_week_growth(weekly_totals: &[WeeklyBucket]) -> Option<WeekOverWeekGrowth> {
    if weekly_totals.len() < 2 {
        return None;
    }

    let previous = &weekly_totals[weekly_totals.len() - 2];
    let current = &weekly_totals[weekly_totals.len() - 1];
    let delta = current.count as i32 - previous.count as i32;
    let percent_change = if previous.count == 0 {
        None
    } else {
        Some((delta as f64 / previous.count as f64) * 100.0)
    };

    Some(WeekOverWeekGrowth {
        current_iso_year: current.iso_year,
        current_iso_week: current.iso_week,
        current_week_start: current.start_date,
        current_week_end: current.end_date,
        current_week_count: current.count,
        previous_iso_year: previous.iso_year,
        previous_iso_week: previous.iso_week,
        previous_week_start: previous.start_date,
        previous_week_end: previous.end_date,
        previous_week_count: previous.count,
        delta,
        percent_change,
    })
}

pub fn compute_moving_average_points(
    contributions: &BTreeMap<NaiveDate, u32>,
    start_date: NaiveDate,
    end_date: NaiveDate,
    window_days: usize,
) -> Vec<MovingAveragePoint> {
    if start_date > end_date || window_days == 0 {
        return Vec::new();
    }

    let mut points = Vec::new();
    let mut rolling_window = VecDeque::with_capacity(window_days);
    let mut rolling_sum = 0u32;

    for_each_day_inclusive(start_date, end_date, |date| {
        let count = contributions.get(&date).copied().unwrap_or(0);
        rolling_window.push_back(count);
        rolling_sum = rolling_sum.saturating_add(count);

        if rolling_window.len() > window_days
            && let Some(removed) = rolling_window.pop_front()
        {
            rolling_sum = rolling_sum.saturating_sub(removed);
        }

        let average = if rolling_window.is_empty() {
            0.0
        } else {
            rolling_sum as f64 / rolling_window.len() as f64
        };

        points.push(MovingAveragePoint {
            date,
            value: average,
        });
    });

    points
}

pub fn compute_peak_productivity(
    contributions: &BTreeMap<NaiveDate, u32>,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> PeakProductivity {
    if start_date > end_date {
        return PeakProductivity::default();
    }

    let mut best_day: Option<PeakDay> = None;
    let mut month_totals: BTreeMap<(i32, u32), u32> = BTreeMap::new();

    for_each_day_inclusive(start_date, end_date, |date| {
        let count = contributions.get(&date).copied().unwrap_or(0);

        if count > 0 {
            match &best_day {
                None => {
                    best_day = Some(PeakDay { date, count });
                }
                Some(current) => {
                    if count > current.count || (count == current.count && date < current.date) {
                        best_day = Some(PeakDay { date, count });
                    }
                }
            }
        }

        let month_key = (date.year(), date.month());
        let total = month_totals.entry(month_key).or_insert(0);
        *total = total.saturating_add(count);
    });

    let mut best_week_bucket: Option<WeeklyBucket> = None;
    for bucket in compute_weekly_buckets(contributions, start_date, end_date) {
        if bucket.count == 0 {
            continue;
        }

        match &best_week_bucket {
            None => {
                best_week_bucket = Some(bucket);
            }
            Some(current) => {
                if bucket.count > current.count
                    || (bucket.count == current.count && bucket.start_date < current.start_date)
                {
                    best_week_bucket = Some(bucket);
                }
            }
        }
    }

    let best_week = best_week_bucket.map(|bucket| PeakWeek {
        iso_year: bucket.iso_year,
        iso_week: bucket.iso_week,
        start_date: bucket.start_date,
        end_date: bucket.end_date,
        count: bucket.count,
    });

    let mut best_month: Option<PeakMonth> = None;
    for ((year, month), count) in month_totals {
        if count == 0 {
            continue;
        }

        match &best_month {
            None => {
                best_month = Some(PeakMonth { year, month, count });
            }
            Some(current) => {
                if count > current.count
                    || (count == current.count
                        && (year < current.year || (year == current.year && month < current.month)))
                {
                    best_month = Some(PeakMonth { year, month, count });
                }
            }
        }
    }

    PeakProductivity {
        best_day,
        best_week,
        best_month,
    }
}

pub fn compute_day_distribution(contributions: &BTreeMap<NaiveDate, u32>) -> DayDistribution {
    let mut weekday_counts = [0u32; 7];

    for (date, count) in contributions {
        let index = date.weekday().num_days_from_monday() as usize;
        weekday_counts[index] = weekday_counts[index].saturating_add(*count);
    }

    let total: u32 = weekday_counts.iter().sum();
    let weekend_total = weekday_counts[5].saturating_add(weekday_counts[6]);
    let weekend_ratio = if total == 0 {
        0.0
    } else {
        weekend_total as f64 / total as f64
    };

    DayDistribution {
        monday: weekday_counts[0],
        tuesday: weekday_counts[1],
        wednesday: weekday_counts[2],
        thursday: weekday_counts[3],
        friday: weekday_counts[4],
        saturday: weekday_counts[5],
        sunday: weekday_counts[6],
        weekend_ratio,
    }
}

fn for_each_day_inclusive(
    start_date: NaiveDate,
    end_date: NaiveDate,
    mut f: impl FnMut(NaiveDate),
) {
    if start_date > end_date {
        return;
    }

    let mut current = start_date;
    loop {
        f(current);
        if current >= end_date {
            break;
        }
        current += Duration::days(1);
    }
}

fn iso_week_start(date: NaiveDate) -> NaiveDate {
    date - Duration::days(date.weekday().num_days_from_monday() as i64)
}

// ── Path helpers ──────────────────────────────────────────────────────────────

/// Normalise a raw path string.  On Windows, a bare drive letter like `D:` is
/// expanded to `D:\` so that it behaves like a proper root path.
pub fn normalize_root_path(raw_path: &str) -> PathBuf {
    let trimmed = raw_path.trim();

    #[cfg(windows)]
    {
        let bytes = trimmed.as_bytes();
        if bytes.len() == 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
            return PathBuf::from(format!("{trimmed}\\"));
        }
    }

    PathBuf::from(trimmed)
}

/// Validate the root path and return the canonical, absolute version.
/// Returns `Err` when the path does not exist, is not a directory, is a
/// filesystem root, or cannot be canonicalised.
pub fn validate_root_path(path: &Path) -> Result<PathBuf, String> {
    if path.as_os_str().is_empty() {
        return Err("Path cannot be empty".to_string());
    }

    if !path.exists() {
        return Err(format!("Path not found: {}", path.display()));
    }

    let canonical =
        std::fs::canonicalize(path).map_err(|e| format!("Invalid or inaccessible path: {e}"))?;

    if !canonical.is_dir() {
        return Err(format!("Path is not a directory: {}", canonical.display()));
    }

    if is_filesystem_root(&canonical) {
        return Err(
            "Scanning from filesystem root is not allowed. Choose a more specific folder."
                .to_string(),
        );
    }

    Ok(canonical)
}

/// Return `true` when `path` is a filesystem root (e.g. `C:\`, `/`, `/home`,
/// `/Users`).  These paths are too broad for a useful scan.
pub fn is_filesystem_root(path: &Path) -> bool {
    #[cfg(windows)]
    {
        // A path is a root on Windows when it has no parent or its string
        // representation is exactly `X:\` (3 chars: letter, colon, backslash).
        if path
            .parent()
            .map(|p| p.as_os_str().is_empty())
            .unwrap_or(true)
        {
            return true;
        }
        let s = path.to_string_lossy();
        s.len() <= 3 && s.chars().nth(1) == Some(':')
    }

    #[cfg(not(windows))]
    {
        let s = path.to_string_lossy();
        matches!(s.as_ref(), "/" | "/home" | "/Users" | "/root")
    }
}

fn path_key(path: &Path) -> String {
    let value = path.to_string_lossy().to_string();

    #[cfg(windows)]
    {
        value.to_ascii_lowercase()
    }

    #[cfg(not(windows))]
    {
        value
    }
}

fn dedupe_paths(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    let mut unique = std::collections::BTreeMap::<String, PathBuf>::new();
    for path in paths {
        let key = path_key(&path);
        unique.entry(key).or_insert(path);
    }
    unique.into_values().collect()
}

// ── Main scanner entry-point ─────────────────────────────────────────────────

/// Scan all git repositories under `root_path` and aggregate contributions for
/// `username` within a selected contribution year window.
///
/// * `max_depth`  – directory recursion limit (UI-configurable, capped at 20).
/// * `selected_year_start` – first contribution year in the scan range.
/// * `selected_year_end` – last contribution year in the scan range.
/// * `cancel`     – cancellation token; the scan aborts as soon as it is
///   triggered.
pub async fn scan_local_contributions(
    username: &str,
    root_path: &Path,
    max_depth: usize,
    selected_year_start: i32,
    selected_year_end: i32,
    cancel: CancellationToken,
) -> Result<ScanResult, String> {
    let roots = [root_path.to_path_buf()];
    scan_local_contributions_multi(
        username,
        &roots,
        max_depth,
        selected_year_start,
        selected_year_end,
        cancel,
    )
    .await
}

/// Multi-root variant for scanning contributions.
///
/// Repositories discovered across all roots are merged and deduplicated before
/// scanning, so overlapping folder trees do not cause duplicate repo scans.
pub async fn scan_local_contributions_multi(
    username: &str,
    root_paths: &[PathBuf],
    max_depth: usize,
    selected_year_start: i32,
    selected_year_end: i32,
    cancel: CancellationToken,
) -> Result<ScanResult, String> {
    let normalized_username = username.trim();
    if normalized_username.is_empty() {
        return Err("Username cannot be empty".to_string());
    }

    if root_paths.is_empty() {
        return Err("Path cannot be empty".to_string());
    }

    // Validate & canonicalize roots (guardrail #5)
    let canonical_roots = dedupe_paths(
        root_paths
            .iter()
            .map(|path| validate_root_path(path))
            .collect::<Result<Vec<_>, _>>()?,
    );
    if canonical_roots.is_empty() {
        return Err("Path cannot be empty".to_string());
    }

    validate_selected_year_range(selected_year_start, selected_year_end)?;

    let primary_root = canonical_roots[0].clone();

    ensure_git_available().await?;

    let (start_date, end_date) = heatmap_range(selected_year_start, selected_year_end);
    let effective_depth = max_depth.clamp(1, 20);
    let repositories = dedupe_paths(
        canonical_roots
            .iter()
            .flat_map(|root| find_git_repositories(root, effective_depth))
            .collect(),
    );

    if cancel.is_cancelled() {
        return Err("Scan cancelled".to_string());
    }

    let concurrency = std::cmp::min(num_cpus::get().max(1), MAX_PARALLELISM);

    // ── Bounded parallel scan ───────────────────────────────────────────────
    // Each repo is scanned in its own async task, with at most `concurrency`
    // tasks running concurrently.
    let username_owned = normalized_username.to_string();

    let stream = futures::stream::iter(repositories).map(|repo| {
        let username_ref = username_owned.clone();
        let cancel_clone = cancel.clone();

        async move {
            if cancel_clone.is_cancelled() {
                return Err("Scan cancelled".to_string());
            }

            tokio::select! {
                _ = cancel_clone.cancelled() => {
                    Err("Scan cancelled".to_string())
                }
                result = scan_repo_log_async(
                    &repo,
                    &username_ref,
                    start_date,
                    end_date,
                ) => {
                    result.map(|data| (repo, data))
                }
            }
        }
    });

    let results: Vec<_> = stream.buffer_unordered(concurrency).collect().await;

    // ── Merge results ────────────────────────────────────────────────────────
    let mut contributions: BTreeMap<NaiveDate, u32> = BTreeMap::new();
    let mut seen_commits: HashSet<String> = HashSet::new();
    let mut repo_commit_counts: Vec<(PathBuf, u32)> = Vec::new();
    let mut repo_errors: Vec<String> = Vec::new();
    let mut repos_scanned = 0usize;

    for result in results {
        match result {
            Ok((repo_path, repo_data)) => {
                repos_scanned += 1;

                let mut per_repo_seen_commits: HashSet<String> = HashSet::new();

                for (date, hashes) in repo_data {
                    for hash in hashes {
                        per_repo_seen_commits.insert(hash.clone());

                        if seen_commits.insert(hash) {
                            *contributions.entry(date).or_insert(0) += 1;
                        }
                    }
                }

                repo_commit_counts.push((repo_path, per_repo_seen_commits.len() as u32));
            }
            Err(e) => {
                if e == "Scan cancelled" {
                    // Propagate the first cancellation immediately
                    return Err(e);
                }
                repo_errors.push(e);
            }
        }
    }

    let analytics =
        compute_scan_analytics(&contributions, &repo_commit_counts, start_date, end_date);

    Ok(ScanResult {
        username: normalized_username.to_string(),
        root_path: primary_root,
        selected_year_start,
        selected_year_end,
        repos_scanned,
        contributions,
        start_date,
        end_date,
        analytics,
        repo_errors,
    })
}

// ── Heatmap date range ────────────────────────────────────────────────────────

fn validate_selected_year_range(
    selected_year_start: i32,
    selected_year_end: i32,
) -> Result<(), String> {
    let current_year = Local::now().year();

    if !(MIN_SUPPORTED_YEAR..=current_year).contains(&selected_year_start) {
        return Err(format!(
            "Start year must be between {MIN_SUPPORTED_YEAR} and {current_year}"
        ));
    }

    if !(MIN_SUPPORTED_YEAR..=current_year).contains(&selected_year_end) {
        return Err(format!(
            "End year must be between {MIN_SUPPORTED_YEAR} and {current_year}"
        ));
    }

    if selected_year_start > selected_year_end {
        return Err("Start year must be less than or equal to end year".to_string());
    }

    Ok(())
}

pub fn heatmap_range(selected_year_start: i32, selected_year_end: i32) -> (NaiveDate, NaiveDate) {
    let today = Local::now().date_naive();
    let current_year = today.year();
    let normalized_start_year = selected_year_start.clamp(MIN_SUPPORTED_YEAR, current_year);
    let normalized_end_year = selected_year_end.clamp(MIN_SUPPORTED_YEAR, current_year);
    let start_year = normalized_start_year.min(normalized_end_year);

    let end_date = if normalized_end_year == current_year {
        today
    } else {
        NaiveDate::from_ymd_opt(normalized_end_year, 12, 31).unwrap_or(today)
    };

    let mut start_date = NaiveDate::from_ymd_opt(start_year, 1, 1).unwrap_or(today);

    while start_date.weekday() != Weekday::Mon {
        start_date -= Duration::days(1);
    }

    (start_date, end_date)
}

// ── Git availability check ────────────────────────────────────────────────────

async fn ensure_git_available() -> Result<(), String> {
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        new_tokio_git_command().arg("--version").output(),
    )
    .await;

    match result {
        Ok(Ok(output)) if output.status.success() => Ok(()),
        Ok(Ok(_)) => Err("Git command available but failed to run".to_string()),
        Ok(Err(_)) => Err("Git not found. Ensure git is installed and added to PATH".to_string()),
        Err(_) => Err("Git availability check timed out — git did not respond".to_string()),
    }
}

// ── Repository discovery ──────────────────────────────────────────────────────

fn find_git_repositories(root_path: &Path, max_depth: usize) -> Vec<PathBuf> {
    let mut repositories = Vec::new();

    // Add 1 because WalkDir depth counts the root itself as depth 0.
    let mut walker = WalkDir::new(root_path)
        .max_depth(max_depth + 1)
        .follow_links(false)
        .into_iter();

    while let Some(entry_result) = walker.next() {
        let entry = match entry_result {
            Ok(e) => e,
            Err(_) => continue,
        };

        let file_name = entry.file_name().to_string_lossy();

        if file_name == ".git" {
            if let Some(parent) = entry.path().parent() {
                // Fast filesystem check — no `git rev-parse` spawn needed
                if is_git_repository_fast(parent) {
                    repositories.push(parent.to_path_buf());
                }
            }

            // Never recurse into .git itself
            if entry.file_type().is_dir() {
                walker.skip_current_dir();
            }

            continue;
        }

        if entry.file_type().is_dir() && entry.depth() > 0 && should_skip_dir(file_name.as_ref()) {
            walker.skip_current_dir();
        }
    }

    dedupe_paths(repositories)
}

/// Validate a potential repo using only filesystem operations — no extra
/// `git rev-parse` process spawn required.
fn is_git_repository_fast(path: &Path) -> bool {
    let git_path = path.join(".git");

    if git_path.is_dir() {
        // Regular repository: must contain a HEAD file
        git_path.join("HEAD").is_file()
    } else if git_path.is_file() {
        // Submodule / worktree: .git is a file pointing to the real git dir
        true
    } else {
        false
    }
}

pub fn should_skip_dir(directory_name: &str) -> bool {
    SKIP_DIRS.contains(&directory_name)
}

// ── Per-repo async scan ───────────────────────────────────────────────────────

/// Scan a single repository and return a map of `date → [commit hashes]`.
/// Returns `Ok(empty_map)` for access-denied / ownership errors so those are
/// surfaced as `repo_errors` rather than hard failures.
async fn scan_repo_log_async(
    repository: &Path,
    username: &str,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<BTreeMap<NaiveDate, Vec<String>>, String> {
    // Command-level date + author pre-filter (#1)
    let since_arg = format!("--since={}", start_date.format("%Y-%m-%d"));
    // Add one day so `--until` is inclusive of end_date
    let until_arg = format!(
        "--until={}",
        (end_date + Duration::days(1)).format("%Y-%m-%d")
    );
    // --author performs a case-insensitive regex search against "Name <email>".
    // We still do a precise post-filter in Rust for correctness.
    let author_arg = format!("--author={}", username);

    let output_future = new_tokio_git_command()
        .arg("-C")
        .arg(repository)
        .args([
            "log",
            "--all",
            "--date=short",
            "--pretty=format:%ad%x09%an%x09%ae%x09%H",
            &since_arg,
            &until_arg,
            &author_arg,
        ])
        .output();

    // Apply per-repo timeout (#3)
    let output = match tokio::time::timeout(
        std::time::Duration::from_secs(GIT_TIMEOUT_SECS),
        output_future,
    )
    .await
    {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => {
            return Err(format!(
                "Failed to read {}: could not run git log ({e})",
                repository.display()
            ));
        }
        Err(_) => {
            return Err(format!(
                "Failed to read {}: git log timed out after {} seconds",
                repository.display(),
                GIT_TIMEOUT_SECS
            ));
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        if is_not_git_repository_error(stderr.trim()) {
            return Ok(BTreeMap::new());
        }

        return Err(format_repo_scan_error(repository, stderr.trim()));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut result: BTreeMap<NaiveDate, Vec<String>> = BTreeMap::new();

    for line in stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }

        let mut parts = line.split('\t');
        let Some(raw_date) = parts.next() else {
            continue;
        };
        let Some(author_name) = parts.next() else {
            continue;
        };
        let Some(author_email) = parts.next() else {
            continue;
        };
        let Some(commit_hash) = parts.next() else {
            continue;
        };

        // Precise post-filter: correct for any mis-match from `--author` regex
        if !matches_username(username, author_name, author_email) {
            continue;
        }

        let Ok(commit_date) = NaiveDate::parse_from_str(raw_date, "%Y-%m-%d") else {
            continue;
        };

        // Safety net in case git extends beyond the requested window
        if commit_date < start_date || commit_date > end_date {
            continue;
        }

        result
            .entry(commit_date)
            .or_default()
            .push(commit_hash.to_string());
    }

    Ok(result)
}

// ── Error helpers ─────────────────────────────────────────────────────────────

fn is_not_git_repository_error(stderr: &str) -> bool {
    stderr.to_ascii_lowercase().contains("not a git repository")
}

fn format_repo_scan_error(repository: &Path, stderr: &str) -> String {
    if is_dubious_ownership_error(stderr) {
        let safe_command = format!(
            "git config --global --add safe.directory \"{}\"",
            repository.display()
        );

        return format!(
            "Failed to read {}: repository skipped due to dubious ownership. Fix: {}",
            repository.display(),
            safe_command
        );
    }

    let compact_stderr = compact_error_text(stderr);
    if compact_stderr.is_empty() {
        return format!(
            "Failed to read {}: git log failed without an error message.",
            repository.display()
        );
    }

    format!(
        "Failed to read {}: {}",
        repository.display(),
        compact_stderr
    )
}

pub fn is_dubious_ownership_error(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    lower.contains("detected dubious ownership") || lower.contains("safe.directory")
}

pub fn compact_error_text(stderr: &str) -> String {
    let compact = stderr
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ");

    const MAX_CHARS: usize = 260;
    if compact.chars().count() <= MAX_CHARS {
        return compact;
    }

    let mut shortened = compact.chars().take(MAX_CHARS).collect::<String>();
    shortened.push_str("...");
    shortened
}

// ── Username matching ─────────────────────────────────────────────────────────

pub fn matches_username(username: &str, author_name: &str, author_email: &str) -> bool {
    let needle = username.trim().to_lowercase();
    let name = author_name.trim().to_lowercase();
    let email = author_email.trim().to_lowercase();

    if needle.is_empty() {
        return false;
    }

    if name == needle {
        return true;
    }

    if email == needle || email.starts_with(&format!("{needle}@")) {
        return true;
    }

    email
        .split('@')
        .next()
        .map(|local_part| local_part == needle)
        .unwrap_or(false)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── normalize_root_path ───────────────────────────────────────────────────

    #[test]
    fn normalize_bare_drive_letter() {
        #[cfg(windows)]
        {
            let result = normalize_root_path("D:");
            assert_eq!(result, PathBuf::from("D:\\"));
        }
    }

    #[test]
    fn normalize_full_path_unchanged() {
        let path = if cfg!(windows) {
            "D:\\Projects"
        } else {
            "/home/user/projects"
        };
        let result = normalize_root_path(path);
        assert_eq!(result, PathBuf::from(path));
    }

    #[test]
    fn normalize_trims_whitespace() {
        let result = normalize_root_path("  /some/path  ");
        assert_eq!(result, PathBuf::from("/some/path"));
    }

    // ── is_filesystem_root ────────────────────────────────────────────────────

    #[cfg(not(windows))]
    #[test]
    fn detects_unix_root() {
        assert!(is_filesystem_root(Path::new("/")));
        assert!(is_filesystem_root(Path::new("/home")));
        assert!(is_filesystem_root(Path::new("/Users")));
        assert!(!is_filesystem_root(Path::new("/home/alice/projects")));
    }

    #[cfg(windows)]
    #[test]
    fn detects_windows_root() {
        assert!(is_filesystem_root(Path::new("C:\\")));
        assert!(is_filesystem_root(Path::new("D:\\")));
        assert!(!is_filesystem_root(Path::new("D:\\Projects")));
    }

    // ── should_skip_dir ───────────────────────────────────────────────────────

    #[test]
    fn skips_known_dirs() {
        assert!(should_skip_dir("node_modules"));
        assert!(should_skip_dir("target"));
        assert!(should_skip_dir(".venv"));
        assert!(!should_skip_dir("src"));
        assert!(!should_skip_dir("my_project"));
    }

    // ── matches_username ──────────────────────────────────────────────────────

    #[test]
    fn matches_exact_name() {
        assert!(matches_username("alice", "alice", "other@example.com"));
    }

    #[test]
    fn matches_case_insensitive_name() {
        assert!(matches_username("alice", "Alice", "other@example.com"));
        assert!(matches_username("ALICE", "alice", "other@example.com"));
    }

    #[test]
    fn matches_full_email() {
        assert!(matches_username(
            "alice@example.com",
            "Someone Else",
            "alice@example.com"
        ));
    }

    #[test]
    fn matches_email_local_part() {
        assert!(matches_username(
            "alice",
            "Someone Else",
            "alice@example.com"
        ));
    }

    #[test]
    fn matches_email_prefix() {
        assert!(matches_username(
            "alice",
            "Someone Else",
            "alice@company.org"
        ));
    }

    #[test]
    fn rejects_partial_name_match() {
        assert!(!matches_username("ali", "alice", "alice@example.com"));
    }

    #[test]
    fn rejects_empty_username() {
        assert!(!matches_username("", "alice", "alice@example.com"));
        assert!(!matches_username("  ", "alice", "alice@example.com"));
    }

    // ── compact_error_text ────────────────────────────────────────────────────

    #[test]
    fn compact_short_text() {
        let result = compact_error_text("  error: something bad  \n  happened here  ");
        assert_eq!(result, "error: something bad happened here");
    }

    #[test]
    fn compact_empty() {
        assert_eq!(compact_error_text(""), "");
        assert_eq!(compact_error_text("   \n   "), "");
    }

    #[test]
    fn compact_truncates_long() {
        let long = "x".repeat(400);
        let result = compact_error_text(&long);
        assert!(result.ends_with("..."));
        assert!(result.chars().count() <= 263); // 260 + "..."
    }

    // ── is_dubious_ownership_error ─────────────────────────────────────────────

    #[test]
    fn detects_dubious_ownership() {
        assert!(is_dubious_ownership_error(
            "fatal: detected dubious ownership in repository"
        ));
        assert!(is_dubious_ownership_error(
            "hint: add safe.directory to your config"
        ));
        assert!(!is_dubious_ownership_error("error: not a git repository"));
    }

    // ── heatmap_range ─────────────────────────────────────────────────────────

    #[test]
    fn heatmap_same_year_starts_on_monday() {
        let current_year = Local::now().year();
        let selected_year = if current_year > MIN_SUPPORTED_YEAR {
            current_year - 1
        } else {
            current_year
        };
        let (start, _end) = heatmap_range(selected_year, selected_year);
        let jan_1 = NaiveDate::from_ymd_opt(selected_year, 1, 1).unwrap();

        assert_eq!(start.weekday(), Weekday::Mon);
        assert!(start <= jan_1);
    }

    #[test]
    fn heatmap_same_year_past_ends_on_december_31() {
        let current_year = Local::now().year();
        let selected_year = if current_year > MIN_SUPPORTED_YEAR {
            current_year - 1
        } else {
            current_year
        };

        if selected_year == current_year {
            return;
        }

        let (_start, end) = heatmap_range(selected_year, selected_year);
        assert_eq!(end, NaiveDate::from_ymd_opt(selected_year, 12, 31).unwrap());
    }

    #[test]
    fn heatmap_range_ending_in_current_year_ends_today() {
        let today = Local::now().date_naive();
        let start_year = if today.year() > MIN_SUPPORTED_YEAR {
            today.year() - 1
        } else {
            today.year()
        };

        let (_start, end) = heatmap_range(start_year, today.year());
        assert_eq!(end, today);
    }

    #[test]
    fn heatmap_multi_year_range_spans_requested_years() {
        let current_year = Local::now().year();
        let end_year = if current_year > MIN_SUPPORTED_YEAR + 1 {
            current_year - 1
        } else {
            current_year
        };
        let start_year = (end_year - 1).max(MIN_SUPPORTED_YEAR);

        let (start, end) = heatmap_range(start_year, end_year);
        let jan_1 = NaiveDate::from_ymd_opt(start_year, 1, 1).unwrap();

        assert_eq!(start.weekday(), Weekday::Mon);
        assert!(start <= jan_1);

        if end_year == current_year {
            assert_eq!(end, Local::now().date_naive());
        } else {
            assert_eq!(end, NaiveDate::from_ymd_opt(end_year, 12, 31).unwrap());
        }
    }

    #[test]
    fn validate_selected_year_range_rejects_inverted_range() {
        let err = validate_selected_year_range(2025, 2024).unwrap_err();
        assert_eq!(err, "Start year must be less than or equal to end year");
    }

    #[test]
    fn validate_selected_year_range_rejects_out_of_bounds_years() {
        let current_year = Local::now().year();
        let err = validate_selected_year_range(MIN_SUPPORTED_YEAR - 1, current_year).unwrap_err();
        assert_eq!(
            err,
            format!("Start year must be between {MIN_SUPPORTED_YEAR} and {current_year}")
        );
    }

    // ── analytics helpers ────────────────────────────────────────────────────

    #[test]
    fn top_repositories_are_ranked_and_limited() {
        let repo_counts = vec![
            (PathBuf::from("/r1"), 11),
            (PathBuf::from("/r2"), 5),
            (PathBuf::from("/r3"), 7),
            (PathBuf::from("/r4"), 2),
            (PathBuf::from("/r5"), 3),
            (PathBuf::from("/r6"), 1),
        ];

        let top = compute_top_repositories(&repo_counts, 20, 5);

        assert_eq!(top.len(), 5);
        assert!(top[0].repo_path.ends_with("r1"));
        assert_eq!(top[0].commit_count, 11);
        assert!((top[0].percentage_of_total - 55.0).abs() < 0.0001);
        assert!(top[4].repo_path.ends_with("r4"));
    }

    #[test]
    fn streak_metrics_include_current_and_longest() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();

        let mut contributions = BTreeMap::new();
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 1);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 1);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 1);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 2);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 1);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 10).unwrap(), 3);

        let streaks = compute_streak_metrics(&contributions, start, end);

        assert_eq!(streaks.longest_streak, 3);
        assert_eq!(streaks.current_streak, 1);
    }

    #[test]
    fn activity_day_metrics_counts_active_and_inactive_days() {
        let start = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 2, 5).unwrap();

        let mut contributions = BTreeMap::new();
        contributions.insert(NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(), 1);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 2, 4).unwrap(), 2);

        let activity = compute_activity_day_metrics(&contributions, start, end);

        assert_eq!(activity.total_days, 5);
        assert_eq!(activity.active_days, 2);
        assert_eq!(activity.inactive_days, 3);
    }

    #[test]
    fn weekly_buckets_use_iso_week_boundaries() {
        let start = NaiveDate::from_ymd_opt(2024, 12, 30).unwrap(); // ISO week 1, 2025
        let end = NaiveDate::from_ymd_opt(2025, 1, 12).unwrap();

        let mut contributions = BTreeMap::new();
        contributions.insert(NaiveDate::from_ymd_opt(2024, 12, 30).unwrap(), 2);
        contributions.insert(NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(), 3);
        contributions.insert(NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(), 4);

        let weeks = compute_weekly_buckets(&contributions, start, end);

        assert_eq!(weeks.len(), 2);
        assert_eq!(weeks[0].iso_year, 2025);
        assert_eq!(weeks[0].iso_week, 1);
        assert_eq!(weeks[0].count, 5);
        assert_eq!(weeks[1].iso_year, 2025);
        assert_eq!(weeks[1].iso_week, 2);
        assert_eq!(weeks[1].count, 4);
    }

    #[test]
    fn week_over_week_growth_uses_latest_two_weeks() {
        let weekly_totals = vec![
            WeeklyBucket {
                iso_year: 2026,
                iso_week: 10,
                start_date: NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 3, 8).unwrap(),
                count: 10,
            },
            WeeklyBucket {
                iso_year: 2026,
                iso_week: 11,
                start_date: NaiveDate::from_ymd_opt(2026, 3, 9).unwrap(),
                end_date: NaiveDate::from_ymd_opt(2026, 3, 15).unwrap(),
                count: 15,
            },
        ];

        let growth = compute_week_over_week_growth(&weekly_totals).unwrap();

        assert_eq!(growth.delta, 5);
        assert!((growth.percent_change.unwrap() - 50.0).abs() < 0.0001);
        assert_eq!(growth.current_iso_week, 11);
        assert_eq!(growth.previous_iso_week, 10);
    }

    #[test]
    fn moving_average_points_apply_rolling_window() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 8).unwrap();

        let mut contributions = BTreeMap::new();
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 1);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 2);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 3);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 4);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 5);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 6).unwrap(), 6);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 7).unwrap(), 7);
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 8);

        let points = compute_moving_average_points(&contributions, start, end, 7);

        assert_eq!(points.len(), 8);
        assert!((points[6].value - 4.0).abs() < 0.0001);
        assert!((points[7].value - 5.0).abs() < 0.0001);
    }

    #[test]
    fn peak_productivity_and_distribution_are_computed() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();

        let mut contributions = BTreeMap::new();
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 2); // Fri
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 5); // Sat
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 4).unwrap(), 3); // Sun
        contributions.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 4); // Thu

        let peak = compute_peak_productivity(&contributions, start, end);
        let distribution = compute_day_distribution(&contributions);

        assert_eq!(
            peak.best_day.unwrap().date,
            NaiveDate::from_ymd_opt(2026, 1, 3).unwrap()
        );
        assert_eq!(peak.best_week.unwrap().count, 10);
        assert_eq!(peak.best_month.unwrap().count, 14);

        assert_eq!(distribution.friday, 2);
        assert_eq!(distribution.saturday, 5);
        assert_eq!(distribution.sunday, 3);
        assert_eq!(distribution.thursday, 4);
        assert!((distribution.weekend_ratio - (8.0 / 14.0)).abs() < 0.0001);
    }

    // ── dedupe_paths ──────────────────────────────────────────────────────────

    #[cfg(windows)]
    #[test]
    fn dedupe_paths_case_insensitive_on_windows() {
        let deduped = dedupe_paths(vec![
            PathBuf::from("D:\\Repo"),
            PathBuf::from("d:\\repo"),
            PathBuf::from("C:\\Repo"),
        ]);

        assert_eq!(
            deduped,
            vec![PathBuf::from("C:\\Repo"), PathBuf::from("D:\\Repo")]
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn dedupe_paths_case_sensitive_on_unix() {
        let deduped = dedupe_paths(vec![
            PathBuf::from("/tmp/repo"),
            PathBuf::from("/tmp/Repo"),
            PathBuf::from("/tmp/repo"),
        ]);

        assert_eq!(
            deduped,
            vec![PathBuf::from("/tmp/Repo"), PathBuf::from("/tmp/repo")]
        );
    }
}
