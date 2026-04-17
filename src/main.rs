#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod git_process;
mod scanner;

use crate::git_process::new_std_git_command;
use chrono::{Datelike, Local};
use scanner::{
    DEFAULT_MAX_DEPTH, ScanResult, normalize_root_path, scan_local_contributions,
    scan_local_contributions_multi, validate_root_path,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use tauri::State;
use tokio_util::sync::CancellationToken;

// ── Constants ────────────────────────────────────────────────────────────────

const SETTINGS_DIR_NAME: &str = "local-git-stat";
const SETTINGS_FILE_NAME: &str = "preferences.json";
const CACHE_FILE_NAME: &str = "scan-cache.json";
const MIN_SUPPORTED_YEAR: i32 = 2005;

/// Maximum serialized cache size (5 MiB).  Scans producing more data than
/// this will not be persisted to disk, but the result is still returned to
/// the UI.
const MAX_CACHE_SIZE_BYTES: usize = 5 * 1024 * 1024;

/// Hard cap on paths accepted per `add_safe_directories` call.
const MAX_SAFE_DIR_PATHS: usize = 50;

// ── Tauri application state ──────────────────────────────────────────────────

/// Shared state that lets `cancel_scan` abort an in-progress `scan_contributions`.
struct ScanState {
    cancel_token: Mutex<Option<CancellationToken>>,
}

impl ScanState {
    fn new() -> Self {
        Self {
            cancel_token: Mutex::new(None),
        }
    }

    /// Replace any existing token and return the new one.
    fn arm(&self) -> CancellationToken {
        let token = CancellationToken::new();
        *self.cancel_token.lock().unwrap() = Some(token.clone());
        token
    }

    /// Cancel the current token (if any) and clear it.
    fn cancel(&self) {
        let mut guard = self.cancel_token.lock().unwrap();
        if let Some(token) = guard.take() {
            token.cancel();
        }
    }
}

// ── Data types ────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScanArgs {
    username: String,
    root_path: String,
    #[serde(default = "default_max_depth")]
    max_depth: usize,
    #[serde(default = "default_selected_year")]
    selected_year: i32,
    #[serde(default)]
    selected_year_end: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavePreferencesArgs {
    username: String,
    root_path: String,
    auto_save: bool,
    #[serde(default = "default_max_depth")]
    max_depth: usize,
    #[serde(default = "default_selected_year")]
    selected_year: i32,
    #[serde(default)]
    selected_year_end: Option<i32>,
}

fn default_auto_save() -> bool {
    true
}

fn default_max_depth() -> usize {
    DEFAULT_MAX_DEPTH
}

fn default_selected_year() -> i32 {
    Local::now().year()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedPreferences {
    username: String,
    root_path: String,
    #[serde(default = "default_auto_save")]
    auto_save: bool,
    #[serde(default = "default_max_depth")]
    max_depth: usize,
    #[serde(default = "default_selected_year")]
    selected_year: i32,
    #[serde(default)]
    selected_year_end: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DailyContribution {
    date: String,
    count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct TopRepositoryPayload {
    repo_path: String,
    commit_count: u32,
    percentage_of_total: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct StreakMetricsPayload {
    current_streak: u32,
    longest_streak: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct ActivityDayMetricsPayload {
    active_days: u32,
    inactive_days: u32,
    total_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeakDayPayload {
    date: String,
    count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeakWeekPayload {
    iso_year: i32,
    iso_week: u32,
    start_date: String,
    end_date: String,
    count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PeakMonthPayload {
    year: i32,
    month: u32,
    count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct PeakProductivityPayload {
    best_day: Option<PeakDayPayload>,
    best_week: Option<PeakWeekPayload>,
    best_month: Option<PeakMonthPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WeeklyBucketPayload {
    iso_year: i32,
    iso_week: u32,
    start_date: String,
    end_date: String,
    count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WeekOverWeekGrowthPayload {
    current_iso_year: i32,
    current_iso_week: u32,
    current_week_start: String,
    current_week_end: String,
    current_week_count: u32,
    previous_iso_year: i32,
    previous_iso_week: u32,
    previous_week_start: String,
    previous_week_end: String,
    previous_week_count: u32,
    delta: i32,
    percent_change: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MovingAveragePointPayload {
    date: String,
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct WeeklyTrendPayload {
    week_over_week_growth: Option<WeekOverWeekGrowthPayload>,
    moving_average_points: Vec<MovingAveragePointPayload>,
    weekly_totals: Vec<WeeklyBucketPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct DayDistributionPayload {
    monday: u32,
    tuesday: u32,
    wednesday: u32,
    thursday: u32,
    friday: u32,
    saturday: u32,
    sunday: u32,
    weekend_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScanResponse {
    username: String,
    root_path: String,
    #[serde(default = "default_selected_year")]
    selected_year: i32,
    #[serde(default = "default_selected_year")]
    selected_year_start: i32,
    #[serde(default = "default_selected_year")]
    selected_year_end: i32,
    repos_scanned: usize,
    start_date: String,
    end_date: String,
    total_contributions: u32,
    max_daily_count: u32,
    contributions: Vec<DailyContribution>,
    repo_errors: Vec<String>,
    #[serde(default)]
    top_repositories: Vec<TopRepositoryPayload>,
    #[serde(default)]
    streak_metrics: StreakMetricsPayload,
    #[serde(default)]
    activity_days: ActivityDayMetricsPayload,
    #[serde(default)]
    peak_productivity: PeakProductivityPayload,
    #[serde(default)]
    weekly_trend: WeeklyTrendPayload,
    #[serde(default)]
    day_distribution: DayDistributionPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CachedScanFile {
    username: String,
    root_path: String,
    #[serde(default)]
    selected_year: Option<i32>,
    #[serde(default)]
    selected_year_start: Option<i32>,
    #[serde(default)]
    selected_year_end: Option<i32>,
    scanned_at: String,
    data: ScanResponse,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CachedScanPayload {
    scanned_at: String,
    data: ScanResponse,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SafeDirectoryFixResponse {
    attempted: usize,
    applied: usize,
    failed: Vec<String>,
}

// ── Conversions ────────────────────────────────────────────────────────────────

fn format_date(date: chrono::NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

impl From<ScanResult> for ScanResponse {
    fn from(value: ScanResult) -> Self {
        let total_contributions = value.total_contributions();
        let max_daily_count = value.max_daily_count();

        let ScanResult {
            username,
            root_path,
            selected_year_start,
            selected_year_end,
            repos_scanned,
            contributions,
            start_date,
            end_date,
            analytics,
            repo_errors,
        } = value;
        let contributions = contributions
            .into_iter()
            .map(|(date, count)| DailyContribution {
                date: format_date(date),
                count,
            })
            .collect();

        let top_repositories = analytics
            .top_repositories
            .into_iter()
            .map(|repo| TopRepositoryPayload {
                repo_path: repo.repo_path,
                commit_count: repo.commit_count,
                percentage_of_total: repo.percentage_of_total,
            })
            .collect();

        let streak_metrics = StreakMetricsPayload {
            current_streak: analytics.streak_metrics.current_streak,
            longest_streak: analytics.streak_metrics.longest_streak,
        };

        let activity_days = ActivityDayMetricsPayload {
            active_days: analytics.activity_days.active_days,
            inactive_days: analytics.activity_days.inactive_days,
            total_days: analytics.activity_days.total_days,
        };

        let peak_productivity = PeakProductivityPayload {
            best_day: analytics
                .peak_productivity
                .best_day
                .map(|best_day| PeakDayPayload {
                    date: format_date(best_day.date),
                    count: best_day.count,
                }),
            best_week: analytics
                .peak_productivity
                .best_week
                .map(|best_week| PeakWeekPayload {
                    iso_year: best_week.iso_year,
                    iso_week: best_week.iso_week,
                    start_date: format_date(best_week.start_date),
                    end_date: format_date(best_week.end_date),
                    count: best_week.count,
                }),
            best_month: analytics
                .peak_productivity
                .best_month
                .map(|best_month| PeakMonthPayload {
                    year: best_month.year,
                    month: best_month.month,
                    count: best_month.count,
                }),
        };

        let weekly_trend = WeeklyTrendPayload {
            week_over_week_growth: analytics.weekly_trend.week_over_week_growth.map(|growth| {
                WeekOverWeekGrowthPayload {
                    current_iso_year: growth.current_iso_year,
                    current_iso_week: growth.current_iso_week,
                    current_week_start: format_date(growth.current_week_start),
                    current_week_end: format_date(growth.current_week_end),
                    current_week_count: growth.current_week_count,
                    previous_iso_year: growth.previous_iso_year,
                    previous_iso_week: growth.previous_iso_week,
                    previous_week_start: format_date(growth.previous_week_start),
                    previous_week_end: format_date(growth.previous_week_end),
                    previous_week_count: growth.previous_week_count,
                    delta: growth.delta,
                    percent_change: growth.percent_change,
                }
            }),
            moving_average_points: analytics
                .weekly_trend
                .moving_average_points
                .into_iter()
                .map(|point| MovingAveragePointPayload {
                    date: format_date(point.date),
                    value: point.value,
                })
                .collect(),
            weekly_totals: analytics
                .weekly_trend
                .weekly_totals
                .into_iter()
                .map(|bucket| WeeklyBucketPayload {
                    iso_year: bucket.iso_year,
                    iso_week: bucket.iso_week,
                    start_date: format_date(bucket.start_date),
                    end_date: format_date(bucket.end_date),
                    count: bucket.count,
                })
                .collect(),
        };

        let day_distribution = DayDistributionPayload {
            monday: analytics.day_distribution.monday,
            tuesday: analytics.day_distribution.tuesday,
            wednesday: analytics.day_distribution.wednesday,
            thursday: analytics.day_distribution.thursday,
            friday: analytics.day_distribution.friday,
            saturday: analytics.day_distribution.saturday,
            sunday: analytics.day_distribution.sunday,
            weekend_ratio: analytics.day_distribution.weekend_ratio,
        };

        Self {
            username,
            root_path: root_path.display().to_string(),
            selected_year: selected_year_start,
            selected_year_start,
            selected_year_end,
            repos_scanned,
            start_date: format_date(start_date),
            end_date: format_date(end_date),
            total_contributions,
            max_daily_count,
            contributions,
            repo_errors,
            top_repositories,
            streak_metrics,
            activity_days,
            peak_productivity,
            weekly_trend,
            day_distribution,
        }
    }
}

// ── Argument validation ────────────────────────────────────────────────────────

fn validate_scan_args(args: &ScanArgs) -> Result<(String, Vec<PathBuf>, String, i32, i32), String> {
    let username = args.username.trim().to_string();
    if username.is_empty() {
        return Err("Username is required".to_string());
    }

    let normalized_paths = parse_root_paths(&args.root_path);
    if normalized_paths.is_empty() {
        return Err("Path is required".to_string());
    }

    let selected_year_start = args.selected_year;
    let selected_year_end = args.selected_year_end.unwrap_or(selected_year_start);
    let (selected_year_start, selected_year_end) =
        validate_selected_year_range(selected_year_start, selected_year_end)?;

    let serialized_paths = serialize_root_paths(&normalized_paths);
    Ok((
        username,
        normalized_paths,
        serialized_paths,
        selected_year_start,
        selected_year_end,
    ))
}

// ── App data paths ────────────────────────────────────────────────────────────

fn app_data_file_path(file_name: &str) -> PathBuf {
    #[cfg(windows)]
    {
        if let Ok(app_data) = std::env::var("APPDATA") {
            return PathBuf::from(app_data)
                .join(SETTINGS_DIR_NAME)
                .join(file_name);
        }
    }

    if let Ok(xdg_config) = std::env::var("XDG_CONFIG_HOME") {
        return PathBuf::from(xdg_config)
            .join(SETTINGS_DIR_NAME)
            .join(file_name);
    }

    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home)
            .join(".config")
            .join(SETTINGS_DIR_NAME)
            .join(file_name);
    }

    PathBuf::from(file_name)
}

fn preferences_file_path() -> PathBuf {
    app_data_file_path(SETTINGS_FILE_NAME)
}

fn cache_file_path() -> PathBuf {
    app_data_file_path(CACHE_FILE_NAME)
}

fn ensure_parent_dir(file_path: &Path) -> Result<(), String> {
    if let Some(parent_dir) = file_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent_dir)
            .map_err(|err| format!("Failed to create application data folder: {err}"))?;
    }

    Ok(())
}

// ── Atomic file write ─────────────────────────────────────────────────────────

/// Write `content` to `target` atomically by first writing to a `.tmp` file,
/// then renaming it.  This prevents partial or corrupt writes on power loss or
/// crashes.
fn atomic_write(target: &Path, content: &str) -> Result<(), String> {
    let temp_path = target.with_extension("tmp");

    fs::write(&temp_path, content).map_err(|e| format!("Failed to write temporary file: {e}"))?;

    fs::rename(&temp_path, target).map_err(|e| {
        // Clean up orphaned temp file on rename failure (best effort)
        let _ = fs::remove_file(&temp_path);
        format!(
            "Failed to complete atomic write to {}: {e}",
            target.display()
        )
    })?;

    Ok(())
}

// ── Path / string helpers ─────────────────────────────────────────────────────

fn normalize_path_key(path: &Path) -> String {
    let normalized = path.display().to_string();

    #[cfg(windows)]
    {
        normalized.to_ascii_lowercase()
    }

    #[cfg(not(windows))]
    {
        normalized
    }
}

fn parse_root_paths(raw_paths: &str) -> Vec<PathBuf> {
    let mut unique = std::collections::BTreeMap::<String, PathBuf>::new();

    for part in raw_paths.split([';', '\n', '\r']) {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        let normalized = normalize_root_path(trimmed);
        if normalized.as_os_str().is_empty() {
            continue;
        }

        let key = normalize_path_key(&normalized);
        unique.entry(key).or_insert(normalized);
    }

    unique.into_values().collect()
}

fn serialize_root_paths(paths: &[PathBuf]) -> String {
    paths
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join("; ")
}

fn normalize_path_string(raw_paths: &str) -> String {
    let parsed = parse_root_paths(raw_paths);
    serialize_root_paths(&parsed)
}

fn same_root_path_set(left: &str, right: &str) -> bool {
    let left_paths = parse_root_paths(left);
    let right_paths = parse_root_paths(right);

    if left_paths.len() != right_paths.len() {
        return false;
    }

    left_paths
        .iter()
        .zip(right_paths.iter())
        .all(|(left, right)| {
            let left = left.display().to_string();
            let right = right.display().to_string();
            same_path(&left, &right)
        })
}

fn same_path(left: &str, right: &str) -> bool {
    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(right)
    }

    #[cfg(not(windows))]
    {
        left == right
    }
}

fn same_username(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn normalize_selected_year(year: i32) -> i32 {
    let current_year = Local::now().year();
    year.clamp(MIN_SUPPORTED_YEAR, current_year)
}

fn normalize_selected_year_range(
    selected_year_start: i32,
    selected_year_end: Option<i32>,
) -> (i32, i32) {
    let start = normalize_selected_year(selected_year_start);
    let raw_end = selected_year_end.unwrap_or(selected_year_start);
    let end = normalize_selected_year(raw_end);

    if start <= end {
        (start, end)
    } else {
        (start, start)
    }
}

fn validate_selected_year_range(
    selected_year_start: i32,
    selected_year_end: i32,
) -> Result<(i32, i32), String> {
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

    Ok((selected_year_start, selected_year_end))
}

fn normalize_cached_year_range(
    selected_year: Option<i32>,
    selected_year_start: Option<i32>,
    selected_year_end: Option<i32>,
) -> Option<(i32, i32)> {
    let raw_start = selected_year_start.or(selected_year)?;
    let raw_end = selected_year_end.or(Some(raw_start));

    Some(normalize_selected_year_range(raw_start, raw_end))
}

// ── Preferences persistence ───────────────────────────────────────────────────

fn save_preferences_to_disk(preferences: &SavedPreferences) -> Result<(), String> {
    let file_path = preferences_file_path();
    ensure_parent_dir(&file_path)?;

    let payload = serde_json::to_string_pretty(preferences)
        .map_err(|err| format!("Failed to serialize preferences: {err}"))?;

    atomic_write(&file_path, &payload)
}

fn load_preferences_from_disk() -> Result<Option<SavedPreferences>, String> {
    let file_path = preferences_file_path();

    if !file_path.exists() {
        return Ok(None);
    }

    let payload = fs::read_to_string(&file_path).map_err(|err| {
        format!(
            "Failed to read preferences file {}: {err}",
            file_path.display()
        )
    })?;

    let mut preferences: SavedPreferences = serde_json::from_str(&payload)
        .map_err(|err| format!("Invalid preferences file format: {err}"))?;

    preferences.username = preferences.username.trim().to_string();
    preferences.root_path = normalize_path_string(&preferences.root_path);
    let (selected_year_start, selected_year_end) =
        normalize_selected_year_range(preferences.selected_year, preferences.selected_year_end);
    preferences.selected_year = selected_year_start;
    preferences.selected_year_end = Some(selected_year_end);

    if preferences.username.is_empty() || preferences.root_path.trim().is_empty() {
        return Ok(None);
    }

    Ok(Some(preferences))
}

// ── Cache persistence ─────────────────────────────────────────────────────────

fn save_cache_to_disk(scan: &ScanResponse) -> Result<(), String> {
    let file_path = cache_file_path();
    ensure_parent_dir(&file_path)?;

    let cache = CachedScanFile {
        username: scan.username.trim().to_string(),
        root_path: normalize_path_string(&scan.root_path),
        selected_year: Some(scan.selected_year_start),
        selected_year_start: Some(scan.selected_year_start),
        selected_year_end: Some(scan.selected_year_end),
        scanned_at: Local::now().to_rfc3339(),
        data: scan.clone(),
    };

    let payload = serde_json::to_string_pretty(&cache)
        .map_err(|err| format!("Failed to serialize statistics cache: {err}"))?;

    // Guard against runaway cache growth (#6)
    if payload.len() > MAX_CACHE_SIZE_BYTES {
        return Err(format!(
            "Cache exceeded {} MB limit, not saved",
            MAX_CACHE_SIZE_BYTES / (1024 * 1024)
        ));
    }

    atomic_write(&file_path, &payload)
}

fn load_cache_from_disk() -> Result<Option<CachedScanFile>, String> {
    let file_path = cache_file_path();

    if !file_path.exists() {
        return Ok(None);
    }

    let payload = fs::read_to_string(&file_path)
        .map_err(|err| format!("Failed to read cache file {}: {err}", file_path.display()))?;

    // Recovery: if the cache is corrupt, back it up and return None so the app
    // can continue with a fresh scan instead of crashing (#6).
    let mut cache: CachedScanFile = match serde_json::from_str(&payload) {
        Ok(c) => c,
        Err(_err) => {
            let backup_path = file_path.with_extension("json.bak");
            let _ = fs::rename(&file_path, &backup_path);
            eprintln!(
                "Statistics cache corrupted — backed up to {} and ignored",
                backup_path.display()
            );
            return Ok(None);
        }
    };

    cache.username = cache.username.trim().to_string();
    cache.root_path = normalize_path_string(&cache.root_path);

    if cache.username.is_empty() || cache.root_path.is_empty() {
        return Ok(None);
    }

    let Some((selected_year_start, selected_year_end)) = normalize_cached_year_range(
        cache.selected_year,
        cache.selected_year_start,
        cache.selected_year_end,
    ) else {
        // Ignore cache files that do not contain any year information.
        return Ok(None);
    };

    cache.selected_year = Some(selected_year_start);
    cache.selected_year_start = Some(selected_year_start);
    cache.selected_year_end = Some(selected_year_end);

    cache.data.username = cache.username.clone();
    cache.data.root_path = cache.root_path.clone();
    cache.data.selected_year = selected_year_start;
    cache.data.selected_year_start = selected_year_start;
    cache.data.selected_year_end = selected_year_end;

    Ok(Some(cache))
}

fn load_matching_cache(
    username: &str,
    root_path: &str,
    selected_year_start: i32,
    selected_year_end: i32,
) -> Result<Option<CachedScanPayload>, String> {
    let requested_username = username.trim();

    let Some(cache) = load_cache_from_disk()? else {
        return Ok(None);
    };

    let cache_year_start = cache
        .selected_year_start
        .or(cache.selected_year)
        .unwrap_or_default();
    let cache_year_end = cache.selected_year_end.unwrap_or(cache_year_start);
    if !same_username(&cache.username, requested_username)
        || !same_root_path_set(&cache.root_path, root_path)
        || cache_year_start != selected_year_start
        || cache_year_end != selected_year_end
    {
        return Ok(None);
    }

    Ok(Some(CachedScanPayload {
        scanned_at: cache.scanned_at,
        data: cache.data,
    }))
}

// ── Tauri commands ─────────────────────────────────────────────────────────────

#[tauri::command]
async fn scan_contributions(
    args: ScanArgs,
    state: State<'_, ScanState>,
) -> Result<ScanResponse, String> {
    let (username, normalized_paths, serialized_paths, selected_year_start, selected_year_end) =
        validate_scan_args(&args)?;

    // Validate path early — provides friendlier errors before spawning tasks
    for path in &normalized_paths {
        validate_root_path(path)?;
    }

    // Arm cancellation token; any previous in-flight scan is cancelled (#3)
    let cancel = state.arm();

    let result = if normalized_paths.len() == 1 {
        scan_local_contributions(
            &username,
            &normalized_paths[0],
            args.max_depth,
            selected_year_start,
            selected_year_end,
            cancel,
        )
        .await
    } else {
        scan_local_contributions_multi(
            &username,
            &normalized_paths,
            args.max_depth,
            selected_year_start,
            selected_year_end,
            cancel,
        )
        .await
    };

    // Clear the token now that the scan finished (success or error)
    state.cancel();

    let result = result?;
    let mut response: ScanResponse = result.into();
    response.root_path = serialized_paths;

    if let Err(error) = save_cache_to_disk(&response) {
        eprintln!("Failed to save statistics cache: {error}");
    }

    Ok(response)
}

/// Abort any scan running in the background.  Safe to call when idle.
#[tauri::command]
fn cancel_scan(state: State<'_, ScanState>) {
    state.cancel();
}

#[tauri::command]
fn save_preferences(args: SavePreferencesArgs) -> Result<SavedPreferences, String> {
    let scan_args = ScanArgs {
        username: args.username.clone(),
        root_path: args.root_path.clone(),
        max_depth: args.max_depth,
        selected_year: args.selected_year,
        selected_year_end: args.selected_year_end,
    };
    let (username, _normalized_paths, serialized_paths, selected_year_start, selected_year_end) =
        validate_scan_args(&scan_args)?;

    let preferences = SavedPreferences {
        username,
        root_path: serialized_paths,
        auto_save: args.auto_save,
        max_depth: args.max_depth,
        selected_year: selected_year_start,
        selected_year_end: Some(selected_year_end),
    };

    save_preferences_to_disk(&preferences)?;
    Ok(preferences)
}

#[tauri::command]
fn load_preferences() -> Result<Option<SavedPreferences>, String> {
    load_preferences_from_disk()
}

#[tauri::command]
fn load_cached_scan(args: ScanArgs) -> Result<Option<CachedScanPayload>, String> {
    let (username, _normalized_paths, serialized_paths, selected_year_start, selected_year_end) =
        validate_scan_args(&args)?;
    load_matching_cache(
        &username,
        &serialized_paths,
        selected_year_start,
        selected_year_end,
    )
}

#[tauri::command]
fn add_safe_directories(paths: Vec<String>) -> Result<SafeDirectoryFixResponse, String> {
    if paths.is_empty() {
        return Err("No safe.directory paths to run".to_string());
    }

    // Guard: reject oversized batches (#7)
    if paths.len() > MAX_SAFE_DIR_PATHS {
        return Err(format!(
            "Too many paths ({} exceeds limit of {})",
            paths.len(),
            MAX_SAFE_DIR_PATHS
        ));
    }

    let unique_paths: BTreeSet<String> = paths
        .into_iter()
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .filter(|p| is_safe_path_input(p))
        .collect();

    if unique_paths.is_empty() {
        return Err("All safe.directory paths are empty, invalid, or rejected".to_string());
    }

    let attempted = unique_paths.len();
    let mut applied = 0usize;
    let mut failed = Vec::new();

    for path in unique_paths {
        match new_std_git_command()
            .args(["config", "--global", "--add", "safe.directory"])
            .arg(&path)
            .output()
        {
            Ok(output) if output.status.success() => {
                applied += 1;
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
                let reason = if stderr.is_empty() {
                    "git config failed without error details".to_string()
                } else {
                    stderr
                };
                failed.push(format!("{path} ({reason})"));
            }
            Err(err) => {
                failed.push(format!("{path} ({err})"));
            }
        }
    }

    Ok(SafeDirectoryFixResponse {
        attempted,
        applied,
        failed,
    })
}

/// Reject paths that contain shell-injection-style characters or are
/// suspiciously short (e.g. bare `/` or `C:`).  This is an additional
/// safety layer on top of the Git binary's own validation.
fn is_safe_path_input(path: &str) -> bool {
    if path.len() <= 2 {
        return false;
    }

    // Reject obvious shell metacharacters
    let dangerous: &[char] = &[';', '|', '&', '`', '$', '(', ')', '<', '>', '\n', '\r'];
    !path.chars().any(|c| dangerous.contains(&c))
}

#[tauri::command]
fn pick_folder() -> Option<String> {
    rfd::FileDialog::new()
        .pick_folder()
        .map(|path| path.display().to_string())
}

#[tauri::command]
fn default_username() -> String {
    std::env::var("GIT_AUTHOR_NAME")
        .ok()
        .filter(|name| !name.trim().is_empty())
        .or_else(|| {
            std::env::var("GIT_COMMITTER_NAME")
                .ok()
                .filter(|name| !name.trim().is_empty())
        })
        .or_else(|| std::env::var("USERNAME").ok())
        .or_else(|| std::env::var("USER").ok())
        .unwrap_or_default()
}

#[tauri::command]
fn default_path() -> Result<String, String> {
    let path = std::env::current_dir()
        .map_err(|err| format!("Failed to read current directory: {err}"))?;
    Ok(path.display().to_string())
}

// ── Entry point ───────────────────────────────────────────────────────────────

fn main() {
    tauri::Builder::default()
        .manage(ScanState::new())
        .invoke_handler(tauri::generate_handler![
            scan_contributions,
            cancel_scan,
            save_preferences,
            load_preferences,
            load_cached_scan,
            add_safe_directories,
            pick_folder,
            default_username,
            default_path
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── same_path ─────────────────────────────────────────────────────────────

    #[cfg(windows)]
    #[test]
    fn same_path_case_insensitive_on_windows() {
        assert!(same_path("D:\\Projects", "d:\\projects"));
        assert!(!same_path("D:\\Projects", "C:\\Projects"));
    }

    #[cfg(not(windows))]
    #[test]
    fn same_path_case_sensitive_on_unix() {
        assert!(same_path("/home/alice/projects", "/home/alice/projects"));
        assert!(!same_path("/home/Alice/projects", "/home/alice/projects"));
    }

    // ── same_username ─────────────────────────────────────────────────────────

    #[test]
    fn same_username_is_case_insensitive() {
        assert!(same_username("Alice", "alice"));
        assert!(same_username("ALICE", "alice"));
        assert!(!same_username("alice", "bob"));
    }

    // ── is_safe_path_input ────────────────────────────────────────────────────

    #[test]
    fn rejects_short_paths() {
        assert!(!is_safe_path_input(""));
        assert!(!is_safe_path_input("/"));
        assert!(!is_safe_path_input("C:"));
    }

    #[test]
    fn rejects_shell_metacharacters() {
        assert!(!is_safe_path_input("/home/alice; rm -rf /"));
        assert!(!is_safe_path_input("/home/alice | cat /etc/passwd"));
        assert!(!is_safe_path_input("/home/alice$(whoami)"));
    }

    #[test]
    fn accepts_normal_paths() {
        assert!(is_safe_path_input("/home/alice/projects"));
        assert!(is_safe_path_input("D:\\Projects\\my-repo"));
        assert!(is_safe_path_input("C:\\Users\\alice\\code"));
    }

    // ── atomic_write ──────────────────────────────────────────────────────────

    #[test]
    fn atomic_write_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("output.json");
        atomic_write(&target, r#"{"ok": true}"#).unwrap();
        let content = fs::read_to_string(&target).unwrap();
        assert_eq!(content, r#"{"ok": true}"#);
        // No temp file should remain
        assert!(!dir.path().join("output.tmp").exists());
    }

    #[test]
    fn atomic_write_overwrites_existing() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("output.json");
        fs::write(&target, "old").unwrap();
        atomic_write(&target, "new").unwrap();
        assert_eq!(fs::read_to_string(&target).unwrap(), "new");
    }

    // ── normalize_path_string ─────────────────────────────────────────────────

    #[test]
    fn normalize_path_string_trims() {
        let result = normalize_path_string("  /some/path  ");
        assert_eq!(result, "/some/path");
    }

    #[test]
    fn parse_root_paths_supports_multi_separator_and_dedup() {
        #[cfg(windows)]
        {
            let paths = parse_root_paths(" D:\\Code ;\nC:\\Work\r\nd:\\code;; ");
            assert_eq!(
                paths,
                vec![PathBuf::from("C:\\Work"), PathBuf::from("D:\\Code")]
            );
        }

        #[cfg(not(windows))]
        {
            let paths = parse_root_paths(" /zeta ;\n/alpha\r\n/zeta;; ");
            assert_eq!(paths, vec![PathBuf::from("/alpha"), PathBuf::from("/zeta")]);
        }
    }

    #[test]
    fn same_root_path_set_ignores_separator_whitespace_and_order() {
        let left = "/work/a;\n/work/b";
        let right = " /work/b \r\n /work/a ";
        assert!(same_root_path_set(left, right));
    }

    #[cfg(windows)]
    #[test]
    fn same_root_path_set_is_case_insensitive_on_windows() {
        assert!(same_root_path_set(
            "D:\\Code;C:\\Work",
            "c:\\work\r\nd:\\code"
        ));
    }

    #[cfg(not(windows))]
    #[test]
    fn same_root_path_set_is_case_sensitive_on_unix() {
        assert!(!same_root_path_set("/Work", "/work"));
    }
}
