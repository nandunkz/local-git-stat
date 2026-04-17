const monthLabel = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const dayLabel = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

const heatPalette = {
  level0: "#022c22",
  level1: "#065f46",
  level2: "#00FF66",
  level3: "#00FFCC",
  level4: "#2563eb",
  future: "rgba(0,0,0,0.4)"
};

const tauriInvoke = window.__TAURI__?.core?.invoke ?? window.__TAURI__?.tauri?.invoke;
const CACHE_TTL_MINUTES = 20;
const SAFE_DIRECTORY_COMMAND = "git config --global --add safe.directory";
const DEFAULT_MAX_DEPTH = 6;
const MIN_SCAN_DEPTH = 1;
const MAX_SCAN_DEPTH = 20;
const MIN_SUPPORTED_YEAR = 2005;
const CURRENT_YEAR = new Date().getFullYear();

const state = {
  autoSave: true,
  maxDepth: DEFAULT_MAX_DEPTH,
  selectedYearStart: CURRENT_YEAR,
  selectedYearEnd: CURRENT_YEAR,
  cacheTtlMinutes: CACHE_TTL_MINUTES,
  cacheTtlMs: CACHE_TTL_MINUTES * 60 * 1000,
  safeDirectoryPaths: [],
  scanning: false
};

let yearRangeRefreshTimer = null;

const ui = {
  form: document.querySelector("#scan-form"),
  username: document.querySelector("#username"),
  rootPath: document.querySelector("#root-path"),
  scanButton: document.querySelector("#scan-btn"),
  cancelButton: document.querySelector("#cancel-btn"),
  refreshButton: document.querySelector("#refresh-btn"),
  pickFolderButton: document.querySelector("#pick-folder-btn"),
  currentButton: document.querySelector("#current-btn"),
  savePrefButton: document.querySelector("#save-pref-btn"),
  autoSaveToggle: document.querySelector("#auto-save-toggle"),
  maxDepthInput: document.querySelector("#max-depth-input"),
  depthDecreaseButton: document.querySelector("#depth-decrease-btn"),
  depthIncreaseButton: document.querySelector("#depth-increase-btn"),
  yearStartDropdown: document.querySelector("#year-start-dropdown"),
  yearEndDropdown: document.querySelector("#year-end-dropdown"),
  yearStartInput: document.querySelector("#year-start-input"),
  yearEndInput: document.querySelector("#year-end-input"),
  yearStartTrigger: document.querySelector("#year-start-trigger"),
  yearEndTrigger: document.querySelector("#year-end-trigger"),
  yearStartLabel: document.querySelector("#year-start-label"),
  yearEndLabel: document.querySelector("#year-end-label"),
  yearStartPanel: document.querySelector("#year-start-panel"),
  yearEndPanel: document.querySelector("#year-end-panel"),
  cacheInfo: document.querySelector("#cache-info"),
  statusLoader: document.querySelector("#status-loader"),
  status: document.querySelector("#status"),
  summary: document.querySelector("#summary"),
  summaryTotal: document.querySelector("#summary-total"),
  summaryRepos: document.querySelector("#summary-repos"),
  summaryUser: document.querySelector("#summary-user"),
  summaryPath: document.querySelector("#summary-path"),
  panel: document.querySelector("#result-panel"),
  analyticsPanel: document.querySelector("#analytics-panel"),
  title: document.querySelector("#result-title"),
  period: document.querySelector("#result-period"),
  heatmap: document.querySelector("#heatmap"),
  topRepoList: document.querySelector("#top-repo-list"),
  currentStreak: document.querySelector("#current-streak"),
  longestStreak: document.querySelector("#longest-streak"),
  activeDays: document.querySelector("#active-days"),
  inactiveDays: document.querySelector("#inactive-days"),
  activeDaysRatio: document.querySelector("#active-days-ratio"),
  activityDaysBar: document.querySelector("#activity-days-bar"),
  peakDay: document.querySelector("#peak-day"),
  peakWeek: document.querySelector("#peak-week"),
  peakMonth: document.querySelector("#peak-month"),
  wowDelta: document.querySelector("#wow-delta"),
  wowDetails: document.querySelector("#wow-details"),
  weeklyBars: document.querySelector("#weekly-bars"),
  movingAverageBars: document.querySelector("#moving-average-bars"),
  movingAverageLatest: document.querySelector("#moving-average-latest"),
  weekdayDistribution: document.querySelector("#weekday-distribution"),
  weekendRatio: document.querySelector("#weekend-ratio"),
  errorsPanel: document.querySelector("#errors-panel"),
  fixAllSafeDirButton: document.querySelector("#fix-all-safe-dir-btn"),
  errorAlert: document.querySelector("#error-alert"),
  errorsList: document.querySelector("#error-list")
};

initializeYearDropdowns();
bootstrap();

// ── Event listeners ───────────────────────────────────────────────────────────

ui.form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await scanFromInputs({
    startupMode: false,
    persistOnSuccess: state.autoSave,
    isRefresh: false
  });
});

ui.cancelButton?.addEventListener("click", async () => {
  if (!tauriInvoke) return;
  try {
    await tauriInvoke("cancel_scan");
    setStatus("Scan cancelled by user.", "idle");
  } catch (error) {
    setStatus(`Failed to cancel scan: ${readError(error)}`, "error");
  }
});

ui.refreshButton.addEventListener("click", async () => {
  await scanFromInputs({
    startupMode: false,
    persistOnSuccess: state.autoSave,
    isRefresh: true
  });
});

ui.pickFolderButton.addEventListener("click", async () => {
  if (!tauriInvoke) {
    setStatus("Folder picker is only available when running within Tauri.", "error");
    return;
  }

  try {
    const selected = await tauriInvoke("pick_folder");
    if (typeof selected === "string" && selected.trim().length > 0) {
      ui.rootPath.value = selected.trim();
      setStatus("Folder successfully selected.", "ok");
    }
  } catch (error) {
    setStatus(`Failed to open folder picker: ${readError(error)}`, "error");
  }
});

ui.currentButton.addEventListener("click", async () => {
  if (!tauriInvoke) {
    return;
  }

  try {
    const path = await tauriInvoke("default_path");
    if (typeof path === "string" && path.length > 0) {
      ui.rootPath.value = path;
    }
  } catch (_) {
    setStatus("Failed to read current folder", "error");
  }
});

ui.savePrefButton.addEventListener("click", async () => {
  const persisted = await persistCurrentPreferences();
  if (persisted.saved) {
    setStatus("Preferences successfully saved manually.", "ok");
    return;
  }

  setStatus(`Manual save failed: ${persisted.error}`, "error");
});

ui.autoSaveToggle.addEventListener("change", async () => {
  state.autoSave = ui.autoSaveToggle.checked;

  const persisted = await persistCurrentPreferences();
  if (persisted.saved) {
    setStatus(
      state.autoSave
        ? "Auto-save enabled. Preferences will be saved automatically upon successful scan."
        : "Auto-save disabled. Use Save Now to persist manually.",
      "idle"
    );
    return;
  }

  setStatus(
    `Auto-save toggled, but failed to persist setting: ${persisted.error}`,
    "error"
  );
});

ui.maxDepthInput?.addEventListener("change", () => {
  state.maxDepth = normalizeDepthValue(ui.maxDepthInput.value, state.maxDepth);
  ui.maxDepthInput.value = String(state.maxDepth);
});

ui.maxDepthInput?.addEventListener("blur", () => {
  state.maxDepth = normalizeDepthValue(ui.maxDepthInput.value, state.maxDepth);
  ui.maxDepthInput.value = String(state.maxDepth);
});

ui.depthDecreaseButton?.addEventListener("click", () => {
  stepDepthValue(-1);
});

ui.depthIncreaseButton?.addEventListener("click", () => {
  stepDepthValue(1);
});

ui.yearStartInput?.addEventListener("change", () => {
  const startYear = sanitizeSelectedYear(ui.yearStartInput.value);
  const endYear = sanitizeSelectedYear(ui.yearEndInput?.value ?? state.selectedYearEnd);
  const normalizedEndYear = Math.max(startYear, endYear);

  syncYearRangeState(startYear, normalizedEndYear);
  scheduleYearRangeAutoRefresh();
});

ui.yearEndInput?.addEventListener("change", () => {
  const endYear = sanitizeSelectedYear(ui.yearEndInput.value);
  const startYear = sanitizeSelectedYear(ui.yearStartInput?.value ?? state.selectedYearStart);
  const normalizedStartYear = Math.min(startYear, endYear);

  syncYearRangeState(normalizedStartYear, endYear);
  scheduleYearRangeAutoRefresh();
});

ui.fixAllSafeDirButton?.addEventListener("click", async () => {
  await runSafeDirectoryFix(state.safeDirectoryPaths, "all");
});

// ── Bootstrap ─────────────────────────────────────────────────────────────────

async function bootstrap() {
  if (!tauriInvoke) {
    setStatus("App running in browser mode. Launch via Tauri to scan local data.", "error");
    return;
  }

  let username = "";
  let rootPath = "";
  let hasSavedPreferences = false;

  try {
    const savedPreferences = await tauriInvoke("load_preferences");
    if (isValidPreference(savedPreferences)) {
      username = savedPreferences.username.trim();
      rootPath = savedPreferences.rootPath.trim();
      state.autoSave = savedPreferences.autoSave !== false;
      state.maxDepth = normalizeDepthValue(savedPreferences.maxDepth, DEFAULT_MAX_DEPTH);
      const savedRange = normalizeYearRange(
        savedPreferences.selectedYear,
        savedPreferences.selectedYearEnd ?? savedPreferences.selectedYear
      );
      syncYearRangeState(savedRange.startYear, savedRange.endYear);
      hasSavedPreferences = true;
    }
  } catch (error) {
    setStatus(`Saved preferences could not be read, using defaults: ${readError(error)}`, "idle");
  }

  try {
    const [usernameResult, pathResult] = await Promise.allSettled([
      tauriInvoke("default_username"),
      tauriInvoke("default_path")
    ]);

    if (!username && usernameResult.status === "fulfilled" && typeof usernameResult.value === "string") {
      const defaultUsername = usernameResult.value.trim();
      if (defaultUsername.length > 0) {
        username = defaultUsername;
      }
    }

    if (!rootPath && pathResult.status === "fulfilled" && typeof pathResult.value === "string") {
      const defaultPath = pathResult.value.trim();
      if (defaultPath.length > 0) {
        rootPath = defaultPath;
      }
    }
  } catch (_) {
    // Promise.allSettled never throws, this branch is defensive only.
  }

  if (username) {
    ui.username.value = username;
  }

  if (rootPath) {
    ui.rootPath.value = rootPath;
  }

  ui.autoSaveToggle.checked = state.autoSave;

  if (ui.maxDepthInput) {
    ui.maxDepthInput.value = state.maxDepth;
  }

  populateYearOptions(ui.yearStartInput, state.selectedYearStart);
  populateYearOptions(ui.yearEndInput, state.selectedYearEnd);

  if (!username || !rootPath) {
    setStatus("Enter your username and at least one path, then click Scan Statistics.", "idle");
    setCacheInfo(`No cache available. TTL is ${state.cacheTtlMinutes} minutes.`);
    return;
  }

  const cacheState = await tryRenderCachedStatistics(username, rootPath);
  if (cacheState.usedCache) {
    if (cacheState.isStale) {
      void runScan({
        username,
        rootPath,
        selectedYearStart: state.selectedYearStart,
        selectedYearEnd: state.selectedYearEnd,
        startupMode: false,
        persistOnSuccess: state.autoSave,
        isRefresh: true,
        isAutoRefresh: true
      });
      return;
    }

    const sourceLabel = hasSavedPreferences ? "saved preferences" : "default";
    setStatus(
      `Statistics loaded from cache (${cacheState.ageInfo.relativeText}) using ${sourceLabel}.`,
      "ok"
    );
    return;
  }

  await runScan({
    username,
    rootPath,
    selectedYearStart: state.selectedYearStart,
    selectedYearEnd: state.selectedYearEnd,
    startupMode: true,
    persistOnSuccess: state.autoSave,
    isRefresh: false
  });
}

async function tryRenderCachedStatistics(username, rootPath) {
  if (!tauriInvoke) {
    return { usedCache: false, isStale: false, ageInfo: null };
  }

  try {
    const cached = await tauriInvoke("load_cached_scan", {
      args: {
        username,
        rootPath,
        maxDepth: state.maxDepth,
        selectedYear: state.selectedYearStart,
        selectedYearEnd: state.selectedYearEnd
      }
    });

    if (!isValidCachedPayload(cached)) {
      setCacheInfo(`No cache available. TTL is ${state.cacheTtlMinutes} minutes.`);
      return { usedCache: false, isStale: false, ageInfo: null };
    }

    renderSummary(cached.data);
    renderHeatmap(cached.data);
    renderErrors(cached.data.repoErrors ?? []);

    const ageInfo = getCacheAgeInfo(cached.scannedAt);
    const isStale = ageInfo.ageMs >= state.cacheTtlMs;
    setCacheInfo(buildCacheInfoMessage(cached.scannedAt, ageInfo, isStale));

    if (isStale) {
      setStatus(
        `Cache ${ageInfo.relativeText} detected, running automatic refresh...`,
        "loading"
      );
    }

    return { usedCache: true, isStale, ageInfo };
  } catch (error) {
    setCacheInfo("Cache unavailable.");
    setStatus(`Failed to read cache, proceeding with new scan: ${readError(error)}`, "idle");
    return { usedCache: false, isStale: false, ageInfo: null };
  }
}

async function scanFromInputs({ startupMode = false, persistOnSuccess = true, isRefresh = false } = {}) {
  const username = ui.username.value.trim();
  const rootPath = ui.rootPath.value.trim();
  const selectedRange = readYearRangeFromInputs();
  syncYearRangeState(selectedRange.startYear, selectedRange.endYear);

  return runScan({
    username,
    rootPath,
    selectedYearStart: selectedRange.startYear,
    selectedYearEnd: selectedRange.endYear,
    startupMode,
    persistOnSuccess,
    isRefresh
  });
}

async function runScan({
  username,
  rootPath,
  selectedYearStart,
  selectedYearEnd,
  startupMode = false,
  persistOnSuccess = true,
  isRefresh = false,
  isAutoRefresh = false
}) {
  if (!username) {
    setStatus("Username is required", "error");
    return false;
  }

  if (!rootPath) {
    setStatus("Path is required (separate multiple paths with ';')", "error");
    return false;
  }

  const resolvedRange = normalizeYearRange(
    selectedYearStart ?? state.selectedYearStart,
    selectedYearEnd ?? state.selectedYearEnd
  );
  syncYearRangeState(resolvedRange.startYear, resolvedRange.endYear);

  if (yearRangeRefreshTimer) {
    clearTimeout(yearRangeRefreshTimer);
    yearRangeRefreshTimer = null;
  }

  if (!tauriInvoke) {
    setStatus("Tauri API unavailable. Run the application via cargo run.", "error");
    return false;
  }

  setBusy(true);
  setScanActive(true);
  setStatus(
    startupMode
      ? "Loading initial statistics..."
      : isAutoRefresh
        ? `Cache TTL exceeded (${state.cacheTtlMinutes} mins), running automatic refresh...`
      : isRefresh
        ? "Refreshing statistics..."
      : "Scanning local repositories and calculating contributions...",
    "loading"
  );

  try {
    const result = await tauriInvoke("scan_contributions", {
      args: {
        username,
        rootPath,
        maxDepth: state.maxDepth,
        selectedYear: resolvedRange.startYear,
        selectedYearEnd: resolvedRange.endYear
      }
    });

    renderSummary(result);
    renderHeatmap(result);
    const scanErrorSummary = renderErrors(result.repoErrors ?? []);

    const refreshedAt = new Date().toISOString();
    const refreshedAge = getCacheAgeInfo(refreshedAt);
    setCacheInfo(buildCacheInfoMessage(refreshedAt, refreshedAge, false));

    let saveWarning = "";
    if (persistOnSuccess) {
      const persisted = await persistPreferences(
        username,
        rootPath,
        state.autoSave,
        state.maxDepth,
        resolvedRange.startYear,
        resolvedRange.endYear
      );
      if (persisted.saved) {
        ui.username.value = persisted.saved.username;
        ui.rootPath.value = persisted.saved.rootPath;
        ui.autoSaveToggle.checked = persisted.saved.autoSave !== false;
        state.autoSave = ui.autoSaveToggle.checked;
        state.maxDepth = persisted.saved.maxDepth ?? state.maxDepth;
        const persistedRange = normalizeYearRange(
          persisted.saved.selectedYear,
          persisted.saved.selectedYearEnd ?? persisted.saved.selectedYear
        );
        syncYearRangeState(persistedRange.startYear, persistedRange.endYear);
        if (ui.maxDepthInput) {
          ui.maxDepthInput.value = state.maxDepth;
        }
        populateYearOptions(ui.yearStartInput, state.selectedYearStart);
        populateYearOptions(ui.yearEndInput, state.selectedYearEnd);
      }
      if (persisted.error) {
        saveWarning = ` Preferences not saved (${persisted.error}).`;
      }
    }

    const scanWarning = buildScanWarning(scanErrorSummary);

    const completionMessage = startupMode
      ? `Initial statistics loaded successfully.${saveWarning}${scanWarning}`
      : isAutoRefresh
        ? `Auto-refresh complete: ${result.totalContributions} contributions from ${result.reposScanned} repositories.${saveWarning}${scanWarning}`
      : isRefresh
        ? `Refresh complete: ${result.totalContributions} contributions from ${result.reposScanned} repositories.${saveWarning}${scanWarning}`
      : `Scan complete: ${result.totalContributions} contributions from ${result.reposScanned} repositories.${saveWarning}${scanWarning}`;

    setStatus(completionMessage, "ok");
    return true;
  } catch (error) {
    const msg = readError(error);
    const wasCancelled = msg.toLowerCase().includes("cancelled") || msg.toLowerCase().includes("cancelled");

    const errorMessage = wasCancelled
      ? "Scan cancelled."
      : isAutoRefresh
        ? `Auto-refresh failed: ${msg}. Displaying latest cached data.`
      : `Scan failed: ${msg}`;

    setStatus(errorMessage, wasCancelled ? "idle" : "error");

    if (!isAutoRefresh && !wasCancelled) {
      ui.panel.classList.add("hidden");
      ui.summary.classList.add("hidden");
      ui.analyticsPanel?.classList.add("hidden");
      renderErrors([]);
    }

    return false;
  } finally {
    setBusy(false);
    setScanActive(false);
  }
}

async function persistCurrentPreferences() {
  const username = ui.username.value.trim();
  const rootPath = ui.rootPath.value.trim();
  const selectedRange = readYearRangeFromInputs();
  syncYearRangeState(selectedRange.startYear, selectedRange.endYear);

  if (!username) {
    return { saved: null, error: "Username is required" };
  }

  if (!rootPath) {
    return { saved: null, error: "Path is required (separate multiple paths with ';')" };
  }

  return persistPreferences(
    username,
    rootPath,
    state.autoSave,
    state.maxDepth,
    selectedRange.startYear,
    selectedRange.endYear
  );
}

async function persistPreferences(
  username,
  rootPath,
  autoSave,
  maxDepth,
  selectedYearStart,
  selectedYearEnd
) {
  if (!tauriInvoke) {
    return { saved: null, error: null };
  }

  const normalizedRange = normalizeYearRange(selectedYearStart, selectedYearEnd);

  try {
    const saved = await tauriInvoke("save_preferences", {
      args: {
        username,
        rootPath,
        autoSave,
        maxDepth: maxDepth ?? DEFAULT_MAX_DEPTH,
        selectedYear: normalizedRange.startYear,
        selectedYearEnd: normalizedRange.endYear
      }
    });

    if (isValidPreference(saved)) {
      return { saved, error: null };
    }

    return { saved: null, error: null };
  } catch (error) {
    return { saved: null, error: readError(error) };
  }
}

// ── Busy state helpers ────────────────────────────────────────────────────────

function setBusy(isBusy) {
  ui.username.disabled = isBusy;
  ui.rootPath.disabled = isBusy;
  ui.scanButton.disabled = isBusy;
  ui.refreshButton.disabled = isBusy;
  ui.pickFolderButton.disabled = isBusy;
  ui.currentButton.disabled = isBusy;
  ui.savePrefButton.disabled = isBusy;
  ui.autoSaveToggle.disabled = isBusy;
  if (ui.yearStartTrigger) {
    ui.yearStartTrigger.disabled = isBusy;
  }
  if (ui.yearEndTrigger) {
    ui.yearEndTrigger.disabled = isBusy;
  }
  if (ui.yearStartInput) {
    ui.yearStartInput.disabled = isBusy;
  }
  if (ui.yearEndInput) {
    ui.yearEndInput.disabled = isBusy;
  }

  if (isBusy) {
    closeAllYearDropdowns();
  }

  if (ui.maxDepthInput) {
    ui.maxDepthInput.disabled = isBusy;
  }

  if (ui.depthDecreaseButton) {
    ui.depthDecreaseButton.disabled = isBusy;
  }

  if (ui.depthIncreaseButton) {
    ui.depthIncreaseButton.disabled = isBusy;
  }

  if (ui.fixAllSafeDirButton) {
    ui.fixAllSafeDirButton.disabled = isBusy || state.safeDirectoryPaths.length === 0;
  }

  const fixButtons = ui.errorsList?.querySelectorAll(".error-fix-btn") ?? [];
  for (const button of fixButtons) {
    button.disabled = isBusy;
  }
}

function setScanActive(isActive) {
  state.scanning = isActive;

  if (!ui.cancelButton) return;

  if (isActive) {
    ui.cancelButton.classList.remove("!hidden", "hidden");
    ui.scanButton.classList.add("hidden");
  } else {
    ui.cancelButton.classList.add("hidden");
    ui.scanButton.classList.remove("!hidden", "hidden");
  }
}

// ── Validation helpers ────────────────────────────────────────────────────────

function isValidPreference(candidate) {
  return (
    candidate &&
    typeof candidate === "object" &&
    typeof candidate.username === "string" &&
    candidate.username.trim().length > 0 &&
    typeof candidate.rootPath === "string" &&
    candidate.rootPath.trim().length > 0 &&
    typeof candidate.autoSave === "boolean" &&
    (candidate.selectedYear === undefined || Number.isFinite(candidate.selectedYear)) &&
    (candidate.selectedYearEnd === undefined || Number.isFinite(candidate.selectedYearEnd))
  );
}

function isValidCachedPayload(candidate) {
  return (
    candidate &&
    typeof candidate === "object" &&
    typeof candidate.scannedAt === "string" &&
    candidate.scannedAt.trim().length > 0 &&
    candidate.data &&
    typeof candidate.data === "object" &&
    typeof candidate.data.username === "string" &&
    typeof candidate.data.rootPath === "string"
  );
}

// ── Rendering ─────────────────────────────────────────────────────────────────

function renderSummary(result) {
  ui.summary.classList.remove("!hidden", "hidden");
  ui.panel.classList.remove("!hidden", "hidden");

  ui.summaryTotal.textContent = formatNumber(result.totalContributions);
  ui.summaryRepos.textContent = formatNumber(result.reposScanned);
  ui.summaryUser.textContent = result.username;
  ui.summaryPath.textContent = result.rootPath;

  const selectedRange = normalizeYearRange(
    result.selectedYearStart ?? result.selectedYear,
    result.selectedYearEnd ?? result.selectedYearStart ?? result.selectedYear
  );
  syncYearRangeState(selectedRange.startYear, selectedRange.endYear);

  ui.title.textContent =
    `${formatNumber(result.totalContributions)} contributions in ${formatYearRangeLabel(selectedRange.startYear, selectedRange.endYear)}`;
  ui.period.textContent = `Period: ${prettyDate(result.startDate)} - ${prettyDate(result.endDate)}`;
  renderAnalytics(result);
}

function renderAnalytics(result) {
  if (!ui.analyticsPanel) {
    return;
  }

  ui.analyticsPanel.classList.remove("!hidden", "hidden");

  renderTopRepositories(result.topRepositories ?? []);
  renderStreakMetrics(result.streakMetrics ?? {});
  renderActivityDays(result.activityDays ?? {});
  renderPeakProductivity(result.peakProductivity ?? {});
  renderWeeklyTrend(result.weeklyTrend ?? {});
  renderDayDistribution(result.dayDistribution ?? {});
}

function renderTopRepositories(topRepositories) {
  if (!ui.topRepoList) {
    return;
  }

  ui.topRepoList.innerHTML = "";
  const rows = Array.isArray(topRepositories) ? topRepositories.slice(0, 5) : [];

  if (rows.length === 0) {
    const empty = document.createElement("li");
    empty.className = "analytics-empty";
    empty.textContent = "No repository commit data available for this period.";
    ui.topRepoList.appendChild(empty);
    return;
  }

  const maxCommitCount = Math.max(
    1,
    ...rows.map((repository) => toSafeNumber(repository?.commitCount))
  );

  for (const [index, repository] of rows.entries()) {
    const repoPath = String(repository?.repoPath ?? "-").trim();
    const repoLabel = shortenRepoPath(repoPath);
    const commitCount = toSafeNumber(repository?.commitCount);
    const percentage = toSafeNumber(repository?.percentageOfTotal);
    const relativeWidth = clamp((commitCount / maxCommitCount) * 100, 0, 100);

    const item = document.createElement("li");
    item.className = "repo-item";

    const header = document.createElement("div");
    header.className = "repo-item-header";

    const rank = document.createElement("span");
    rank.className = "analytics-rank";
    rank.textContent = `#${index + 1}`;

    const titleWrap = document.createElement("div");
    titleWrap.className = "repo-label-wrap";

    const path = document.createElement("span");
    path.className = "analytics-repo-name";
    path.textContent = repoLabel;
    path.title = repoPath;

    const commit = document.createElement("span");
    commit.className = "repo-commit-count";
    commit.textContent = `${formatNumber(commitCount)} commits`;

    titleWrap.append(path, commit);
    header.append(rank, titleWrap);

    const track = document.createElement("div");
    track.className = "repo-bar-track";

    const fill = document.createElement("span");
    fill.className = "repo-bar-fill";
    fill.style.width = `${Math.max(8, relativeWidth)}%`;
    track.appendChild(fill);

    const meta = document.createElement("span");
    meta.className = "repo-percentage";
    meta.textContent = `${formatPercentage(percentage)} of total contributions`;

    item.append(header, track, meta);
    ui.topRepoList.appendChild(item);
  }
}

function renderStreakMetrics(streakMetrics) {
  if (ui.currentStreak) {
    ui.currentStreak.textContent = formatNumber(toSafeNumber(streakMetrics.currentStreak));
  }

  if (ui.longestStreak) {
    ui.longestStreak.textContent = formatNumber(toSafeNumber(streakMetrics.longestStreak));
  }
}

function renderActivityDays(activityDays) {
  const activeDays = toSafeNumber(activityDays.activeDays);
  const inactiveDays = toSafeNumber(activityDays.inactiveDays);
  const totalDays = toSafeNumber(activityDays.totalDays);
  const activeRatio = totalDays > 0 ? (activeDays / totalDays) * 100 : 0;

  if (ui.activeDays) {
    ui.activeDays.textContent = formatNumber(activeDays);
  }

  if (ui.inactiveDays) {
    ui.inactiveDays.textContent = formatNumber(inactiveDays);
  }

  if (ui.activeDaysRatio) {
    ui.activeDaysRatio.textContent = `${formatPercentage(activeRatio)} active days`;
  }

  if (ui.activityDaysBar) {
    ui.activityDaysBar.style.width = `${clamp(activeRatio, 0, 100)}%`;
  }
}

function renderPeakProductivity(peakProductivity) {
  if (ui.peakDay) {
    if (peakProductivity.bestDay) {
      const count = toSafeNumber(peakProductivity.bestDay.count);
      ui.peakDay.textContent = `${prettyDate(peakProductivity.bestDay.date)} (${formatNumber(count)} commits)`;
    } else {
      ui.peakDay.textContent = "No peak day yet";
    }
  }

  if (ui.peakWeek) {
    if (peakProductivity.bestWeek) {
      const count = toSafeNumber(peakProductivity.bestWeek.count);
      const isoWeek = toSafeNumber(peakProductivity.bestWeek.isoWeek);
      const isoYear = toSafeNumber(peakProductivity.bestWeek.isoYear);
      const start = prettyDate(peakProductivity.bestWeek.startDate);
      const end = prettyDate(peakProductivity.bestWeek.endDate);
      ui.peakWeek.textContent = `W${isoWeek} ${isoYear} (${formatNumber(count)} commits, ${start} - ${end})`;
    } else {
      ui.peakWeek.textContent = "No peak week yet";
    }
  }

  if (ui.peakMonth) {
    if (peakProductivity.bestMonth) {
      const count = toSafeNumber(peakProductivity.bestMonth.count);
      const monthIndex = clamp(toSafeNumber(peakProductivity.bestMonth.month) - 1, 0, 11);
      const year = toSafeNumber(peakProductivity.bestMonth.year);
      ui.peakMonth.textContent = `${monthLabel[monthIndex]} ${year} (${formatNumber(count)} commits)`;
    } else {
      ui.peakMonth.textContent = "No peak month yet";
    }
  }
}

function renderWeeklyTrend(weeklyTrend) {
  const growth = weeklyTrend?.weekOverWeekGrowth;

  if (ui.wowDelta) {
    if (growth) {
      const delta = toSafeNumber(growth.delta);
      const deltaPrefix = delta > 0 ? "+" : "";
      ui.wowDelta.textContent = `${deltaPrefix}${formatNumber(delta)} commits`;
      ui.wowDelta.classList.remove("text-emerald-400", "text-rose-400", "text-gray-200");
      if (delta > 0) {
        ui.wowDelta.classList.add("text-emerald-400");
      } else if (delta < 0) {
        ui.wowDelta.classList.add("text-rose-400");
      } else {
        ui.wowDelta.classList.add("text-gray-200");
      }
    } else {
      ui.wowDelta.textContent = "No WoW data yet";
      ui.wowDelta.classList.remove("text-emerald-400", "text-rose-400");
      ui.wowDelta.classList.add("text-gray-200");
    }
  }

  if (ui.wowDetails) {
    if (growth) {
      const currentCount = toSafeNumber(growth.currentWeekCount);
      const previousCount = toSafeNumber(growth.previousWeekCount);
      const percent = typeof growth.percentChange === "number"
        ? `${formatSignedPercentage(growth.percentChange)} vs previous week`
        : "vs previous week (previous week was zero)";

      ui.wowDetails.textContent =
        `Current: ${formatNumber(currentCount)}, Previous: ${formatNumber(previousCount)} (${percent})`;
    } else {
      ui.wowDetails.textContent = "Need at least two weeks of data to compute week-over-week trend.";
    }
  }

  const weeklyBars = Array.isArray(weeklyTrend?.weeklyTotals)
    ? weeklyTrend.weeklyTotals.slice(-12).map((week) => ({
      value: toSafeNumber(week.count),
      label: `${week.isoYear}-W${week.isoWeek}`,
      title: `${week.isoYear}-W${week.isoWeek}: ${formatNumber(toSafeNumber(week.count))} commits`
    }))
    : [];
  renderLineChart(ui.weeklyBars, weeklyBars, {
    lineColor: "#22d3ee",
    areaColor: "rgba(34, 211, 238, 0.18)",
    pointColor: "#67e8f9",
    valueLabelFormatter: (value) => formatNumber(Math.round(toSafeNumber(value))),
    latestLabel: "Latest"
  });

  const movingAveragePoints = Array.isArray(weeklyTrend?.movingAveragePoints)
    ? weeklyTrend.movingAveragePoints.slice(-28).map((point) => ({
      value: toSafeNumber(point.value),
      label: point.date,
      title: `${prettyDate(point.date)}: ${formatFloat(point.value)} commits/day (7d avg)`
    }))
    : [];
  renderLineChart(ui.movingAverageBars, movingAveragePoints, {
    lineColor: "#60a5fa",
    areaColor: "rgba(96, 165, 250, 0.18)",
    pointColor: "#bfdbfe",
    valueLabelFormatter: (value) => formatFloat(value),
    latestLabel: "Latest"
  });

  if (ui.movingAverageLatest) {
    const latestPoint = movingAveragePoints[movingAveragePoints.length - 1];
    if (latestPoint) {
      ui.movingAverageLatest.textContent =
        `Latest 7-day moving average: ${formatFloat(latestPoint.value)} commits/day`;
    } else {
      ui.movingAverageLatest.textContent = "Latest 7-day moving average: n/a";
    }
  }
}

function renderLineChart(container, rows, options = {}) {
  if (!container) {
    return;
  }

  container.innerHTML = "";

  if (!rows || rows.length === 0) {
    const empty = document.createElement("span");
    empty.className = "analytics-empty";
    empty.textContent = "No data";
    container.appendChild(empty);
    return;
  }

  const values = rows.map((row) => toSafeNumber(row.value));
  const maxValue = Math.max(...values);
  const minValue = Math.min(...values);
  const valueRange = maxValue - minValue;

  const valueLabelFormatter = typeof options.valueLabelFormatter === "function"
    ? options.valueLabelFormatter
    : (value) => formatFloat(value);

  const lineColor = options.lineColor ?? "#22d3ee";
  const areaColor = options.areaColor ?? "rgba(34, 211, 238, 0.18)";
  const pointColor = options.pointColor ?? "#67e8f9";
  const latestLabel = options.latestLabel ?? "Latest";

  const SVG_NS = "http://www.w3.org/2000/svg";
  const width = 700;
  const height = 170;
  const padding = {
    top: 12,
    right: 12,
    bottom: 22,
    left: 12
  };

  const chartWidth = width - padding.left - padding.right;
  const chartHeight = height - padding.top - padding.bottom;
  const baselineY = padding.top + chartHeight;

  const points = values.map((value, index) => {
    const normalized = valueRange > 0 ? (value - minValue) / valueRange : 0.5;
    const x = values.length > 1
      ? padding.left + (index * chartWidth) / (values.length - 1)
      : padding.left + chartWidth / 2;
    const y = padding.top + (1 - normalized) * chartHeight;
    return {
      x,
      y,
      value,
      label: rows[index].label,
      title: rows[index].title
    };
  });

  const pathD = points
    .map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(2)},${point.y.toFixed(2)}`)
    .join(" ");

  const areaD = `${pathD} L${points[points.length - 1].x.toFixed(2)},${baselineY.toFixed(2)} ` +
    `L${points[0].x.toFixed(2)},${baselineY.toFixed(2)} Z`;

  const svg = document.createElementNS(SVG_NS, "svg");
  svg.setAttribute("class", "trend-svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");

  for (let index = 0; index <= 4; index += 1) {
    const y = padding.top + (chartHeight * index) / 4;
    const line = document.createElementNS(SVG_NS, "line");
    line.setAttribute("x1", `${padding.left}`);
    line.setAttribute("x2", `${width - padding.right}`);
    line.setAttribute("y1", `${y}`);
    line.setAttribute("y2", `${y}`);
    line.setAttribute("class", "trend-grid-line");
    svg.appendChild(line);
  }

  const area = document.createElementNS(SVG_NS, "path");
  area.setAttribute("d", areaD);
  area.setAttribute("class", "trend-area");
  area.setAttribute("fill", areaColor);
  svg.appendChild(area);

  const path = document.createElementNS(SVG_NS, "path");
  path.setAttribute("d", pathD);
  path.setAttribute("class", "trend-line");
  path.setAttribute("stroke", lineColor);
  svg.appendChild(path);

  for (const point of points) {
    const circle = document.createElementNS(SVG_NS, "circle");
    circle.setAttribute("cx", `${point.x}`);
    circle.setAttribute("cy", `${point.y}`);
    circle.setAttribute("r", values.length > 20 ? "2.2" : "2.9");
    circle.setAttribute("class", "trend-point");
    circle.setAttribute("fill", pointColor);

    const title = document.createElementNS(SVG_NS, "title");
    title.textContent = point.title ?? `${point.label}: ${valueLabelFormatter(point.value)}`;
    circle.appendChild(title);
    svg.appendChild(circle);
  }

  container.appendChild(svg);

  const meta = document.createElement("div");
  meta.className = "trend-chart-meta";

  const minText = document.createElement("span");
  minText.textContent = `Min ${valueLabelFormatter(minValue)}`;

  const maxText = document.createElement("span");
  maxText.textContent = `Max ${valueLabelFormatter(maxValue)}`;

  const latestText = document.createElement("span");
  latestText.textContent = `${latestLabel} ${valueLabelFormatter(values[values.length - 1])}`;

  meta.append(minText, maxText, latestText);
  container.appendChild(meta);
}

function renderDayDistribution(dayDistribution) {
  if (!ui.weekdayDistribution) {
    return;
  }

  const weekdayRows = [
    { label: "Mon", value: toSafeNumber(dayDistribution.monday) },
    { label: "Tue", value: toSafeNumber(dayDistribution.tuesday) },
    { label: "Wed", value: toSafeNumber(dayDistribution.wednesday) },
    { label: "Thu", value: toSafeNumber(dayDistribution.thursday) },
    { label: "Fri", value: toSafeNumber(dayDistribution.friday) },
    { label: "Sat", value: toSafeNumber(dayDistribution.saturday) },
    { label: "Sun", value: toSafeNumber(dayDistribution.sunday) }
  ];

  const maxValue = Math.max(1, ...weekdayRows.map((row) => row.value));
  ui.weekdayDistribution.innerHTML = "";

  for (const row of weekdayRows) {
    const line = document.createElement("div");
    line.className = "weekday-row";

    const day = document.createElement("span");
    day.className = "weekday-label";
    day.textContent = row.label;

    const track = document.createElement("span");
    track.className = "weekday-track";

    const fill = document.createElement("span");
    fill.className = "weekday-fill";
    fill.style.width = `${Math.max(2, (row.value / maxValue) * 100)}%`;
    track.appendChild(fill);

    const value = document.createElement("span");
    value.className = "weekday-value";
    value.textContent = formatNumber(row.value);

    line.append(day, track, value);
    ui.weekdayDistribution.appendChild(line);
  }

  if (ui.weekendRatio) {
    const weekendRatio = clamp(toSafeNumber(dayDistribution.weekendRatio), 0, 1) * 100;
    ui.weekendRatio.textContent = `${formatPercentage(weekendRatio)} of commits happen on weekends`;
  }
}

function shortenRepoPath(repoPath) {
  if (!repoPath) {
    return "-";
  }

  const normalized = String(repoPath).replace(/\\+/g, "/").replace(/\/+$/, "");
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length <= 2) {
    return normalized;
  }

  return parts.slice(-2).join("/");
}

function renderErrors(errors) {
  if (ui.errorAlert) {
    ui.errorAlert.textContent = "";
    ui.errorAlert.classList.add("hidden");
  }

  ui.errorsList.innerHTML = "";
  updateSafeDirectoryActions([]);

  if (!errors || errors.length === 0) {
    ui.errorsPanel.classList.add("hidden");
    return { total: 0, ownershipCount: 0, fixableCount: 0 };
  }

  let ownershipCount = 0;
  const fixablePaths = [];

  for (const rawItem of errors) {
    const item = toFriendlyRepoError(rawItem);
    if (item.type === "ownership") {
      ownershipCount += 1;
    }

    const li = document.createElement("li");
    li.textContent = item.message;

    if (item.command) {
      const actionRow = document.createElement("div");
      actionRow.className = "error-action-row";

      const command = document.createElement("div");
      command.className = "error-command";
      command.textContent = item.command;
      actionRow.appendChild(command);

      if (item.safeDirectoryPath) {
        fixablePaths.push(item.safeDirectoryPath);

        const fixButton = document.createElement("button");
        fixButton.type = "button";
        fixButton.className = "error-fix-btn";
        fixButton.textContent = "Run Fix";
        fixButton.addEventListener("click", async () => {
          await runSafeDirectoryFix([item.safeDirectoryPath], "single");
        });
        actionRow.appendChild(fixButton);
      }

      li.appendChild(actionRow);
    }

    ui.errorsList.appendChild(li);
  }

  updateSafeDirectoryActions(fixablePaths);

  if (ownershipCount > 0 && ui.errorAlert) {
    const actionHint = fixablePaths.length > 0
      ? "Gunakan One Click Fix atau tombol Run Fix per repository."
      : "Add safe.directory for the affected repositories, then scan again.";

    ui.errorAlert.textContent =
      `${ownershipCount} repositories skipped due to ownership mismatch. ` +
      actionHint;
    ui.errorAlert.classList.remove("!hidden", "hidden");
  }

  ui.errorsPanel.classList.remove("!hidden", "hidden");

  return { total: errors.length, ownershipCount, fixableCount: fixablePaths.length };
}

function updateSafeDirectoryActions(paths) {
  state.safeDirectoryPaths = uniqueNonEmptyPaths(paths);

  if (!ui.fixAllSafeDirButton) {
    return;
  }

  const hasFixablePaths = state.safeDirectoryPaths.length > 0;
  ui.fixAllSafeDirButton.classList.toggle("hidden", !hasFixablePaths);
  ui.fixAllSafeDirButton.disabled = !hasFixablePaths;
}

function uniqueNonEmptyPaths(paths) {
  const uniquePaths = new Set();

  for (const path of paths ?? []) {
    const normalizedPath = String(path ?? "").trim().replace(/\\/g, "/");
    if (normalizedPath.length > 0) {
      uniquePaths.add(normalizedPath);
    }
  }

  return [...uniquePaths];
}

// ── Safe directory fix ────────────────────────────────────────────────────────

async function runSafeDirectoryFix(paths, mode) {
  const targetPaths = uniqueNonEmptyPaths(paths);

  if (targetPaths.length === 0) {
    setStatus("No safe.directory commands available to run.", "idle");
    return false;
  }

  if (!tauriInvoke) {
    setStatus("The safe.directory fix feature is only available when running within Tauri.", "error");
    return false;
  }

  setSafeDirectoryFixBusy(true);

  const loadingMessage =
    mode === "all"
      ? `Running One Click Fix for ${targetPaths.length} repositories...`
      : "Running safe.directory fix...";
  setStatus(loadingMessage, "loading");

  try {
    const result = await tauriInvoke("add_safe_directories", { paths: targetPaths });
    const attempted = Number(result?.attempted ?? targetPaths.length);
    const applied = Number(result?.applied ?? 0);
    const failed = Array.isArray(result?.failed) ? result.failed : [];

    if (failed.length === 0) {
      if (mode === "all") {
        setStatus(
          `One Click Fix successful for ${applied} repositories. Running automatic scan refresh...`,
          "loading"
        );

        return scanFromInputs({
          startupMode: false,
          persistOnSuccess: state.autoSave,
          isRefresh: true
        });
      }

      const successMessage =
        attempted > 1
          ? `safe.directory fix successful for ${applied} repositories.`
          : "safe.directory fix successful.";
      setStatus(successMessage, "ok");
      return true;
    }

    const preview = failed.slice(0, 2).join(" | ");
    setStatus(
      `Fix completed with partial failures (${failed.length}/${attempted}). ${preview}`,
      "error"
    );
    return false;
  } catch (error) {
    setStatus(`Failed to run safe.directory fix: ${readError(error)}`, "error");
    return false;
  } finally {
    setSafeDirectoryFixBusy(false);
  }
}

function setSafeDirectoryFixBusy(isBusy) {
  if (ui.fixAllSafeDirButton) {
    ui.fixAllSafeDirButton.disabled = isBusy || state.safeDirectoryPaths.length === 0;
  }

  const buttons = ui.errorsList?.querySelectorAll(".error-fix-btn") ?? [];
  for (const button of buttons) {
    button.disabled = isBusy;
  }
}

// ── Error message parsing ─────────────────────────────────────────────────────

function toFriendlyRepoError(rawMessage) {
  const message = collapseWhitespace(String(rawMessage ?? ""));
  const repository = extractRepositoryFromError(message);
  const lowerMessage = message.toLowerCase();
  const safeDirectoryFix = extractSafeDirectoryFix(message, repository);

  const isOwnershipIssue =
    lowerMessage.includes("detected dubious ownership") ||
    lowerMessage.includes("safe.directory") ||
    lowerMessage.includes("dubious ownership") ||
    lowerMessage.includes("dubious ownership");

  if (isOwnershipIssue) {
    const repoLabel = repository ?? "this repository";
    return {
      type: "ownership",
      message: `Repository ${repoLabel} skipped due to dubious ownership or mismatched folder permissions.`,
      command: safeDirectoryFix?.command ?? null,
      safeDirectoryPath: safeDirectoryFix?.path ?? null
    };
  }

  const isTimeout = lowerMessage.includes("timeout");
  if (isTimeout) {
    const repoLabel = repository ?? "a repository";
    return {
      type: "timeout",
      message: `Repository ${repoLabel} exceeded scan timeout (30 seconds). The repository might be too large or located on a network drive.`,
      command: null,
      safeDirectoryPath: null
    };
  }

  const fallback =
    message.length > 220 ? `${message.slice(0, 217)}...` : message;

  return {
    type: "generic",
    message: fallback || "Repository failed to scan due to an unknown error.",
    command: safeDirectoryFix?.command ?? null,
    safeDirectoryPath: safeDirectoryFix?.path ?? null
  };
}

function extractSafeDirectoryFix(message, fallbackRepository) {
  if (!message.toLowerCase().includes(SAFE_DIRECTORY_COMMAND)) {
    return null;
  }

  const quotedMatch = message.match(/git config --global --add safe\.directory\s+"([^"]+)"/i);
  const singleQuotedMatch = message.match(/git config --global --add safe\.directory\s+'([^']+)'/i);
  const unquotedMatch = message.match(/git config --global --add safe\.directory\s+([^\s]+)/i);

  const pathFromCommand = quotedMatch?.[1] ?? singleQuotedMatch?.[1] ?? unquotedMatch?.[1] ?? "";
  const preferRepositoryPath = !quotedMatch && !singleQuotedMatch && Boolean(fallbackRepository);
  const selectedPath = (preferRepositoryPath ? fallbackRepository : pathFromCommand || fallbackRepository || "").trim();

  if (!selectedPath) {
    return null;
  }

  const normalizedPath = selectedPath.replace(/\\/g, "/");
  return {
    command: `${SAFE_DIRECTORY_COMMAND} "${normalizedPath}"`,
    path: normalizedPath
  };
}

function extractRepositoryFromError(message) {
  const prefix = "Failed to read ";
  if (!message.startsWith(prefix)) {
    return null;
  }

  const separatorIndex = message.indexOf(": ");
  if (separatorIndex <= prefix.length) {
    return null;
  }

  return message.slice(prefix.length, separatorIndex).trim();
}

function collapseWhitespace(value) {
  return value.replace(/\s+/g, " ").trim();
}

function buildScanWarning(summary) {
  if (!summary || summary.total === 0) {
    return "";
  }

  if (summary.fixableCount > 0) {
    return ` ${summary.fixableCount} repositories have a safe.directory fix button.`;
  }

  if (summary.ownershipCount > 0) {
    return ` ${summary.ownershipCount} repositories skipped due to dubious ownership.`;
  }

  return ` ${summary.total} repositories failed to scan.`;
}

// ── Heatmap rendering ─────────────────────────────────────────────────────────

function renderHeatmap(result) {
  const contributionMap = new Map();
  for (const item of result.contributions ?? []) {
    contributionMap.set(item.date, item.count);
  }

  const startDate = parseIsoDate(result.startDate);
  const endDate = parseIsoDate(result.endDate);
  const totalDays = dayDiff(startDate, endDate) + 1;
  const weekCount = Math.ceil(totalDays / 7);
  const maxDaily = Math.max(result.maxDailyCount || 0, 1);

  ui.heatmap.innerHTML = "";

  const monthRow = document.createElement("div");
  monthRow.className = "month-row";

  for (let week = 0; week < weekCount; week += 1) {
    const weekStart = addDay(startDate, week * 7);
    const previous = addDay(weekStart, -7);

    const labelCell = document.createElement("span");
    labelCell.className = "month-cell";
    if (week === 0 || weekStart.getMonth() !== previous.getMonth()) {
      labelCell.textContent = monthLabel[weekStart.getMonth()];
    } else {
      labelCell.textContent = "";
    }

    monthRow.appendChild(labelCell);
  }

  const mapBody = document.createElement("div");
  mapBody.className = "map-body";

  const labels = document.createElement("div");
  labels.className = "day-labels";
  for (let day = 0; day < 7; day += 1) {
    const label = document.createElement("span");
    label.textContent = day === 0 || day === 2 || day === 4 ? dayLabel[day] : "";
    labels.appendChild(label);
  }

  const weekGrid = document.createElement("div");
  weekGrid.className = "week-grid";

  for (let week = 0; week < weekCount; week += 1) {
    const column = document.createElement("div");
    column.className = "week-column";

    for (let day = 0; day < 7; day += 1) {
      const date = addDay(startDate, week * 7 + day);
      const key = formatIsoDate(date);
      const inRange = date <= endDate;
      const count = inRange ? contributionMap.get(key) ?? 0 : 0;

      const cell = document.createElement("span");
      cell.className = "day-cell";

      if (!inRange) {
        cell.classList.add("is-future");
        cell.style.backgroundColor = heatPalette.future;
      } else {
        cell.style.backgroundColor = colorByIntensity(count, maxDaily);
        cell.title = `${prettyDate(key)}: ${count} commit`;
      }

      column.appendChild(cell);
    }

    weekGrid.appendChild(column);
  }

  mapBody.append(labels, weekGrid);
  ui.heatmap.append(monthRow, mapBody);
}

// ── Status helpers ────────────────────────────────────────────────────────────

function setStatus(message, kind) {
  ui.status.textContent = message;
  ui.status.className = `status is-${kind}`;
  setLoadingIndicator(kind === "loading");
}

function setLoadingIndicator(isLoading) {
  if (!ui.statusLoader) {
    return;
  }

  ui.statusLoader.classList.toggle("is-visible", isLoading);
}

// ── Color / date utilities ────────────────────────────────────────────────────

function colorByIntensity(count, maxDaily) {
  if (count <= 0) {
    return heatPalette.level0;
  }

  const ratio = count / maxDaily;
  if (ratio < 0.25) {
    return heatPalette.level1;
  }
  if (ratio < 0.5) {
    return heatPalette.level2;
  }
  if (ratio < 0.75) {
    return heatPalette.level3;
  }
  return heatPalette.level4;
}

function parseIsoDate(value) {
  if (!value) {
    return new Date();
  }
  const [y, m, d] = value.split("-").map((n) => Number.parseInt(n, 10));
  return new Date(y, m - 1, d);
}

function formatIsoDate(date) {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function prettyDate(value) {
  const date = typeof value === "string" ? parseIsoDate(value) : value;
  return new Intl.DateTimeFormat("en-US", {
    day: "2-digit",
    month: "short",
    year: "numeric"
  }).format(date);
}

function addDay(base, offset) {
  const date = new Date(base);
  date.setDate(date.getDate() + offset);
  return date;
}

function dayDiff(start, end) {
  const oneDay = 24 * 60 * 60 * 1000;
  return Math.floor((end.getTime() - start.getTime()) / oneDay);
}

function readError(error) {
  if (!error) {
    return "Unknown error";
  }

  if (typeof error === "string") {
    return error;
  }

  if (typeof error === "object") {
    if (typeof error.message === "string") {
      return error.message;
    }

    try {
      return JSON.stringify(error);
    } catch (_) {
      return "Unknown error object";
    }
  }

  return String(error);
}

function formatNumber(value) {
  return new Intl.NumberFormat("en-US").format(value ?? 0);
}

function formatFloat(value) {
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 1,
    maximumFractionDigits: 2
  }).format(toSafeNumber(value));
}

function formatPercentage(value) {
  return `${formatFloat(toSafeNumber(value))}%`;
}

function formatSignedPercentage(value) {
  const numeric = toSafeNumber(value);
  const prefix = numeric > 0 ? "+" : "";
  return `${prefix}${formatFloat(numeric)}%`;
}

function toSafeNumber(value, fallback = 0) {
  const numeric = typeof value === "number" ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function sanitizeSelectedYear(value) {
  const numeric = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(numeric)) {
    return CURRENT_YEAR;
  }

  return clamp(numeric, MIN_SUPPORTED_YEAR, CURRENT_YEAR);
}

function resolveYearDropdownRole(target) {
  if (target === "start" || target === ui.yearStartInput || target === ui.yearStartDropdown) {
    return "start";
  }

  if (target === "end" || target === ui.yearEndInput || target === ui.yearEndDropdown) {
    return "end";
  }

  return null;
}

function getYearDropdownRefs(role) {
  if (role === "start") {
    return {
      dropdown: ui.yearStartDropdown,
      trigger: ui.yearStartTrigger,
      panel: ui.yearStartPanel,
      label: ui.yearStartLabel,
      input: ui.yearStartInput
    };
  }

  if (role === "end") {
    return {
      dropdown: ui.yearEndDropdown,
      trigger: ui.yearEndTrigger,
      panel: ui.yearEndPanel,
      label: ui.yearEndLabel,
      input: ui.yearEndInput
    };
  }

  return {
    dropdown: null,
    trigger: null,
    panel: null,
    label: null,
    input: null
  };
}

function initializeYearDropdowns() {
  bindYearDropdown("start");
  bindYearDropdown("end");

  document.addEventListener("click", handleYearDropdownOutsideClick);
  document.addEventListener("keydown", handleYearDropdownGlobalKeydown);
  window.addEventListener("resize", handleYearDropdownViewportChange);
  window.addEventListener("scroll", handleYearDropdownViewportChange, true);
}

function bindYearDropdown(role) {
  const refs = getYearDropdownRefs(role);
  if (!refs.trigger || !refs.panel) {
    return;
  }

  refs.trigger.addEventListener("click", () => {
    toggleYearDropdown(role);
  });

  refs.trigger.addEventListener("keydown", (event) => {
    handleYearDropdownTriggerKeydown(role, event);
  });

  refs.panel.addEventListener("keydown", (event) => {
    handleYearDropdownPanelKeydown(role, event);
  });
}

function handleYearDropdownTriggerKeydown(role, event) {
  if (["Enter", " ", "ArrowDown", "ArrowUp"].includes(event.key)) {
    event.preventDefault();
    openYearDropdown(role);
    const direction =
      event.key === "ArrowUp"
        ? -1
        : event.key === "ArrowDown"
          ? 1
          : 0;
    focusYearDropdownOption(role, direction);
    return;
  }

  if (event.key === "Escape") {
    event.preventDefault();
    closeYearDropdown(role);
  }
}

function handleYearDropdownPanelKeydown(role, event) {
  if (event.key === "ArrowDown") {
    event.preventDefault();
    focusYearDropdownOption(role, 1);
    return;
  }

  if (event.key === "ArrowUp") {
    event.preventDefault();
    focusYearDropdownOption(role, -1);
    return;
  }

  if (event.key === "Home") {
    event.preventDefault();
    focusYearDropdownOption(role, -9999);
    return;
  }

  if (event.key === "End") {
    event.preventDefault();
    focusYearDropdownOption(role, 9999);
    return;
  }

  if (event.key === "Enter" || event.key === " ") {
    event.preventDefault();
    const refs = getYearDropdownRefs(role);
    if (!refs.panel) {
      return;
    }

    const activeOption =
      document.activeElement instanceof HTMLElement &&
      document.activeElement.classList.contains("year-dropdown-option")
        ? document.activeElement
        : refs.panel.querySelector(".year-dropdown-option.is-active");

    if (activeOption instanceof HTMLElement) {
      selectYearFromDropdown(role, activeOption.dataset.year, true);
    }
    return;
  }

  if (event.key === "Escape") {
    event.preventDefault();
    closeYearDropdown(role);
    const refs = getYearDropdownRefs(role);
    refs.trigger?.focus();
    return;
  }

  if (event.key === "Tab") {
    closeYearDropdown(role);
  }
}

function getVisibleYearOptions(role) {
  const refs = getYearDropdownRefs(role);
  if (!refs.panel) {
    return [];
  }

  return [...refs.panel.querySelectorAll(".year-dropdown-option")];
}

function focusYearDropdownOption(role, direction) {
  const refs = getYearDropdownRefs(role);
  const options = getVisibleYearOptions(role);
  if (!refs.panel || options.length === 0) {
    return;
  }

  let nextIndex;
  if (direction <= -9999) {
    nextIndex = 0;
  } else if (direction >= 9999) {
    nextIndex = options.length - 1;
  } else {
    const focusedIndex = options.findIndex((option) => option === document.activeElement);
    const activeIndex = options.findIndex((option) => option.classList.contains("is-active"));
    const baseIndex = focusedIndex >= 0 ? focusedIndex : activeIndex >= 0 ? activeIndex : 0;
    nextIndex = clamp(baseIndex + direction, 0, options.length - 1);
  }

  const targetOption = options[nextIndex];
  if (!(targetOption instanceof HTMLElement)) {
    return;
  }

  targetOption.focus();
  targetOption.scrollIntoView({ block: "nearest" });
}

function selectYearFromDropdown(role, selectedYear, shouldNotify = true) {
  const refs = getYearDropdownRefs(role);
  if (!refs.input) {
    return;
  }

  const normalizedYear = sanitizeSelectedYear(selectedYear);
  const nextValue = String(normalizedYear);
  const previousValue = refs.input.value;

  refs.input.value = nextValue;
  if (refs.label) {
    refs.label.textContent = nextValue;
  }

  updateYearDropdownSelection(role, normalizedYear);
  closeYearDropdown(role);

  if (shouldNotify && previousValue !== nextValue) {
    refs.input.dispatchEvent(new Event("change", { bubbles: true }));
  }
}

function updateYearDropdownSelection(role, selectedYear) {
  const options = getVisibleYearOptions(role);
  const selectedValue = String(sanitizeSelectedYear(selectedYear));

  for (const option of options) {
    const isActive = option.dataset.year === selectedValue;
    option.classList.toggle("is-active", isActive);
    option.setAttribute("aria-selected", isActive ? "true" : "false");
  }
}

function toggleYearDropdown(role) {
  const refs = getYearDropdownRefs(role);
  if (!refs.dropdown) {
    return;
  }

  if (refs.dropdown.classList.contains("is-open")) {
    closeYearDropdown(role);
  } else {
    openYearDropdown(role);
  }
}

function openYearDropdown(role) {
  const refs = getYearDropdownRefs(role);
  if (!refs.dropdown || !refs.trigger || !refs.panel || refs.trigger.disabled) {
    return;
  }

  closeAllYearDropdowns(role);

  refs.dropdown.classList.add("is-open");
  refs.trigger.setAttribute("aria-expanded", "true");
  refs.panel.classList.remove("hidden");
  updateYearDropdownDirection(role);

  const activeOption = refs.panel.querySelector(".year-dropdown-option.is-active");
  if (activeOption instanceof HTMLElement) {
    activeOption.focus();
    activeOption.scrollIntoView({ block: "nearest" });
  }
}

function closeYearDropdown(role) {
  const refs = getYearDropdownRefs(role);
  if (!refs.dropdown || !refs.trigger || !refs.panel) {
    return;
  }

  refs.dropdown.classList.remove("is-open");
  refs.dropdown.classList.remove("is-drop-up");
  refs.trigger.setAttribute("aria-expanded", "false");
  refs.panel.classList.add("hidden");
}

function closeAllYearDropdowns(exceptRole = null) {
  if (exceptRole !== "start") {
    closeYearDropdown("start");
  }

  if (exceptRole !== "end") {
    closeYearDropdown("end");
  }
}

function handleYearDropdownOutsideClick(event) {
  const target = event.target;
  const clickedStart = ui.yearStartDropdown?.contains(target);
  const clickedEnd = ui.yearEndDropdown?.contains(target);

  if (!clickedStart && !clickedEnd) {
    closeAllYearDropdowns();
  }
}

function handleYearDropdownGlobalKeydown(event) {
  if (event.key === "Escape") {
    closeAllYearDropdowns();
  }
}

function updateYearDropdownDirection(role) {
  const refs = getYearDropdownRefs(role);
  if (!refs.dropdown || !refs.trigger || !refs.panel) {
    return;
  }

  refs.dropdown.classList.remove("is-drop-up");

  const triggerRect = refs.trigger.getBoundingClientRect();
  const panelHeight = Math.min(refs.panel.scrollHeight, 245) + 10;
  const spaceBelow = window.innerHeight - triggerRect.bottom;
  const spaceAbove = triggerRect.top;
  const shouldDropUp = spaceBelow < panelHeight && spaceAbove > spaceBelow;

  refs.dropdown.classList.toggle("is-drop-up", shouldDropUp);
}

function handleYearDropdownViewportChange() {
  if (ui.yearStartDropdown?.classList.contains("is-open")) {
    updateYearDropdownDirection("start");
  }

  if (ui.yearEndDropdown?.classList.contains("is-open")) {
    updateYearDropdownDirection("end");
  }
}

function normalizeYearRange(startYear, endYear) {
  const sanitizedStartYear = sanitizeSelectedYear(startYear);
  const sanitizedEndYear = sanitizeSelectedYear(endYear ?? sanitizedStartYear);

  if (sanitizedStartYear <= sanitizedEndYear) {
    return {
      startYear: sanitizedStartYear,
      endYear: sanitizedEndYear
    };
  }

  return {
    startYear: sanitizedEndYear,
    endYear: sanitizedStartYear
  };
}

function syncYearRangeState(startYear, endYear) {
  const normalizedRange = normalizeYearRange(startYear, endYear);
  state.selectedYearStart = normalizedRange.startYear;
  state.selectedYearEnd = normalizedRange.endYear;

  if (ui.yearStartInput) {
    ui.yearStartInput.value = String(normalizedRange.startYear);
  }
  if (ui.yearStartLabel) {
    ui.yearStartLabel.textContent = String(normalizedRange.startYear);
  }

  if (ui.yearEndInput) {
    ui.yearEndInput.value = String(normalizedRange.endYear);
  }
  if (ui.yearEndLabel) {
    ui.yearEndLabel.textContent = String(normalizedRange.endYear);
  }

  updateYearDropdownSelection("start", normalizedRange.startYear);
  updateYearDropdownSelection("end", normalizedRange.endYear);

  return normalizedRange;
}

function readYearRangeFromInputs() {
  const startYear = ui.yearStartInput ? ui.yearStartInput.value : state.selectedYearStart;
  const endYear = ui.yearEndInput ? ui.yearEndInput.value : state.selectedYearEnd;
  return normalizeYearRange(startYear, endYear);
}

function formatYearRangeLabel(startYear, endYear) {
  return startYear === endYear
    ? String(startYear)
    : `${startYear} - ${endYear}`;
}

function scheduleYearRangeAutoRefresh() {
  if (!tauriInvoke || state.scanning) {
    return;
  }

  const username = ui.username.value.trim();
  const rootPath = ui.rootPath.value.trim();
  if (!username || !rootPath) {
    return;
  }

  if (yearRangeRefreshTimer) {
    clearTimeout(yearRangeRefreshTimer);
  }

  yearRangeRefreshTimer = window.setTimeout(() => {
    yearRangeRefreshTimer = null;
    void scanFromInputs({
      startupMode: false,
      persistOnSuccess: state.autoSave,
      isRefresh: true
    });
  }, 250);
}

function populateYearOptions(selectElement, selectedYear) {
  const role = resolveYearDropdownRole(selectElement);
  const refs = getYearDropdownRefs(role);
  if (!role || !refs.panel) {
    return;
  }

  const resolvedYear = sanitizeSelectedYear(selectedYear);
  const resolvedValue = String(resolvedYear);

  if (refs.input) {
    refs.input.value = resolvedValue;
  }

  if (refs.label) {
    refs.label.textContent = resolvedValue;
  }

  refs.panel.innerHTML = "";

  for (let year = CURRENT_YEAR; year >= MIN_SUPPORTED_YEAR; year -= 1) {
    const option = document.createElement("button");
    option.type = "button";
    option.className = "year-dropdown-option";
    option.dataset.year = String(year);
    option.textContent = String(year);
    option.setAttribute("role", "option");
    option.setAttribute("aria-selected", year === resolvedYear ? "true" : "false");
    option.tabIndex = -1;

    if (year === resolvedYear) {
      option.classList.add("is-active");
    }

    option.addEventListener("click", () => {
      selectYearFromDropdown(role, year, true);
    });

    refs.panel.appendChild(option);
  }

  updateYearDropdownSelection(role, resolvedYear);
}

function setCacheInfo(message) {
  if (ui.cacheInfo) {
    ui.cacheInfo.textContent = message;
  }
}

function getCacheAgeInfo(scannedAt) {
  const scannedDate = new Date(scannedAt);
  if (Number.isNaN(scannedDate.getTime())) {
    return {
      ageMs: Number.POSITIVE_INFINITY,
      relativeText: "invalid cache time"
    };
  }

  const ageMs = Math.max(0, Date.now() - scannedDate.getTime());
  return {
    ageMs,
    relativeText: formatRelativeAge(ageMs)
  };
}

function formatRelativeAge(ageMs) {
  const minuteMs = 60 * 1000;
  const hourMs = 60 * minuteMs;
  const dayMs = 24 * hourMs;

  if (ageMs < 45 * 1000) {
    return "just now";
  }

  if (ageMs < hourMs) {
    const minutes = Math.max(1, Math.round(ageMs / minuteMs));
    return `${minutes} mins ago`;
  }

  if (ageMs < dayMs) {
    const hours = Math.max(1, Math.round(ageMs / hourMs));
    return `${hours} hours ago`;
  }

  const days = Math.max(1, Math.round(ageMs / dayMs));
  return `${days} days ago`;
}

function buildCacheInfoMessage(scannedAt, ageInfo, isStale) {
  const staleLabel = isStale ? ` • TTL exceeded by ${state.cacheTtlMinutes} menit` : "";
  return `Cache: ${ageInfo.relativeText} (${formatDateTime(scannedAt)})${staleLabel}`;
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return new Intl.DateTimeFormat("en-US", {
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit"
  }).format(date);
}

function getCssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

function normalizeDepthValue(value, fallback = DEFAULT_MAX_DEPTH) {
  const fallbackNumber = Number.parseInt(String(fallback ?? DEFAULT_MAX_DEPTH), 10);
  const safeFallback = Number.isFinite(fallbackNumber)
    ? clamp(fallbackNumber, MIN_SCAN_DEPTH, MAX_SCAN_DEPTH)
    : DEFAULT_MAX_DEPTH;

  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) {
    return safeFallback;
  }

  return clamp(parsed, MIN_SCAN_DEPTH, MAX_SCAN_DEPTH);
}

function stepDepthValue(delta) {
  if (!ui.maxDepthInput) {
    return;
  }

  const currentDepth = normalizeDepthValue(ui.maxDepthInput.value, state.maxDepth);
  const nextDepth = clamp(currentDepth + delta, MIN_SCAN_DEPTH, MAX_SCAN_DEPTH);
  state.maxDepth = nextDepth;
  ui.maxDepthInput.value = String(nextDepth);
}
