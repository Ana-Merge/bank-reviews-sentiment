export const hasComparisonPeriod = (date_start_2, date_end_2) => {
    return date_start_2 && date_end_2 && date_start_2 !== "2026-01-01" && date_end_2 !== "2026-01-01";
};