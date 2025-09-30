import { useState, useEffect } from "react";
import styles from "./DateFilter.module.scss";

const DateFilter = ({
    startDate,
    endDate,
    startDate2,
    endDate2,
    onStartDateChange,
    onEndDateChange,
    onStartDate2Change,
    onEndDate2Change,
    aggregationType,
    onDateErrorsChange
}) => {
    const [errors, setErrors] = useState({});
    const [prevAggregationType, setPrevAggregationType] = useState(aggregationType);
    const MIN_DATE = "2024-01-01";
    const MAX_DATE = "2025-05-31";

    const roundToMonthStart = (dateString) => {
        const date = new Date(dateString);
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        return `${year}-${month}-01`;
    };

    const roundToMonthEnd = (dateString) => {
        const date = new Date(dateString);
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const lastDay = new Date(year, date.getMonth() + 1, 0).getDate();
        return `${year}-${month}-${lastDay}`;
    };

    const adjustDatesForMonthAggregation = () => {
        const newStartDate = roundToMonthStart(startDate);
        const newEndDate = roundToMonthEnd(endDate);

        const newStartDate2 = roundToMonthStart(startDate2);
        const newEndDate2 = roundToMonthEnd(endDate2);

        onStartDateChange(newStartDate);
        onEndDateChange(newEndDate);
        onStartDate2Change(newStartDate2);
        onEndDate2Change(newEndDate2);
    };

    const getMinMaxForAggregation = (type) => {
        if (type === 'month') {
            return {
                min: "2024-01",
                max: "2025-05"
            };
        }
        return {
            min: MIN_DATE,
            max: MAX_DATE
        };
    };

    const formatDateForInput = (dateString, type) => {
        if (!dateString) return '';
        if (type === 'month') {
            return dateString.substring(0, 7);
        }
        return dateString;
    };

    const parseDateFromInput = (inputValue, type) => {
        if (type === 'month') {
            return inputValue + '-01';
        }
        return inputValue;
    };

    const isValidDateForAggregation = (dateString, type) => {
        if (!dateString) return false;

        if (type === 'month') {
            const monthRegex = /^\d{4}-\d{2}$/;
            if (!monthRegex.test(dateString)) return false;
        } else {
            const dateRegex = /^\d{4}-\d{2}-\d{2}$/;
            if (!dateRegex.test(dateString)) return false;
        }

        const date = new Date(type === 'month' ? dateString + '-01' : dateString);
        const minDate = new Date(MIN_DATE);
        const maxDate = new Date(MAX_DATE);
        return date >= minDate && date <= maxDate;
    };

    const isEndDateAfterStartDate = (start, end, type) => {
        if (!start || !end) return true;

        const startDateObj = type === 'month' ? new Date(start + '-01') : new Date(start);
        const endDateObj = type === 'month' ? new Date(end + '-01') : new Date(end);
        return endDateObj >= startDateObj;
    };

    const isPeriod2BeforePeriod1 = (start1, end2, type) => {
        if (!start1 || !end2) return true;

        const start1Obj = type === 'month' ? new Date(start1 + '-01') : new Date(start1);
        const end2Obj = type === 'month' ? new Date(end2 + '-01') : new Date(end2);
        return end2Obj < start1Obj;
    };

    const getMaxDateForPeriod2 = (startDate, type) => {
        if (!startDate) return MAX_DATE;

        const startDateObj = type === 'month' ? new Date(startDate + '-01') : new Date(startDate);
        const dayBeforeStart = new Date(startDateObj);
        dayBeforeStart.setDate(dayBeforeStart.getDate() - 1);

        if (type === 'month') {
            const year = dayBeforeStart.getFullYear();
            const month = String(dayBeforeStart.getMonth() + 1).padStart(2, '0');
            return `${year}-${month}`;
        } else {
            return dayBeforeStart.toISOString().split('T')[0];
        }
    };

    const validateDateRange = (start, end, type, fieldPrefix, isPeriod2 = false) => {
        const newErrors = { ...errors };

        if (start && !isValidDateForAggregation(start, type)) {
            newErrors[`${fieldPrefix}Start`] = `Дата выходит за пределы доступного периода`;
        } else {
            delete newErrors[`${fieldPrefix}Start`];
        }

        if (end && !isValidDateForAggregation(end, type)) {
            newErrors[`${fieldPrefix}End`] = `Дата выходит за пределы доступного периода`;
        } else {
            delete newErrors[`${fieldPrefix}End`];
        }

        if (start && end && isValidDateForAggregation(start, type) && isValidDateForAggregation(end, type)) {
            if (!isEndDateAfterStartDate(start, end, type)) {
                newErrors[`${fieldPrefix}Range`] = 'Конечная дата не может быть раньше начальной';
            } else {
                delete newErrors[`${fieldPrefix}Range`];
            }
        }

        if (isPeriod2 && start && end && isValidDateForAggregation(start, type) && isValidDateForAggregation(end, type)) {
            const period1Start = formatDateForInput(startDate, aggregationType);
            if (!isPeriod2BeforePeriod1(period1Start, end, type)) {
                newErrors.period2BeforePeriod1 = 'Второй период должен быть раньше первого';
            } else {
                delete newErrors.period2BeforePeriod1;
            }
        }

        setErrors(newErrors);
        onDateErrorsChange(newErrors);
    };

    const getInputType = () => {
        return aggregationType === 'month' ? 'month' : 'date';
    };

    const getDateRangeInfoText = () => {
        if (aggregationType === 'month') {
            return `Доступный период: 01.01.2024 – 31.05.2025 (выбор по месяцам)`;
        }
        return `Доступный период: 01.01.2024 – 31.05.2025`;
    };

    useEffect(() => {
        if (prevAggregationType !== aggregationType) {
            if (aggregationType === 'month' && (prevAggregationType === 'day' || prevAggregationType === 'week')) {
                adjustDatesForMonthAggregation();
            }
            setPrevAggregationType(aggregationType);
        }
    }, [aggregationType, prevAggregationType]);

    useEffect(() => {
        validateDateRange(
            formatDateForInput(startDate, aggregationType),
            formatDateForInput(endDate, aggregationType),
            aggregationType,
            'period1'
        );
        validateDateRange(
            formatDateForInput(startDate2, aggregationType),
            formatDateForInput(endDate2, aggregationType),
            aggregationType,
            'period2',
            true
        );
    }, [startDate, endDate, startDate2, endDate2, aggregationType]);

    const handleStartDateChange = (inputValue) => {
        const parsedDate = parseDateFromInput(inputValue, aggregationType);
        onStartDateChange(parsedDate);
    };

    const handleEndDateChange = (inputValue) => {
        const parsedDate = parseDateFromInput(inputValue, aggregationType);
        onEndDateChange(parsedDate);
    };

    const handleStartDate2Change = (inputValue) => {
        const parsedDate = parseDateFromInput(inputValue, aggregationType);
        onStartDate2Change(parsedDate);
    };

    const handleEndDate2Change = (inputValue) => {
        const parsedDate = parseDateFromInput(inputValue, aggregationType);
        onEndDate2Change(parsedDate);
    };

    const { min, max } = getMinMaxForAggregation(aggregationType);
    const maxDateForPeriod2 = getMaxDateForPeriod2(formatDateForInput(startDate, aggregationType), aggregationType);

    return (
        <div className={styles.dateFilter}>
            <div className={styles.filterControls}>
                <div className={styles.periodSection}>
                    <h4 className={styles.periodTitle}>Период</h4>
                    <div className={styles.periodControls}>
                        <div className={styles.filterGroup}>
                            <label htmlFor="start-date">Начальная дата:</label>
                            <input
                                id="start-date"
                                type={getInputType()}
                                value={formatDateForInput(startDate, aggregationType)}
                                min={min}
                                max={max}
                                onChange={(e) => handleStartDateChange(e.target.value)}
                                className={errors.period1Start ? styles.errorInput : ''}
                            />
                            {errors.period1Start && <span className={styles.errorText}>{errors.period1Start}</span>}
                        </div>
                        <div className={styles.filterGroup}>
                            <label htmlFor="end-date">Конечная дата:</label>
                            <input
                                id="end-date"
                                type={getInputType()}
                                value={formatDateForInput(endDate, aggregationType)}
                                min={min}
                                max={max}
                                onChange={(e) => handleEndDateChange(e.target.value)}
                                className={errors.period1End ? styles.errorInput : ''}
                            />
                            {errors.period1End && <span className={styles.errorText}>{errors.period1End}</span>}
                        </div>
                    </div>
                    {errors.period1Range && <div className={styles.rangeError}>{errors.period1Range}</div>}
                </div>

                <div className={styles.periodSection}>
                    <h4 className={styles.periodTitle}>Период для сравнения</h4>
                    <div className={styles.periodControls}>
                        <div className={styles.filterGroup}>
                            <label htmlFor="start-date2">Начальная дата:</label>
                            <input
                                id="start-date2"
                                type={getInputType()}
                                value={formatDateForInput(startDate2, aggregationType)}
                                min={min}
                                max={maxDateForPeriod2}
                                onChange={(e) => handleStartDate2Change(e.target.value)}
                                className={errors.period2Start ? styles.errorInput : ''}
                            />
                            {errors.period2Start && <span className={styles.errorText}>{errors.period2Start}</span>}
                        </div>
                        <div className={styles.filterGroup}>
                            <label htmlFor="end-date2">Конечная дата:</label>
                            <input
                                id="end-date2"
                                type={getInputType()}
                                value={formatDateForInput(endDate2, aggregationType)}
                                min={min}
                                max={maxDateForPeriod2}
                                onChange={(e) => handleEndDate2Change(e.target.value)}
                                className={errors.period2End ? styles.errorInput : ''}
                            />
                            {errors.period2End && <span className={styles.errorText}>{errors.period2End}</span>}
                        </div>
                    </div>
                    {errors.period2Range && <div className={styles.rangeError}>{errors.period2Range}</div>}
                </div>
            </div>

            {errors.period2BeforePeriod1 && (
                <div className={styles.periodOrderError}>
                    ⚠️ {errors.period2BeforePeriod1}
                </div>
            )}

            <div className={styles.dateRangeInfo}>
                {getDateRangeInfoText()}
            </div>
        </div>
    );
};

export default DateFilter;