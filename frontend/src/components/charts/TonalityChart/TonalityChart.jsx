import styles from "./TonalityChart.module.scss";

const TonalityChart = ({ chartData, aggregationType, productName }) => {
    if (!chartData || !chartData.period1 || !chartData.period2 || !chartData.changes) {
        return <div className={styles.noData}>Нет данных для отображения графика тональности</div>;
    }

    const period1 = chartData.period1;
    const period2 = chartData.period2;
    const changes = chartData.changes;

    const maxValue = Math.max(
        ...period1.flatMap(item => Object.values(item.tonality)),
        ...period2.flatMap(item => Object.values(item.tonality))
    );

    const formatChange = (value) => {
        if (value === null || value === undefined) return null;
        const formattedValue = (value % 1 === 0) ? value : value.toFixed(1);
        return `${value > 0 ? '+' : ''}${formattedValue}%`;
    };

    const getChangeClassName = (value) => {
        if (value > 0) return styles.changePositive;
        if (value < 0) return styles.changeNegative;
        return styles.changeNeutral;
    };

    const formatDate = (dateString) => {
        if (aggregationType === 'month') {
            const date = new Date(dateString + '-01');
            const month = date.toLocaleDateString('ru-RU', { month: 'short' });
            const year = date.getFullYear();
            return { type: 'month', month, year };
        } else if (aggregationType === 'week') {
            return formatWeekLabel(dateString);
        } else {
            const date = new Date(dateString);
            const dayMonth = date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });
            const year = date.getFullYear();
            return { type: 'day', dayMonth, year };
        }
    };

    const formatWeekLabel = (dateString) => {
        const date = new Date(dateString);
        const startOfWeek = new Date(date);
        startOfWeek.setDate(date.getDate() - (date.getDay() === 0 ? 6 : date.getDay() - 1));
        const endOfWeek = new Date(startOfWeek);
        endOfWeek.setDate(startOfWeek.getDate() + 6);

        const monthStart = startOfWeek.toLocaleDateString('ru-RU', { month: 'short' });
        const monthEnd = endOfWeek.toLocaleDateString('ru-RU', { month: 'short' });
        const year = startOfWeek.getFullYear();

        if (monthStart === monthEnd) {
            return {
                type: 'week',
                compact: `${startOfWeek.getDate()}-${endOfWeek.getDate()} ${monthStart}`,
                year,
                full: `${startOfWeek.getDate()}-${endOfWeek.getDate()} ${monthStart} ${year}`
            };
        } else {
            return {
                type: 'week',
                compact: `${startOfWeek.getDate()} ${monthStart}-${endOfWeek.getDate()} ${monthEnd}`,
                year,
                full: `${startOfWeek.getDate()} ${monthStart}-${endOfWeek.getDate()} ${monthEnd} ${year}`
            };
        }
    };

    const calculateHeight = (value) => {
        if (maxValue === 0) return 40;
        return Math.max((value / maxValue) * 320, 40);
    };

    const getAggregationText = () => {
        switch (aggregationType) {
            case 'month': return 'по месяцам';
            case 'week': return 'по неделям';
            case 'day': return 'по дням';
            default: return 'по периодам';
        }
    };

    const getChartDataWithChanges = () => {
        return period1.map(periodItem => {
            const correspondingChange = changes.find(changeItem => changeItem.aggregation === periodItem.aggregation);
            return {
                ...periodItem,
                percentage_change: correspondingChange ? correspondingChange.percentage_change : { positive: null, neutral: null, negative: null }
            };
        });
    };

    const chartDataWithChanges = getChartDataWithChanges();

    return (
        <div className={styles.tonalityChartContainer}>
            <div className={styles.header}>
                <h3>Тональность отзывов по периодам</h3>
                {productName && <span className={styles.productName}>{productName}</span>}
            </div>

            <div className={styles.chartContent}>
                <div className={styles.chartRow}>
                    {chartDataWithChanges.map((item, index) => {
                        const { aggregation, tonality, percentage_change } = item;
                        const { positive, neutral, negative } = tonality;
                        const { positive: pChange, neutral: nChange, negative: negChange } = percentage_change;

                        const positiveHeight = calculateHeight(positive);
                        const neutralHeight = calculateHeight(neutral);
                        const negativeHeight = calculateHeight(negative);

                        const formattedDate = formatDate(aggregation);

                        return (
                            <div key={index} className={styles.tonalityBarGroup}>
                                <div className={styles.barsContainer}>
                                    {/* Негативные отзывы */}
                                    <div className={styles.singleBarContainer}>
                                        <div className={styles.barColumn}>
                                            <div className={styles.barVisualContainer}>
                                                <div
                                                    className={`${styles.barVisual} ${styles.negative}`}
                                                    style={{ height: `${negativeHeight}px` }}
                                                    title={`Отрицательные: ${negative} отзывов`}
                                                >
                                                    <span className={styles.percentageChangeLabel}>
                                                        <span className={getChangeClassName(negChange)}>{formatChange(negChange)}</span>
                                                    </span>
                                                    <span className={`${styles.barValue} ${styles.negativeText}`}>{negative}</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    {/* Нейтральные отзывы */}
                                    <div className={styles.singleBarContainer}>
                                        <div className={styles.barColumn}>
                                            <div className={styles.barVisualContainer}>
                                                <div
                                                    className={`${styles.barVisual} ${styles.neutral}`}
                                                    style={{ height: `${neutralHeight}px` }}
                                                    title={`Нейтральные: ${neutral} отзывов`}
                                                >
                                                    <span className={styles.percentageChangeLabel}>
                                                        <span className={getChangeClassName(nChange)}>{formatChange(nChange)}</span>
                                                    </span>
                                                    <span className={styles.barValue}>{neutral}</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    {/* Положительные отзывы */}
                                    <div className={styles.singleBarContainer}>
                                        <div className={styles.barColumn}>
                                            <div className={styles.barVisualContainer}>
                                                <div
                                                    className={`${styles.barVisual} ${styles.positive}`}
                                                    style={{ height: `${positiveHeight}px` }}
                                                    title={`Положительные: ${positive} отзывов`}
                                                >
                                                    <span className={styles.percentageChangeLabel}>
                                                        <span className={getChangeClassName(pChange)}>{formatChange(pChange)}</span>
                                                    </span>
                                                    <span className={styles.barValue}>{positive}</span>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </div>

                                <div className={styles.dateLabelContainer}>
                                    <div className={`${styles.dateLabel} ${formattedDate.type === 'week' ? styles.weekLabel : ''}`}>
                                        {formattedDate.type === 'month' ? (
                                            <div className={styles.monthLabel}>
                                                <div className={styles.monthName}>{formattedDate.month}</div>
                                                <div className={styles.year}>{formattedDate.year}</div>
                                            </div>
                                        ) : formattedDate.type === 'day' ? (
                                            <div className={styles.dayLabel}>
                                                <div className={styles.dayMonth}>{formattedDate.dayMonth}</div>
                                                <div className={styles.year}>{formattedDate.year}</div>
                                            </div>
                                        ) : formattedDate.type === 'week' ? (
                                            <div className={styles.weekLabelContent} title={formattedDate.full}>
                                                <div className={styles.weekDates}>{formattedDate.compact}</div>
                                                <div className={styles.weekYear}>{formattedDate.year}</div>
                                            </div>
                                        ) : (
                                            '—'
                                        )}
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            <div className={styles.legend}>
                <div className={styles.legendItem}>
                    <div className={styles.legendColorNegative}></div>
                    <span>Отрицательные</span>
                </div>
                <div className={styles.legendItem}>
                    <div className={styles.legendColorNeutral}></div>
                    <span>Нейтральные</span>
                </div>
                <div className={styles.legendItem}>
                    <div className={styles.legendColorPositive}></div>
                    <span>Положительные</span>
                </div>
                {period1.length > 0 && (
                    <div className={styles.periodsInfo}>
                        Всего периодов: {period1.length}
                    </div>
                )}
                <div className={styles.aggregationInfo}>
                    Группировка: {getAggregationText()}
                </div>
            </div>
        </div>
    );
};

export default TonalityChart;