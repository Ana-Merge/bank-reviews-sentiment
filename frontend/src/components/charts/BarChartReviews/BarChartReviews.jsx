import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
} from "recharts";
import { useState } from "react";
import styles from "./BarChartReviews.module.scss";

const BarChartReviews = ({ chartData, aggregationType, productName }) => {
    const [activeChart, setActiveChart] = useState("period2");

    if (!chartData || !chartData.period1 || !chartData.period2) {
        return <div className={styles.noData}>Нет данных для отображения графика</div>;
    }

    const period1 = chartData.period2;
    const period2 = chartData.period1;

    const totalPairs = Math.max(period1.length, period2.length);

    const maxValue = Math.max(
        ...period1.map(item => item.count),
        ...period2.map(item => item.count)
    );

    // Данные для линейного графика (процентные изменения)
    const lineChartData = chartData.changes ? chartData.changes.map((item) => ({
        name: item.aggregation,
        "В сравнении с прошлым": item.change_percent,
    })) : [];

    // Данные для линейного графика period2
    const period2LineChartData = period2.map((item) => ({
        name: item.aggregation,
        "Выбранный период": item.count,
    }));

    const formatDate = (dateString) => {
        if (aggregationType === 'month') {
            const date = new Date(dateString + '-01');
            const month = date.toLocaleDateString('ru-RU', { month: 'short' });
            const year = date.getFullYear();
            return { month, year };
        } else if (aggregationType === 'week') {
            return formatWeekLabel(dateString);
        } else {
            const date = new Date(dateString);
            const dayMonth = date.toLocaleDateString('ru-RU', { day: 'numeric', month: 'short' });
            const year = date.getFullYear();
            return { dayMonth, year };
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
            return `${startOfWeek.getDate()}-${endOfWeek.getDate()} ${monthStart} ${year}`;
        } else {
            return `${startOfWeek.getDate()} ${monthStart}-${endOfWeek.getDate()} ${monthEnd} ${year}`;
        }
    };

    const calculateHeight = (value) => {
        if (maxValue === 0) return 30;
        return Math.max((value / maxValue) * 150, 30);
    };

    const getAggregationText = () => {
        switch (aggregationType) {
            case 'month': return 'по месяцам';
            case 'week': return 'по неделям';
            case 'day': return 'по дням';
            default: return 'по периодам';
        }
    };

    const renderLineChart = () => {
        if (activeChart === "period2") {
            return (
                <div className={styles.lineChartSection}>
                    <div className={styles.lineChartContainer}>
                        <ResponsiveContainer width="100%" height={300}>
                            <LineChart
                                data={period2LineChartData}
                                margin={{
                                    top: 5,
                                    right: 30,
                                    left: 20,
                                    bottom: 40,
                                }}
                            >
                                <defs>
                                    <linearGradient id="colorPeriod2" x1="0" y1="0" x2="1" y2="0">
                                        <stop offset="5%" stopColor="#ff7c7c" stopOpacity={0.8} />
                                        <stop offset="95%" stopColor="#ff4d4d" stopOpacity={0.8} />
                                    </linearGradient>
                                </defs>
                                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                                <XAxis
                                    dataKey="name"
                                    tickFormatter={(tick) => {
                                        if (aggregationType === "month") {
                                            return tick.substring(0, 7);
                                        }
                                        return tick;
                                    }}
                                    tick={{ fontSize: 12, dy: 10 }}
                                />
                                <YAxis />
                                <Tooltip
                                    formatter={(value) => [`${value}`, 'Выбранный период']}
                                    labelFormatter={(label) => {
                                        if (aggregationType === "month") {
                                            return `Период: ${label.substring(0, 7)}`;
                                        }
                                        return `Период: ${label}`;
                                    }}
                                />
                                <Line
                                    type="monotone"
                                    dataKey="Выбранный период"
                                    stroke="url(#colorPeriod2)"
                                    strokeWidth={2}
                                    dot={{ r: 2 }}
                                    activeDot={{ r: 4 }}
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            );
        } else {
            return (
                <div className={styles.lineChartSection}>
                    <div className={styles.lineChartContainer}>
                        <ResponsiveContainer width="100%" height={300}>
                            <LineChart
                                data={lineChartData}
                                margin={{
                                    top: 5,
                                    right: 30,
                                    left: 20,
                                    bottom: 40,
                                }}
                            >
                                <defs>
                                    <linearGradient id="colorPercentageChange" x1="0" y1="0" x2="1" y2="0">
                                        <stop offset="5%" stopColor="#82ca9d" stopOpacity={0.8} />
                                        <stop offset="95%" stopColor="#8884d8" stopOpacity={0.8} />
                                    </linearGradient>
                                </defs>
                                <CartesianGrid strokeDasharray="3 3" vertical={false} />
                                <XAxis
                                    dataKey="name"
                                    tickFormatter={(tick) => {
                                        if (aggregationType === "month") {
                                            return tick.substring(0, 7);
                                        }
                                        return tick;
                                    }}
                                    tick={{ fontSize: 12, dy: 10 }}
                                />
                                <YAxis />
                                <Tooltip
                                    formatter={(value) => [`${value}%`, 'В сравнении с прошлым']}
                                    labelFormatter={(label) => {
                                        if (aggregationType === "month") {
                                            return `Период: ${label.substring(0, 7)}`;
                                        }
                                        return `Период: ${label}`;
                                    }}
                                />
                                <Line
                                    type="monotone"
                                    dataKey="В сравнении с прошлым"
                                    stroke="url(#colorPercentageChange)"
                                    strokeWidth={2}
                                    dot={{ r: 2 }}
                                    activeDot={{ r: 4 }}
                                />
                            </LineChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            );
        }
    };

    return (
        <div className={styles.barChartContainer}>
            <div className={styles.header}>
                <h3>Динамика отзывов</h3>
                {productName && <span className={styles.productName}>{productName}</span>}
            </div>

            {/* Кнопки переключения графиков */}
            <div className={styles.chartToggle}>
                <button
                    className={`${styles.toggleButton} ${activeChart === "period2" ? styles.active : ""}`}
                    onClick={() => setActiveChart("period2")}
                >
                    Выбранный период
                </button>
                <button
                    className={`${styles.toggleButton} ${activeChart === "percentage" ? styles.active : ""}`}
                    onClick={() => setActiveChart("percentage")}
                >
                    В сравнении с прошлым
                </button>
            </div>

            {/* Линейный график */}
            {renderLineChart()}

            {/* Столбчатый график */}
            <div className={styles.chartContent}>
                <div className={styles.chartRow}>
                    {Array.from({ length: totalPairs }).map((_, index) => {
                        const period1Item = period1[index];
                        const period2Item = period2[index];

                        const period1Total = period1Item ? period1Item.count : 0;
                        const period2Total = period2Item ? period2Item.count : 0;
                        const period1Date = period1Item ? period1Item.aggregation : '';
                        const period2Date = period2Item ? period2Item.aggregation : '';

                        const height1 = calculateHeight(period1Total);
                        const height2 = calculateHeight(period2Total);

                        const formattedPeriod1 = period1Date ? formatDate(period1Date) : null;
                        const formattedPeriod2 = period2Date ? formatDate(period2Date) : null;

                        return (
                            <div key={index} className={styles.barPair}>
                                <div className={styles.barsContainer}>
                                    <div className={styles.singleBarContainer}>
                                        <div className={styles.barColumn}>
                                            <span className={styles.barValue}>{Number(period1Total) || 0}</span>
                                            <div
                                                className={`${styles.barVisual} ${styles.barPeriod1}`}
                                                style={{ height: `${height1}px` }}
                                                title={`Период 1: ${period1Total} отзывов (${period1Date})`}
                                            />
                                        </div>
                                        <div className={styles.barLabelContainer}>
                                            <div className={`${styles.barLabel} ${aggregationType === 'week' ? styles.weekLabel : ''}`}>
                                                {aggregationType === 'month' && formattedPeriod1 ? (
                                                    <div className={styles.monthLabel}>
                                                        <div className={styles.monthName}>{formattedPeriod1.month}</div>
                                                        <div className={styles.year}>{formattedPeriod1.year}</div>
                                                    </div>
                                                ) : aggregationType === 'day' && formattedPeriod1 ? (
                                                    <div className={styles.dayLabel}>
                                                        <div className={styles.dayMonth}>{formattedPeriod1.dayMonth}</div>
                                                        <div className={styles.year}>{formattedPeriod1.year}</div>
                                                    </div>
                                                ) : (
                                                    formattedPeriod1 || '—'
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                    <div className={styles.singleBarContainer}>
                                        <div className={styles.barColumn}>
                                            <span className={styles.barValue}>{Number(period2Total) || 0}</span>
                                            <div
                                                className={`${styles.barVisual} ${styles.barPeriod2}`}
                                                style={{ height: `${height2}px` }}
                                                title={`Период 2: ${period2Total} отзывов (${period2Date})`}
                                            />
                                        </div>
                                        <div className={styles.barLabelContainer}>
                                            <div className={`${styles.barLabel} ${aggregationType === 'week' ? styles.weekLabel : ''}`}>
                                                {aggregationType === 'month' && formattedPeriod2 ? (
                                                    <div className={styles.monthLabel}>
                                                        <div className={styles.monthName}>{formattedPeriod2.month}</div>
                                                        <div className={styles.year}>{formattedPeriod2.year}</div>
                                                    </div>
                                                ) : aggregationType === 'day' && formattedPeriod2 ? (
                                                    <div className={styles.dayLabel}>
                                                        <div className={styles.dayMonth}>{formattedPeriod2.dayMonth}</div>
                                                        <div className={styles.year}>{formattedPeriod2.year}</div>
                                                    </div>
                                                ) : (
                                                    formattedPeriod2 || '—'
                                                )}
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            </div>

            <div className={styles.legend}>
                <div className={styles.legendItem}>
                    <div className={styles.legendColorPeriod1}></div>
                    <span>Период для сравнения</span>
                </div>
                <div className={styles.legendItem}>
                    <div className={styles.legendColorPeriod2}></div>
                    <span>Выбранный период</span>
                </div>
                {totalPairs > 0 && (
                    <div className={styles.periodsInfo}>
                        Всего периодов: {totalPairs}
                    </div>
                )}
                <div className={styles.aggregationInfo}>
                    Группировка: {getAggregationText()}
                </div>
            </div>
        </div>
    );
};

export default BarChartReviews;