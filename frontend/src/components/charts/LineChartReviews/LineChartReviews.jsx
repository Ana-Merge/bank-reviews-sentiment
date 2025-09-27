import {
    LineChart,
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ResponsiveContainer,
} from "recharts";
import styles from "./LineChartReviews.module.scss";

const LineChartReviews = ({ chartData, aggregationType }) => {
    const formattedData = chartData.map((item) => ({
        name: item.aggregation,
        "Процентное изменение": item.change_percent,
    }));

    return (
        <div className={styles.chartContainer}>
            <ResponsiveContainer width="100%" height={320}>
                <LineChart
                    data={formattedData}
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
                    <Tooltip />
                    <Line
                        type="monotone"
                        dataKey="Процентное изменение"
                        stroke="url(#colorPercentageChange)"
                        strokeWidth={2}
                        dot={false}
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
};

export default LineChartReviews;