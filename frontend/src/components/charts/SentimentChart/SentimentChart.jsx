import styles from "./SentimentChart.module.scss";

const SentimentChart = ({ title, reviews_count, change_percent, data }) => {
  const colors = {
    Негатив: styles.colorNegative,
    Нейтрал: styles.colorNeutral,
    Позитив: styles.colorPositive,
  };

  const maxPercent = Math.max(...data.map((item) => item.percent));

  return (
    <div className={styles.sentimentChart}>
      <div className={styles.sentimentChartTop}>
        <span className={styles.chartTitle}>{title}</span>
      </div>
      <div className={styles.sentimentChartBottom}>
        <div className={styles.leftInfo}>
          <span className={styles.reviewsCount}>{reviews_count} отзывов</span>
          <span
            className={
              change_percent >= 0
                ? styles.positiveChange
                : styles.negativeChange
            }
          >
            {change_percent > 0 ? "+" : ""}
            {change_percent}%
          </span>
        </div>

        <div className={styles.chartContent}>
          <div className={styles.barChart}>
            {data.map((item) => (
              <div key={item.label} className={styles.barContainer}>
                <div
                  className={styles.bar}
                  style={{
                    height: `${(item.percent / maxPercent) * 100}%`,
                    backgroundColor: colors[item.label],
                  }}
                />
              </div>
            ))}
          </div>
        </div>

        <div className={styles.rightInfo}>
          {data.map((item) => (
            <div key={item.label} className={styles.percentItem}>
              <div
                className={styles.colorDot}
                style={{ backgroundColor: colors[item.label] }}
              />
              <span className={styles.percentValue}>{item.percent}%</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default SentimentChart;