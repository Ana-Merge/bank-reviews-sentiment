import styles from "./ChangeChart.module.scss";

const ChangeChart = ({ data, productName, showComparison = true }) => {
  if (!data) return null;

  const { total, change_percent } = data;

  // Если сравнение отключено, показ только общего количества
  if (!showComparison) {
    return (
      <div className={styles.changeChart}>
        <div className={styles.header}>
          <h3>Общая статистика отзывов</h3>
          {productName && <span className={styles.productName}>{productName}</span>}
        </div>
        <div className={styles.chartContainer}>
          <div className={styles.circleWrapper}>
            <div className={styles.outerCircle} style={{ background: 'transparent' }}>
              <div className={styles.innerCircle}>
                <span className={styles.totalValue}>{total}</span>
              </div>
            </div>
            <span className={styles.percentValue} style={{ display: 'none' }}>
              {change_percent > 0 ? `+${change_percent}%` : `${change_percent}%`}
            </span>
          </div>
        </div>
      </div>
    );
  }

  const normalizedPercent = Math.min(Math.abs(change_percent), 100);

  const getChangeConfig = () => {
    if (change_percent > 0) return { color: "#10b981", direction: "normal" };
    if (change_percent < 0) return { color: "#ef4444", direction: "reverse" };
    return { color: "#6b7280", direction: "normal" };
  };

  const { color: changeColor, direction } = getChangeConfig();
  const absoluteChange = Math.abs(change_percent);

  const formattedPercent = change_percent > 0
    ? `+${absoluteChange}%`
    : change_percent < 0
      ? `-${absoluteChange}%`
      : `${absoluteChange}%`;

  return (
    <div className={styles.changeChart}>
      <div className={styles.header}>
        <h3>Общая статистика отзывов</h3>
        {productName && <span className={styles.productName}>{productName}</span>}
      </div>
      <div className={styles.chartContainer}>
        <div className={styles.circleWrapper}>
          <div
            className={styles.outerCircle}
            style={{
              background: direction === "normal"
                ? `conic-gradient(from 0deg, ${changeColor} 0% ${normalizedPercent}%, transparent ${normalizedPercent}% 100%)`
                : `conic-gradient(from 0deg, transparent 0% ${100 - normalizedPercent}%, ${changeColor} ${100 - normalizedPercent}% 100%)`
            }}
          >
            <div className={styles.innerCircle}>
              <span className={styles.totalValue}>{total}</span>
            </div>
          </div>
          <span className={styles.percentValue} style={{ color: changeColor }}>
            {formattedPercent}
          </span>
        </div>
      </div>
    </div>
  );
};

export default ChangeChart;