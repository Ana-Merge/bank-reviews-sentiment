import styles from "./ProductAnalyticsTable.module.scss";

const ProductAnalyticsTable = ({ productStats, showComparison = true }) => {
  const getRatingColor = (rating) => {
    if (rating >= 4.0) return styles.colorPositive;
    if (rating >= 3.0) return styles.colorNeutral;
    return styles.colorNegative;
  };

  const formatNumber = (num) => {
    return new Intl.NumberFormat("ru-RU").format(num);
  };

  const getChangeColor = (changePercent) => {
    return changePercent >= 0 ? styles.colorPositive : styles.colorNegative;
  };

  const formatPercent = (percent) => {
    return `${percent >= 0 ? "+" : ""}${percent.toFixed(1)}%`;
  };

  if (!productStats || productStats.length === 0) {
    return <div className={styles.noData}>Данные не найдены для выбранного продукта.</div>;
  }

  const maxChange = Math.max(
    ...productStats.map((p) => Math.abs(p.change_percent))
  );
  const maxCount = Math.max(...productStats.map((p) => p.count));

  // Если сравнение отключено, убираем столбец "Изменение"
  if (!showComparison) {
    return (
      <div className={styles.tableContainer}>
        <h3 className={styles.tableTitle}>Аналитика продуктов</h3>

        <div className={styles.tableWrapper}>
          <table className={styles.table}>
            <thead>
              <tr>
                <th style={{ width: "25%" }}>Продукт</th>
                <th style={{ width: "20%" }}>Количество отзывов</th>
                <th style={{ width: "40%" }}>Тональность отзывов</th>
                <th style={{ width: "15%" }}>Средний рейтинг</th>
              </tr>
            </thead>
            <tbody>
              {productStats.map((product, index) => {
                const maxProductTonal = Math.max(
                  product.tonality.negative,
                  product.tonality.neutral,
                  product.tonality.positive
                );
                return (
                  <tr key={index}>
                    <td>
                      <span className={styles.productName}>
                        {product.product_name}
                      </span>
                    </td>
                    <td>
                      <div className={styles.countChart}>
                        <div className={styles.chartWithValue}>
                          <span className={styles.chartValue}>
                            {formatNumber(product.count)}
                          </span>
                          <div
                            className={styles.chartBar}
                            style={{
                              width: `${(product.count / maxCount) * 80}%`,
                              backgroundColor: styles.colorPrimary,
                            }}
                          ></div>
                        </div>
                      </div>
                    </td>
                    <td className={styles.tonalityCell}>
                      <div className={styles.tonalityCharts}>
                        <div className={styles.tonalityRow}>
                          <div
                            className={styles.tonalityBar}
                            style={{
                              flexGrow:
                                maxProductTonal > 0
                                  ? product.tonality.negative / maxProductTonal
                                  : 1,
                              backgroundColor: styles.colorNegative,
                            }}
                          >
                            <span className={styles.tonalityValue}>
                              {product.tonality.negative}
                            </span>
                          </div>
                          <div
                            className={styles.tonalityBar}
                            style={{
                              flexGrow:
                                maxProductTonal > 0
                                  ? product.tonality.neutral / maxProductTonal
                                  : 1,
                              backgroundColor: styles.colorNeutral,
                            }}
                          >
                            <span className={styles.tonalityValue}>
                              {product.tonality.neutral}
                            </span>
                          </div>
                          <div
                            className={styles.tonalityBar}
                            style={{
                              flexGrow:
                                maxProductTonal > 0
                                  ? product.tonality.positive / maxProductTonal
                                  : 1,
                              backgroundColor: styles.colorPositive,
                            }}
                          >
                            <span className={styles.tonalityValue}>
                              {product.tonality.positive}
                            </span>
                          </div>
                        </div>
                      </div>
                    </td>
                    <td>
                      <div className={styles.rating}>
                        <span className={styles.ratingValue}>
                          {product.avg_rating.toFixed(1)}
                        </span>
                        <span
                          className={styles.singleStar}
                          style={{ color: getRatingColor(product.avg_rating) }}
                        >
                          ★
                        </span>
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    );
  }

  return (
    <div className={styles.tableContainer}>
      <h3 className={styles.tableTitle}>Аналитика продуктов</h3>

      <div className={styles.tableWrapper}>
        <table className={styles.table}>
          <thead>
            <tr>
              <th style={{ width: "25%" }}>Продукт</th>
              <th style={{ width: "15%" }}>Изменение</th>
              <th style={{ width: "15%" }}>Количество отзывов</th>
              <th style={{ width: "30%" }}>Тональность отзывов</th>
              <th style={{ width: "15%" }}>Средний рейтинг</th>
            </tr>
          </thead>
          <tbody>
            {productStats.map((product, index) => {
              const maxProductTonal = Math.max(
                product.tonality.negative,
                product.tonality.neutral,
                product.tonality.positive
              );
              return (
                <tr key={index}>
                  <td>
                    <span className={styles.productName}>
                      {product.product_name}
                    </span>
                  </td>
                  <td>
                    <div className={styles.changeChart}>
                      <div className={styles.chartWithValue}>
                        <span className={styles.chartValue}>
                          {formatPercent(product.change_percent)}
                        </span>
                        <div
                          className={styles.chartBar}
                          style={{
                            width: `${(Math.abs(product.change_percent) / maxChange) *
                              80
                              }%`,
                            backgroundColor: getChangeColor(
                              product.change_percent
                            ),
                          }}
                        ></div>
                      </div>
                    </div>
                  </td>
                  <td>
                    <div className={styles.countChart}>
                      <div className={styles.chartWithValue}>
                        <span className={styles.chartValue}>
                          {formatNumber(product.count)}
                        </span>
                        <div
                          className={styles.chartBar}
                          style={{
                            width: `${(product.count / maxCount) * 80}%`,
                            backgroundColor: styles.colorPrimary,
                          }}
                        ></div>
                      </div>
                    </div>
                  </td>
                  <td className={styles.tonalityCell}>
                    <div className={styles.tonalityCharts}>
                      <div className={styles.tonalityRow}>
                        <div
                          className={styles.tonalityBar}
                          style={{
                            flexGrow:
                              maxProductTonal > 0
                                ? product.tonality.negative / maxProductTonal
                                : 1,
                            backgroundColor: styles.colorNegative,
                          }}
                        >
                          <span className={styles.tonalityValue}>
                            {product.tonality.negative}
                          </span>
                        </div>
                        <div
                          className={styles.tonalityBar}
                          style={{
                            flexGrow:
                              maxProductTonal > 0
                                ? product.tonality.neutral / maxProductTonal
                                : 1,
                            backgroundColor: styles.colorNeutral,
                          }}
                        >
                          <span className={styles.tonalityValue}>
                            {product.tonality.neutral}
                          </span>
                        </div>
                        <div
                          className={styles.tonalityBar}
                          style={{
                            flexGrow:
                              maxProductTonal > 0
                                ? product.tonality.positive / maxProductTonal
                                : 1,
                            backgroundColor: styles.colorPositive,
                          }}
                        >
                          <span className={styles.tonalityValue}>
                            {product.tonality.positive}
                          </span>
                        </div>
                      </div>
                    </div>
                  </td>
                  <td>
                    <div className={styles.rating}>
                      <span className={styles.ratingValue}>
                        {product.avg_rating.toFixed(1)}
                      </span>
                      <span
                        className={styles.singleStar}
                        style={{ color: getRatingColor(product.avg_rating) }}
                      >
                        ★
                      </span>
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
};
export default ProductAnalyticsTable;