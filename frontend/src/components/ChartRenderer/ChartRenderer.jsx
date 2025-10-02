import { useState, useEffect, useCallback } from "react";
import { apiService } from "../../services/api";
import { getAllChildProducts, findProductInTree } from "../../utils/productUtils";
import { formatDate } from "../../utils/formatters";
import { hasComparisonPeriod } from "../../utils/dateUtils";
import {
    ProductAnalyticsTable,
    BarChartReviews,
    ChangeChart,
    TonalityChart,
    LoadingSpinner
} from "../../components";
import styles from "./ChartRenderer.module.scss";

const getSourceDisplayName = (source) => {
    const sourceMap = {
        null: 'Все источники'
    };
    return sourceMap[source] || 'Все источники';
};

const ChartRenderer = ({
    chartConfig,
    onDelete,
    onEdit,
    productTree,
    isEditable = false
}) => {
    const [chartData, setChartData] = useState(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const [selectedProduct, setSelectedProduct] = useState(null);
    const [childProducts, setChildProducts] = useState([]);

    const loadChartData = useCallback(async (product, children) => {
        if (!chartConfig || !product) return;

        setIsLoading(true);
        setError(null);

        try {
            const { type, attributes } = chartConfig;
            const {
                date_start_1, date_end_1, date_start_2, date_end_2,
                product_id, source, aggregation_type
            } = attributes;

            let data;

            switch (type) {
                case 'product_stats':
                    if (children && children.length > 0) {
                        const productStatsPromises = children.map(childProduct =>
                            apiService.getProductStats(
                                date_start_1, date_end_1, date_start_2, date_end_2,
                                childProduct.id, null, source || null
                            )
                        );

                        const results = await Promise.all(productStatsPromises);
                        data = results.flat().filter(item => item != null);
                    } else {
                        data = await apiService.getProductStats(
                            date_start_1, date_end_1, date_start_2, date_end_2,
                            product_id, null, source || null
                        );
                        if (data && !Array.isArray(data)) {
                            data = [];
                        }
                    }
                    break;
                case 'monthly-review-count':
                    data = await apiService.getReviewTonality(
                        product_id, date_start_1, date_end_1, date_start_2, date_end_2,
                        aggregation_type, source || null
                    );
                    break;
                case 'regional-bar-chart':
                    data = await apiService.getBarChartChanges(
                        product_id, date_start_1, date_end_1, date_start_2, date_end_2,
                        aggregation_type, source || null
                    );
                    break;
                case 'change-chart':
                    data = await apiService.getChangeChart(
                        product_id, date_start_1, date_end_1, date_start_2, date_end_2,
                        source || null
                    );
                    break;
                default:
                    throw new Error(`Unknown chart type: ${type}`);
            }

            setChartData(data);
        } catch (err) {
            setError(`Ошибка загрузки данных: ${err.message}`);
        } finally {
            setIsLoading(false);
        }
    }, [chartConfig]);

    useEffect(() => {
        if (!chartConfig || !productTree) return;

        const product = findProductInTree(productTree, chartConfig.attributes.product_id);
        setSelectedProduct(product);

        if (product) {
            const children = getAllChildProducts(productTree, product.id);
            setChildProducts(children);
            loadChartData(product, children);
        }
    }, [chartConfig, productTree, loadChartData]);

    const handleDelete = () => {
        if (onDelete && chartConfig.id) {
            onDelete(chartConfig.id);
        }
    };

    const handleEdit = () => {
        if (onEdit && chartConfig.id) {
            onEdit(chartConfig);
        }
    };

    if (isLoading) {
        return (
            <div className={styles.chartSection}>
                <LoadingSpinner />
            </div>
        );
    }

    if (error) {
        return (
            <div className={styles.chartSection}>
                <div className={styles.error}>{error}</div>
            </div>
        );
    }

    if (!chartData || !selectedProduct) {
        return (
            <div className={styles.chartSection}>
                <div className={styles.noData}>Нет данных для отображения</div>
            </div>
        );
    }

    const { type, attributes } = chartConfig;
    const {
        source,
        date_start_1,
        date_end_1,
        date_start_2,
        date_end_2
    } = attributes;

    const commonProps = {
        productName: selectedProduct.name,
        showComparison: hasComparisonPeriod(date_start_2, date_end_2)
    };

    const renderChartContent = () => {
        switch (type) {
            case 'product_stats':
                const safeProductStats = Array.isArray(chartData) ? chartData : [];
                return (
                    <ProductAnalyticsTable
                        productStats={safeProductStats}
                        showComparison={hasComparisonPeriod(date_start_2, date_end_2)}
                    />
                );
            case 'monthly-review-count':
                return (
                    <TonalityChart
                        chartData={chartData}
                        aggregationType={attributes.aggregation_type}
                        {...commonProps}
                    />
                );
            case 'regional-bar-chart':
                return (
                    <BarChartReviews
                        chartData={chartData}
                        aggregationType={attributes.aggregation_type}
                        {...commonProps}
                    />
                );
            case 'change-chart':
                return (
                    <ChangeChart
                        data={chartData}
                        {...commonProps}
                    />
                );
            default:
                return <div className={styles.error}>Неизвестный тип графика: {type}</div>;
        }
    };

    return (
        <div className={styles.chartSection}>
            <div className={styles.chartHeader}>
                <div className={styles.chartTitleSection}>
                    <div className={styles.chartFiltersInfo}>
                        {type === 'product_stats' && (
                            <div className={styles.filterItem}>
                                <span className={styles.filterLabel}>Продукт:</span>
                                <span className={styles.filterValue}>{selectedProduct.name}</span>
                            </div>
                        )}
                        <div className={styles.filterItem}>
                            <span className={styles.filterLabel}>Источник:</span>
                            <span className={styles.filterValue}>
                                {getSourceDisplayName(source)}
                            </span>
                        </div>
                        <div className={styles.filterItem}>
                            <span className={styles.filterLabel}>Период:</span>
                            <span className={styles.filterValue}>
                                {formatDate(date_start_1)} - {formatDate(date_end_1)}
                            </span>
                        </div>
                        {hasComparisonPeriod(date_start_2, date_end_2) && (
                            <div className={styles.filterItem}>
                                <span className={styles.filterLabel}>Период для сравнения:</span>
                                <span className={styles.filterValue}>
                                    {formatDate(date_start_2)} - {formatDate(date_end_2)}
                                </span>
                            </div>
                        )}
                    </div>
                </div>
                {isEditable && (
                    <div className={styles.chartActions}>
                        <button
                            className={styles.editChartButton}
                            onClick={handleEdit}
                            title="Редактировать график"
                        >
                            ✎
                        </button>
                        <button
                            className={styles.deleteChartButton}
                            onClick={handleDelete}
                            title="Удалить график"
                        >
                            ×
                        </button>
                    </div>
                )}
            </div>
            <div className={styles.chartContent}>
                {renderChartContent()}
            </div>
        </div>
    );
};

export default ChartRenderer;