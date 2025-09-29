import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { useAppSelector, useAppDispatch } from "../../hooks/redux";
import { authService } from "../../services/auth";
import { apiService } from "../../services/api";
import { fetchProductTree } from "../../store/slices/productSlice";
import {
    ProductAnalyticsTable,
    BarChartReviews,
    ChangeChart,
    TonalityChart,
    AddChartModal,
    LoadingSpinner
} from "../../components";

import styles from "./DashboardPage.module.scss";

const getAllChildProducts = (productTree, productId) => {
    const findProductAndChildren = (nodes, targetId) => {
        for (let node of nodes) {
            if (node.id === targetId) {
                if (node.children && node.children.length > 0) {
                    return getAllLeafProducts(node.children);
                }
                return [node];
            }
            if (node.children) {
                const found = findProductAndChildren(node.children, targetId);
                if (found.length > 0) return found;
            }
        }
        return [];
    };

    const getAllLeafProducts = (nodes) => {
        let leaves = [];
        for (let node of nodes) {
            if (node.children && node.children.length > 0) {
                leaves = leaves.concat(getAllLeafProducts(node.children));
            } else {
                leaves.push(node);
            }
        }
        return leaves;
    };

    return findProductAndChildren(productTree, productId);
};

// Компонент для отображения графика по конфигурации
const ChartRenderer = ({ chartConfig, onDelete, onEdit, productTree }) => {
    const [chartData, setChartData] = useState(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const [selectedProduct, setSelectedProduct] = useState(null);
    const [childProducts, setChildProducts] = useState([]);

    // Нахождение продукта по product_id из конфигурации
    useEffect(() => {
        if (!chartConfig || !productTree) return;

        const findProduct = (nodes) => {
            for (let node of nodes) {
                if (node.id === chartConfig.attributes.product_id) {
                    return node;
                }
                if (node.children) {
                    const found = findProduct(node.children);
                    if (found) return found;
                }
            }
            return null;
        };

        const product = findProduct(productTree);
        setSelectedProduct(product);

        if (product) {
            const children = getAllChildProducts(productTree, product.id);
            setChildProducts(children);
        }
    }, [chartConfig, productTree]);

    useEffect(() => {
        if (!chartConfig || !selectedProduct) return;

        const fetchChartData = async () => {
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
                        if (childProducts.length > 0) {
                            // Загрузка данных для каждого дочернего продукта
                            const productStatsPromises = childProducts.map(childProduct =>
                                apiService.getProductStats(
                                    date_start_1, date_end_1, date_start_2, date_end_2,
                                    childProduct.id, null, source || null
                                )
                            );

                            const results = await Promise.all(productStatsPromises);
                            data = results.flat();
                        } else {
                            data = await apiService.getProductStats(
                                date_start_1, date_end_1, date_start_2, date_end_2,
                                product_id, null, source || null
                            );
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
                console.error("Failed to load chart data:", err);
            } finally {
                setIsLoading(false);
            }
        };

        fetchChartData();
    }, [chartConfig, selectedProduct, childProducts]);

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

    const formatDate = (dateString) => {
        const date = new Date(dateString);
        return date.toLocaleDateString('ru-RU', {
            day: '2-digit',
            month: '2-digit',
            year: 'numeric'
        });
    };

    const getSourceName = (source) => {
        const sourceNames = {
            null: 'Все источники',
            'Banki.ru': 'Banki.ru',
            'App Store': 'App Store',
            'Google Play': 'Google Play'
        };
        return sourceNames[source] || 'Все источники';
    };

    if (isLoading) {
        return (
            <div className={styles.chartSection}>
                <div className={styles.loading}><LoadingSpinner /></div>
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
        productName: selectedProduct.name
    };

    const renderChartContent = () => {
        switch (type) {
            case 'product_stats':
                return <ProductAnalyticsTable productStats={chartData} />;
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
                        {/* Имя продукта только для ProductAnalyticsTable */}
                        {type === 'product_stats' && (
                            <div className={styles.filterItem}>
                                <span className={styles.filterLabel}>Продукт:</span>
                                <span className={styles.filterValue}>{selectedProduct.name}</span>
                            </div>
                        )}
                        <div className={styles.filterItem}>
                            <span className={styles.filterLabel}>Источник:</span>
                            <span className={styles.filterValue}>{getSourceName(source)}</span>
                        </div>
                        <div className={styles.filterItem}>
                            <span className={styles.filterLabel}>Период:</span>
                            <span className={styles.filterValue}>
                                {formatDate(date_start_1)} - {formatDate(date_end_1)}
                            </span>
                        </div>
                        {date_start_2 && date_end_2 && (
                            <div className={styles.filterItem}>
                                <span className={styles.filterLabel}>Период для сравнения:</span>
                                <span className={styles.filterValue}>
                                    {formatDate(date_start_2)} - {formatDate(date_end_2)}
                                </span>
                            </div>
                        )}
                    </div>
                </div>
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
            </div>
            <div className={styles.chartContent}>
                {renderChartContent()}
            </div>
        </div>
    );
};

const DashboardPage = () => {
    const { pageId } = useParams();
    const dispatch = useAppDispatch();
    const { isAuthenticated, token } = useAppSelector(state => state.auth);
    const { productTree } = useAppSelector(state => state.product);

    const [page, setPage] = useState(null);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);
    const [showAddModal, setShowAddModal] = useState(false);
    const [editingChart, setEditingChart] = useState(null);

    useEffect(() => {
        if (isAuthenticated && token && pageId) {
            loadPage();
        }
        if (!productTree) {
            dispatch(fetchProductTree());
        }
    }, [isAuthenticated, token, pageId, dispatch, productTree]);

    const loadPage = async () => {
        setIsLoading(true);
        setError(null);

        try {
            const config = await authService.getUserDashboardsConfig(token);
            const foundPage = config.pages?.find(p => p.id === pageId);

            if (foundPage) {
                setPage(foundPage);
            } else {
                setError("Страница не найдена");
            }
        } catch (err) {
            setError(`Ошибка загрузки страницы: ${err.message}`);
            console.error("Failed to load page:", err);
        } finally {
            setIsLoading(false);
        }
    };

    const handleAddChart = async (chartData) => {
        try {
            const updatedPage = {
                ...page,
                charts: [...(page.charts || []), chartData]
            };

            const config = await authService.getUserDashboardsConfig(token);
            const updatedPages = config.pages.map(p =>
                p.id === pageId ? updatedPage : p
            );

            await authService.saveUserDashboardsConfig(token, { pages: updatedPages });

            setPage(updatedPage);
            setShowAddModal(false);
        } catch (err) {
            setError(`Ошибка добавления графика: ${err.message}`);
            console.error("Failed to add chart:", err);
        }
    };

    const handleEditChart = async (chartData) => {
        try {
            const updatedPage = {
                ...page,
                charts: page.charts.map(chart =>
                    chart.id === editingChart.id ? { ...chartData, id: editingChart.id } : chart
                )
            };

            const config = await authService.getUserDashboardsConfig(token);
            const updatedPages = config.pages.map(p =>
                p.id === pageId ? updatedPage : p
            );

            await authService.saveUserDashboardsConfig(token, { pages: updatedPages });

            setPage(updatedPage);
            setEditingChart(null);
        } catch (err) {
            setError(`Ошибка редактирования графика: ${err.message}`);
            console.error("Failed to edit chart:", err);
        }
    };

    const handleDeleteChart = async (chartId) => {
        if (!confirm("Вы уверены, что хотите удалить этот график?")) {
            return;
        }

        try {
            const updatedPage = {
                ...page,
                charts: page.charts.filter(chart => chart.id !== chartId)
            };

            const config = await authService.getUserDashboardsConfig(token);
            const updatedPages = config.pages.map(p =>
                p.id === pageId ? updatedPage : p
            );

            await authService.saveUserDashboardsConfig(token, { pages: updatedPages });

            setPage(updatedPage);
        } catch (err) {
            setError(`Ошибка удаления графика: ${err.message}`);
            console.error("Failed to delete chart:", err);
        }
    };

    const handleStartEdit = (chartConfig) => {
        setEditingChart(chartConfig);
    };

    const handleBackToDashboards = () => {
        window.location.href = "/my-dashboards";
    };

    if (!isAuthenticated) {
        return (
            <div className={styles.pageContainer}>
                <div className={styles.error}>
                    Для просмотра этой страницы необходимо авторизоваться.
                    <div className={styles.actions}>
                        <button onClick={handleBackToDashboards} className={styles.backButton}>
                            ← Назад к списку дашбордов
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    if (isLoading) {
        return (
            <div className={styles.pageContainer}>
                <div className={styles.loading}>Загрузка страницы...</div>
            </div>
        );
    }

    if (error || !page) {
        return (
            <div className={styles.pageContainer}>
                <div className={styles.error}>
                    {error || "Страница не найдена"}
                    <div className={styles.actions}>
                        <button onClick={handleBackToDashboards} className={styles.backButton}>
                            ← Назад к списку дашбордов
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    return (
        <div className={styles.pageContainer}>
            {/* Заголовок страницы */}
            <div className={styles.pageHeader}>
                <div className={styles.headerContent}>
                    <h1 className={styles.pageTitle}>{page.name}</h1>
                    <div className={styles.pageMeta}>
                        <span className={styles.chartsCount}>
                            Графиков: {page.charts?.length || 0}
                        </span>
                        <span className={styles.pageStatus}>
                            Статус: {page.charts?.length > 0 ? "Настроена" : "Пустая"}
                        </span>
                    </div>
                </div>
                <button
                    className={styles.addChartButton}
                    onClick={() => setShowAddModal(true)}
                >
                    + Добавить график
                </button>
            </div>

            {/* Контент страницы */}
            <div className={styles.pageContent}>
                {page.charts && page.charts.length > 0 ? (
                    <div className={styles.chartsSection}>
                        {page.charts.map((chart) => (
                            <ChartRenderer
                                key={chart.id}
                                chartConfig={chart}
                                onDelete={handleDeleteChart}
                                onEdit={handleStartEdit}
                                productTree={productTree}
                            />
                        ))}
                    </div>
                ) : (
                    <div className={styles.emptyState}>
                        <div className={styles.emptyContent}>
                            <h3>Страница пустая</h3>
                            <p>На этой странице пока нет настроенных графиков.</p>
                            <button
                                className={styles.addFirstChartButton}
                                onClick={() => setShowAddModal(true)}
                            >
                                + Добавить первый график
                            </button>
                        </div>
                    </div>
                )}
            </div>

            {/* Действия */}
            <div className={styles.actionsSection}>
                <button onClick={handleBackToDashboards} className={styles.backButton}>
                    ← Назад к списку дашбордов
                </button>
            </div>

            {/* Модальное окно добавления графика */}
            {showAddModal && (
                <AddChartModal
                    isOpen={showAddModal}
                    onClose={() => setShowAddModal(false)}
                    onSave={handleAddChart}
                    productTree={productTree}
                    token={token}
                />
            )}

            {/* Модальное окно редактирования графика */}
            {editingChart && (
                <AddChartModal
                    isOpen={true}
                    onClose={() => setEditingChart(null)}
                    onSave={handleEditChart}
                    productTree={productTree}
                    token={token}
                    editingChart={editingChart}
                />
            )}
        </div>
    );
};

export default DashboardPage;