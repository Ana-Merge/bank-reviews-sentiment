import { useState, useEffect, useCallback } from "react";
import { useParams } from "react-router-dom";
import { useAppSelector, useAppDispatch } from "../../hooks/redux";
import { authService } from "../../services/auth";
import { fetchProductTree } from "../../store/slices/productSlice";
import { AddChartModal, ChartRenderer, LoadingSpinner } from "../../components";
import styles from "./DashboardPage.module.scss";

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
        if (!productTree) {
            dispatch(fetchProductTree());
        }
    }, [dispatch, productTree]);

    const loadPage = useCallback(async () => {
        if (!isAuthenticated || !token || !pageId) return;

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
        } finally {
            setIsLoading(false);
        }
    }, [isAuthenticated, token, pageId]);

    useEffect(() => {
        loadPage();
    }, [loadPage]);

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
                <div className={styles.loading}><LoadingSpinner /></div>
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
                                isEditable={true}
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

            <div className={styles.actionsSection}>
                <button onClick={handleBackToDashboards} className={styles.backButton}>
                    ← Назад к списку дашбордов
                </button>
            </div>

            {showAddModal && (
                <AddChartModal
                    isOpen={showAddModal}
                    onClose={() => setShowAddModal(false)}
                    onSave={handleAddChart}
                    productTree={productTree}
                    token={token}
                />
            )}

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