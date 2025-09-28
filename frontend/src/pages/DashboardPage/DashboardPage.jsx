import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { useAppSelector } from "../../hooks/redux";
import { authService } from "../../services/auth";
import styles from "./DashboardPage.module.scss";

const DashboardPage = () => {
    const { pageId } = useParams();
    const { isAuthenticated, token } = useAppSelector(state => state.auth);

    const [page, setPage] = useState(null);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (isAuthenticated && token && pageId) {
            loadPage();
        }
    }, [isAuthenticated, token, pageId]);

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
            </div>

            {/* Контент страницы */}
            <div className={styles.pageContent}>
                {page.charts && page.charts.length > 0 ? (
                    <div className={styles.chartsSection}>
                        <div className={styles.sectionHeader}>
                            <h3>Настроенные графики</h3>
                        </div>
                        <div className={styles.chartsList}>
                            {page.charts.map((chart, index) => (
                                <div key={chart.id || index} className={styles.chartCard}>
                                    <h4 className={styles.chartName}>{chart.name}</h4>
                                    <div className={styles.chartInfo}>
                                        <span className={styles.chartType}>Тип: {chart.type}</span>
                                        {chart.attributes?.product_id && (
                                            <span className={styles.chartProduct}>
                                                Продукт: {chart.attributes.product_id}
                                            </span>
                                        )}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                ) : (
                    <div className={styles.emptyState}>
                        <div className={styles.emptyContent}>
                            <h3>Страница пустая</h3>
                            <p>На этой странице пока нет настроенных графиков.</p>
                            <p>Добавьте графики через редактор дашбордов на главной странице "Мои дашборды".</p>
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
        </div>
    );
};

export default DashboardPage;