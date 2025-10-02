import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { useAppSelector, useAppDispatch } from "../../hooks/redux";
import { authService } from "../../services/auth";
import { fetchProductTree } from "../../store/slices/productSlice";
import { usePageOperations } from "../../hooks/usePageOperations";
import { LoadingSpinner, ChartRenderer } from "../../components";
import styles from "./UserDashboardPage.module.scss";

const UserDashboardPage = () => {
    const { userId, pageId } = useParams();
    const dispatch = useAppDispatch();
    const { isAuthenticated, token } = useAppSelector(state => state.auth);
    const { productTree } = useAppSelector(state => state.product);
    const { isSaving, error, savePageToMyDashboards, setError } = usePageOperations(token);

    const [user, setUser] = useState(null);
    const [page, setPage] = useState(null);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        if (isAuthenticated && token && userId && pageId) {
            loadPage();
        }
        if (!productTree) {
            dispatch(fetchProductTree());
        }
    }, [isAuthenticated, token, userId, pageId, dispatch, productTree]);

    const loadPage = async () => {
        setIsLoading(true);
        setError(null);

        try {
            const usersData = await authService.getAllUsers(token);
            const foundUser = usersData.users.find(u => u.id.toString() === userId);

            if (foundUser) {
                setUser(foundUser);
                const foundPage = foundUser.dashboard_config?.pages?.find(p => p.id === pageId);

                if (foundPage) {
                    setPage(foundPage);
                } else {
                    setError("Страница не найдена");
                }
            } else {
                setError("Пользователь не найден");
            }
        } catch (err) {
            setError(`Ошибка загрузки страницы: ${err.message}`);
            console.error("Failed to load page:", err);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSavePage = async () => {
        const success = await savePageToMyDashboards(page, user.username);
        if (success) {
            alert(`Страница "${page.name}" успешно сохранена!`);
        }
    };

    const handleBackToUser = () => {
        window.location.href = `/user-dashboards/${userId}`;
    };

    if (!isAuthenticated) {
        return (
            <div className={styles.pageContainer}>
                <div className={styles.error}>
                    Для просмотра этой страницы необходимо авторизоваться.
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

    if (error || !page || !user) {
        return (
            <div className={styles.pageContainer}>
                <div className={styles.error}>
                    {error || "Страница не найдена"}
                    <div className={styles.actions}>
                        <button onClick={handleBackToUser} className={styles.backButton}>
                            ← Назад к страницам пользователя
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
                        <span className={styles.userInfo}>
                            Владелец: {user.username}
                        </span>
                        <span className={styles.chartsCount}>
                            Графиков: {page.charts?.length || 0}
                        </span>
                    </div>
                </div>
                <div className={styles.pageActions}>
                    <button
                        className={styles.saveButton}
                        onClick={handleSavePage}
                        disabled={isSaving}
                    >
                        {isSaving ? "Сохранение..." : "Сохранить к себе"}
                    </button>
                    <button
                        className={styles.backButton}
                        onClick={handleBackToUser}
                    >
                        ← Назад
                    </button>
                </div>
            </div>

            <div className={styles.pageContent}>
                {page.charts && page.charts.length > 0 ? (
                    <div className={styles.chartsSection}>
                        {page.charts.map((chart) => (
                            <ChartRenderer
                                key={chart.id}
                                chartConfig={chart}
                                productTree={productTree}
                                isEditable={false}
                            />
                        ))}
                    </div>
                ) : (
                    <div className={styles.emptyState}>
                        <div className={styles.emptyContent}>
                            <h3>Страница пустая</h3>
                            <p>На этой странице нет настроенных графиков.</p>
                        </div>
                    </div>
                )}
            </div>

            <div className={styles.viewModeInfo}>
                <div className={styles.infoBanner}>
                    <strong>Режим просмотра</strong> - вы можете просматривать графики, но не можете их редактировать.
                    Нажмите "Сохранить к себе", чтобы добавить эту страницу в свои дашборды.
                </div>
            </div>
        </div>
    );
};

export default UserDashboardPage;