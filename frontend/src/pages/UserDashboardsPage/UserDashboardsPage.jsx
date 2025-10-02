import { useState, useEffect } from "react";
import { useParams } from "react-router-dom";
import { useAppSelector } from "../../hooks/redux";
import { authService } from "../../services/auth";
import { usePageOperations } from "../../hooks/usePageOperations";
import styles from "./UserDashboardsPage.module.scss";

const UserDashboardsPage = () => {
    const { userId } = useParams();
    const { isAuthenticated, token } = useAppSelector(state => state.auth);
    const { isSaving, error, savePageToMyDashboards, setError } = usePageOperations(token);

    const [user, setUser] = useState(null);
    const [isLoading, setIsLoading] = useState(true);

    useEffect(() => {
        if (isAuthenticated && token && userId) {
            loadUserData();
        }
    }, [isAuthenticated, token, userId]);

    const loadUserData = async () => {
        setIsLoading(true);
        setError(null);

        try {
            const usersData = await authService.getAllUsers(token);
            const foundUser = usersData.users.find(u => u.id.toString() === userId);

            if (foundUser) {
                setUser(foundUser);
            } else {
                setError("Пользователь не найден");
            }
        } catch (err) {
            setError(`Ошибка загрузки данных пользователя: ${err.message}`);
            console.error("Failed to load user data:", err);
        } finally {
            setIsLoading(false);
        }
    };

    const handleSavePage = async (page) => {
        const success = await savePageToMyDashboards(page, user.username);
        if (success) {
            alert(`Страница "${page.name}" успешно сохранена!`);
        }
    };

    const handlePageClick = (pageId) => {
        window.open(`/user-dashboard/${userId}/${pageId}`, '_blank');
    };

    const handleBackToUsers = () => {
        window.history.back();
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
                <div className={styles.loading}>Загрузка данных пользователя...</div>
            </div>
        );
    }

    if (error || !user) {
        return (
            <div className={styles.pageContainer}>
                <div className={styles.error}>
                    {error || "Пользователь не найден"}
                    <div className={styles.actions}>
                        <button onClick={handleBackToUsers} className={styles.backButton}>
                            ← Назад к списку пользователей
                        </button>
                    </div>
                </div>
            </div>
        );
    }

    const userPages = user.dashboard_config?.pages || [];

    return (
        <div className={styles.pageContainer}>
            {/* Заголовок страницы */}
            <div className={styles.pageHeader}>
                <div className={styles.headerContent}>
                    <h1 className={styles.pageTitle}>Страницы пользователя: {user.username}</h1>
                    <div className={styles.pageMeta}>
                        <span className={styles.pagesCount}>
                            Страниц: {userPages.length}
                        </span>
                    </div>
                </div>
            </div>

            {/* Контент страницы */}
            <div className={styles.pageContent}>
                {userPages.length === 0 ? (
                    <div className={styles.emptyState}>
                        <div className={styles.emptyContent}>
                            <h3>У пользователя нет страниц</h3>
                            <p>У этого пользователя пока нет настроенных страниц дашбордов.</p>
                        </div>
                    </div>
                ) : (
                    <div className={styles.pagesGrid}>
                        {userPages.map((page) => (
                            <div key={page.id} className={styles.pageCard}>
                                <div
                                    className={styles.pageContent}
                                    onClick={() => handlePageClick(page.id)}
                                >
                                    <h4 className={styles.pageName}>{page.name}</h4>
                                    <div className={styles.pageInfo}>
                                        <span className={styles.chartsCount}>
                                            Графиков: {page.charts?.length || 0}
                                        </span>
                                    </div>
                                </div>
                                <div className={styles.pageActions}>
                                    <button
                                        onClick={() => handlePageClick(page.id)}
                                        className={styles.openButton}
                                        title="Открыть в новой вкладке"
                                    >
                                        Открыть
                                    </button>
                                    <button
                                        onClick={() => handleSavePage(page)}
                                        className={styles.saveButton}
                                        disabled={isSaving}
                                        title="Сохранить страницу к себе"
                                    >
                                        {isSaving ? "Сохранение..." : "Сохранить"}
                                    </button>
                                </div>
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
};

export default UserDashboardsPage;