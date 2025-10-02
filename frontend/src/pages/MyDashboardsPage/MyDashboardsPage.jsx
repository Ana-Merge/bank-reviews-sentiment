import { useState, useEffect } from "react";
import { useAppSelector } from "../../hooks/redux";
import { authService } from "../../services/auth";
import styles from "./MyDashboardsPage.module.scss";

const MyDashboardsPage = () => {
  const { isAuthenticated, user, token } = useAppSelector(state => state.auth);

  const [pages, setPages] = useState([]);
  const [users, setUsers] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const [isCreating, setIsCreating] = useState(false);
  const [newPageName, setNewPageName] = useState("");
  const [error, setError] = useState(null);
  const [successMessage, setSuccessMessage] = useState(null);
  const [activeView, setActiveView] = useState("my");
  useEffect(() => {
    if (isAuthenticated && token) {
      loadPages();
      loadUsers();
    }
  }, [isAuthenticated, token]);

  const loadPages = async () => {
    setIsLoading(true);
    setError(null);

    try {
      const config = await authService.getUserDashboardsConfig(token);
      setPages(config.pages || []);
    } catch (err) {
      setError(`Ошибка загрузки дашбордов: ${err.message}`);
      console.error("Failed to load pages:", err);
    } finally {
      setIsLoading(false);
    }
  };

  const loadUsers = async () => {
    try {
      const usersData = await authService.getAllUsers(token);
      setUsers(usersData.users || []);
    } catch (err) {
      console.error("Failed to load users:", err);
    }
  };

  const handleCreatePage = async (e) => {
    e.preventDefault();
    if (!newPageName.trim()) {
      setError("Введите название страницы");
      return;
    }

    setIsCreating(true);
    setError(null);

    try {
      const newPage = {
        id: Date.now().toString(),
        name: newPageName.trim(),
        charts: []
      };

      await authService.createDashboardPage(token, newPage);
      await loadPages();

      setNewPageName("");
      setSuccessMessage(`Страница "${newPage.name}" создана успешно!`);
      setTimeout(() => setSuccessMessage(null), 3000);
    } catch (err) {
      setError(`Ошибка создания страницы: ${err.message}`);
      console.error("Failed to create page:", err);
    } finally {
      setIsCreating(false);
    }
  };

  const handleDeletePage = async (pageId, pageName) => {
    if (!confirm(`Вы уверены, что хотите удалить страницу "${pageName}"?`)) {
      return;
    }

    try {
      await authService.deleteDashboardPage(token, pageId);
      setPages(prev => prev.filter(page => page.id !== pageId));
      setSuccessMessage(`Страница "${pageName}" удалена успешно!`);
      setTimeout(() => setSuccessMessage(null), 3000);
    } catch (err) {
      setError(`Ошибка удаления страницы: ${err.message}`);
      console.error("Failed to delete page:", err);
    }
  };

  const handlePageClick = (pageId) => {
    window.open(`/dashboard/${pageId}`, '_blank');
  };

  const handleUserClick = (userId) => {
    window.open(`/user-dashboards/${userId}`, '_blank');
  };

  if (!isAuthenticated) {
    return (
      <div className={styles.pageContainer}>
        <div className={styles.error}>
          Для доступа к персонализированным дашбордам необходимо авторизоваться.
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className={styles.pageContainer}>
        <div className={styles.loading}>Загрузка дашбордов...</div>
      </div>
    );
  }

  return (
    <div className={styles.pageContainer}>
      <div className={styles.filtersContainer}>
        <div className={styles.filtersHeader}>
          <h3>Создать новую страницу</h3>
        </div>
        <div className={styles.filtersContent}>
          <form onSubmit={handleCreatePage} className={styles.createForm}>
            <div className={styles.filterGroup}>
              <label htmlFor="page-name">Название страницы:</label>
              <div className={styles.createInputGroup}>
                <input
                  id="page-name"
                  type="text"
                  value={newPageName}
                  onChange={(e) => setNewPageName(e.target.value)}
                  placeholder="Введите название страницы"
                  className={styles.nameInput}
                  disabled={isCreating}
                />
                <button
                  type="submit"
                  className={styles.createButton}
                  disabled={isCreating || !newPageName.trim()}
                >
                  {isCreating ? "Создание..." : "Создать страницу"}
                </button>
              </div>
            </div>
          </form>
        </div>
      </div>

      {error && (
        <div className={styles.error}>
          {error}
          <button
            onClick={() => setError(null)}
            className={styles.closeMessage}
          >
            ×
          </button>
        </div>
      )}

      {successMessage && (
        <div className={styles.successMessage}>
          {successMessage}
          <button
            onClick={() => setSuccessMessage(null)}
            className={styles.closeMessage}
          >
            ×
          </button>
        </div>
      )}

      {/* Переключение между видами */}
      <div className={styles.viewTabs}>
        <button
          className={`${styles.viewTab} ${activeView === "my" ? styles.active : ""}`}
          onClick={() => setActiveView("my")}
        >
          Мои страницы
        </button>
        <button
          className={`${styles.viewTab} ${activeView === "users" ? styles.active : ""}`}
          onClick={() => setActiveView("users")}
        >
          Страницы других пользователей
        </button>
      </div>

      {activeView === "my" ? (
        <div className={styles.pagesContainer}>
          <div className={styles.sectionHeader}>
            <h3>Мои страницы дашбордов</h3>
            <span className={styles.pagesCount}>Всего: {pages.length}</span>
          </div>

          {pages.length === 0 ? (
            <div className={styles.noData}>
              У вас пока нет созданных страниц. Создайте первую страницу выше.
            </div>
          ) : (
            <div className={styles.pagesGrid}>
              {pages.map((page) => (
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
                      onClick={() => handleDeletePage(page.id, page.name)}
                      className={styles.deleteButton}
                      title="Удалить страницу"
                    >
                      Удалить
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      ) : (
        <div className={styles.usersContainer}>
          <div className={styles.sectionHeader}>
            <h3>Пользователи системы</h3>
            <span className={styles.usersCount}>Всего: {users.length}</span>
          </div>

          {users.length === 0 ? (
            <div className={styles.noData}>
              Нет других пользователей в системе.
            </div>
          ) : (
            <div className={styles.usersGrid}>
              {users
                .filter(u => u.username !== user.username)
                .map((userItem) => (
                  <div key={userItem.id} className={styles.userCard}>
                    <div className={styles.userContent}>
                      <h4 className={styles.userName}>{userItem.username}</h4>
                      <div className={styles.userInfo}>
                        <span className={styles.pagesCount}>
                          Страниц: {userItem.dashboard_config?.pages?.length || 0}
                        </span>
                      </div>
                    </div>
                    <div className={styles.userActions}>
                      <button
                        onClick={() => handleUserClick(userItem.id)}
                        className={styles.viewButton}
                        title="Посмотреть страницы пользователя"
                      >
                        Посмотреть страницы
                      </button>
                    </div>
                  </div>
                ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default MyDashboardsPage;