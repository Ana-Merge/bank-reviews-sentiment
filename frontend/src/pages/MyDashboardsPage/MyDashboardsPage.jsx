import { useState } from "react";
import { useAppSelector } from "../../hooks/redux";
import styles from "./MyDashboardsPage.module.scss";
import { authService } from "../../services/auth";

const MyDashboardsPage = () => {
  const { isAuthenticated, user, token } = useAppSelector(state => state.auth);
  const [isLoading, setIsLoading] = useState(false);
  const [response, setResponse] = useState(null);
  const [error, setError] = useState(null);

  const handleTestRequest = async () => {
    if (!token) {
      setError("Токен отсутствует");
      return;
    }

    setIsLoading(true);
    setError(null);
    setResponse(null);

    try {
      const config = await authService.getUserDashboardsConfig(token);
      setResponse(config);
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  };

  if (!isAuthenticated) {
    return (
      <div className={styles.container}>
        <h1>Мои дашборды</h1>
        <p>Для доступа к персонализированным дашбордам необходимо авторизоваться.</p>
      </div>
    );
  }

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h1>Мои дашборды</h1>
        <div className={styles.userWelcome}>
          Добро пожаловать, <strong>{user?.username}</strong>!
        </div>
      </div>

      <div className={styles.content}>
        <p>
          На этой странице будет список ваших персонализированных дашбордов.
          <br />
          Вы сможете создавать, редактировать и удалять их по мере необходимости.
        </p>

        <div className={styles.testSection}>
          <button
            onClick={handleTestRequest}
            disabled={isLoading}
          >
            {isLoading ? "Загрузка..." : "Тест API запроса"}
          </button>

          {response && (
            <div className={styles.response}>
              <h4>Ответ от API:</h4>
              <pre>{JSON.stringify(response, null, 2)}</pre>
            </div>
          )}

          {error && (
            <div className={styles.error}>
              <strong>Ошибка:</strong> {error}
            </div>
          )}
        </div>

        <div className={styles.placeholder}>
          <p>Здесь будут отображаться ваши сохраненные дашборды</p>
        </div>
      </div>
    </div>
  );
};

export default MyDashboardsPage;