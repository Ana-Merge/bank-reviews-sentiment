const API_BASE_URL = "http://localhost:80/api";

const request = async (endpoint, options = {}) => {
    const url = `${API_BASE_URL}${endpoint}`;

    const headers = {
        "Content-Type": "application/json",
        ...options.headers,
    };

    const config = { ...options, headers };

    try {
        const response = await fetch(url, config);

        if (!response.ok) {
            if (response.status === 401) {
                // Токен истек или невалидный
                localStorage.removeItem('token');
                localStorage.removeItem('username');

                // Событие для уведомления о истечении сессии
                const sessionExpiredEvent = new Event('sessionExpired');
                window.dispatchEvent(sessionExpiredEvent);

                const errorData = await response.json().catch(() => null);
                throw new Error(errorData?.detail || 'Сессия истекла. Пожалуйста, войдите снова.');
            }

            const errorData = await response.json().catch(() => null);
            throw new Error(errorData?.detail || `HTTP error! status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error("API request failed:", error);
        throw error;
    }
};

export const authService = {
    async register(username, password, role = "manager") {
        return request("/v1/auth/register", {
            method: "POST",
            body: JSON.stringify({ username, password, role }),
        });
    },

    async login(username, password) {
        return request("/v1/auth/login-json", {
            method: "POST",
            body: JSON.stringify({ username, password }),
        });
    },

    async getUserDashboardsConfig(token) {
        return request("/v1/user_dashboards/config", {
            headers: {
                "Authorization": `Bearer ${token}`,
            },
        });
    },

    async createDashboardPage(token, pageData) {
        return request("/v1/user_dashboards/pages", {
            method: "POST",
            headers: {
                "Authorization": `Bearer ${token}`,
            },
            body: JSON.stringify(pageData),
        });
    },

    async deleteDashboardPage(token, pageId) {
        return request(`/v1/user_dashboards/pages/${pageId}`, {
            method: "DELETE",
            headers: {
                "Authorization": `Bearer ${token}`,
            },
        });
    },

    async saveUserDashboardsConfig(token, configData) {
        return request("/v1/user_dashboards/config", {
            method: "POST",
            headers: {
                "Authorization": `Bearer ${token}`,
            },
            body: JSON.stringify(configData),
        });
    },

    async updateDashboardPage(token, pageId, pageData) {
        return request(`/v1/user_dashboards/pages/${pageId}`, {
            method: "PUT",
            headers: {
                "Authorization": `Bearer ${token}`,
            },
            body: JSON.stringify(pageData),
        });
    },

    // Новый метод для получения всех пользователей
    async getAllUsers(token) {
        return request("/v1/user_dashboards/users", {
            headers: {
                "Authorization": `Bearer ${token}`,
            },
        });
    },
};