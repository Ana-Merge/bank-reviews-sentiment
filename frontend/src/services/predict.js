const API_BASE_URL = "http://158.160.25.202:8005/api";

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
            const errorData = await response.json().catch(() => null);
            throw new Error(errorData?.detail || `HTTP error! status: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error("Predict API request failed:", error);
        throw error;
    }
};

export const predictService = {
    async predict(data) {
        return request("/v1/dashboards/predict", {
            method: "POST",
            body: JSON.stringify({ data })
        });
    },
};