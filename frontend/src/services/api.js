const API_BASE_URL = "http://158.160.53.51:80/api";

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
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error("API request failed:", error);
    throw error;
  }
};

const formatDateForApi = (dateString, type) =>
  type === "month" ? dateString.substring(0, 7) : dateString;

export const apiService = {
  async getProductStats(startDate, endDate, startDate2, endDate2, productId = null, categoryId = null, source = null) {
    let endpoint = `/v1/dashboards/product-stats?start_date=${startDate}&end_date=${endDate}&start_date2=${startDate2}&end_date2=${endDate2}`;
    if (productId) endpoint += `&product_id=${productId}`;
    if (categoryId) endpoint += `&category_id=${categoryId}`;
    if (source) endpoint += `&source=${encodeURIComponent(source)}`;
    return request(endpoint);
  },

  async getProductTree() {
    return request("/v1/dashboards/public-product-tree");
  },

  async getBarChartChanges(productId, startDate, endDate, startDate2, endDate2, aggregationType, source = null) {
    let endpoint = `/v1/dashboards/bar_chart_changes?product_id=${productId}&start_date=${formatDateForApi(startDate, aggregationType)}&end_date=${formatDateForApi(endDate, aggregationType)}&start_date2=${formatDateForApi(startDate2, aggregationType)}&end_date2=${formatDateForApi(endDate2, aggregationType)}&aggregation_type=${aggregationType}`;
    if (source) endpoint += `&source=${encodeURIComponent(source)}`;
    const data = await request(endpoint);
    return { ...data, changes: data.changes || [] };
  },

  async getChangeChart(productId, startDate, endDate, startDate2, endDate2, source = null) {
    let endpoint = `/v1/dashboards/change-chart?product_id=${productId}&start_date=${startDate}&end_date=${endDate}&start_date2=${startDate2}&end_date2=${endDate2}`;
    if (source) endpoint += `&source=${encodeURIComponent(source)}`;
    return request(endpoint);
  },

  async getReviewTonality(productId, startDate, endDate, startDate2, endDate2, aggregationType, source = null) {
    let endpoint = `/v1/dashboards/monthly-review-count?product_id=${productId}&start_date=${formatDateForApi(startDate, aggregationType)}&end_date=${formatDateForApi(endDate, aggregationType)}&start_date2=${formatDateForApi(startDate2, aggregationType)}&end_date2=${formatDateForApi(endDate2, aggregationType)}&aggregation_type=${aggregationType}`;
    if (source) endpoint += `&source=${encodeURIComponent(source)}`;
    return request(endpoint);
  },

  async getReviews(productId, startDate = null, endDate = null, source = null, sentiment = null, orderBy = 'asc', page = 0, size = 30) {
    let endpoint = `/v1/dashboards/reviews?product_id=${productId}&page=${page}&size=${size}&order_by=${orderBy}`;
    if (startDate) endpoint += `&start_date=${startDate}`;
    if (endDate) endpoint += `&end_date=${endDate}`;
    if (source) endpoint += `&source=${encodeURIComponent(source)}`;
    if (sentiment) endpoint += `&sentiment=${sentiment}`;
    return request(endpoint);
  },
};