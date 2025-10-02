import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { apiService } from '../../services/api';

export const fetchProductStats = createAsyncThunk(
    'chart/fetchProductStats',
    async ({ startDate, endDate, startDate2, endDate2, selectedProduct, categoryId, source }) => {
        let statsData = [];
        if (selectedProduct?.children?.length > 0) {
            const productIds = selectedProduct.children.map(child => child.id);
            const allStatsPromises = productIds.map(id =>
                apiService.getProductStats(startDate, endDate, startDate2, endDate2, id, selectedProduct.id, source)
            );
            const allStatsResults = await Promise.all(allStatsPromises);
            statsData = allStatsResults.flat();
        } else {
            const productId = selectedProduct?.id;
            statsData = await apiService.getProductStats(
                startDate,
                endDate,
                startDate2,
                endDate2,
                productId,
                categoryId,
                source
            );
        }
        return statsData;
    }
);

export const fetchBarChartData = createAsyncThunk(
    'chart/fetchBarChartData',
    async ({ productId, startDate, endDate, startDate2, endDate2, aggregationType, source }) => {
        const chartData = await apiService.getBarChartChanges(
            productId,
            startDate,
            endDate,
            startDate2,
            endDate2,
            aggregationType,
            source
        );
        return { ...chartData, changes: chartData.changes || [] };
    }
);

export const fetchChangeChartData = createAsyncThunk(
    'chart/fetchChangeChartData',
    async ({ productId, startDate, endDate, startDate2, endDate2, source }) => {
        const chartData = await apiService.getChangeChart(
            productId,
            startDate,
            endDate,
            startDate2,
            endDate2,
            source
        );
        return chartData;
    }
);

export const fetchTonalityChartData = createAsyncThunk(
    'chart/fetchTonalityChartData',
    async ({ productId, startDate, endDate, startDate2, endDate2, aggregationType, source }) => {
        const chartData = await apiService.getReviewTonality(
            productId,
            startDate,
            endDate,
            startDate2,
            endDate2,
            aggregationType,
            source
        );
        return chartData;
    }
);

const chartSlice = createSlice({
    name: 'chart',
    initialState: {
        productStats: null,
        barChartData: null,
        changeChartData: null,
        tonalityChartData: null,
        isLoadingProduct: false,
        isLoadingChart: false,
        isLoadingChangeChart: false,
        isLoadingTonalityChart: false,
        errorProduct: null,
        errorChart: null,
        errorChangeChart: null,
        errorTonalityChart: null,
    },
    reducers: {
        clearChartData: (state) => {
            state.productStats = null;
            state.barChartData = null;
            state.changeChartData = null;
            state.tonalityChartData = null;
        },
        clearErrors: (state) => {
            state.errorProduct = null;
            state.errorChart = null;
            state.errorChangeChart = null;
            state.errorTonalityChart = null;
        },
    },
    extraReducers: (builder) => {
        builder
            // Product Stats
            .addCase(fetchProductStats.pending, (state) => {
                state.isLoadingProduct = true;
                state.errorProduct = null;
            })
            .addCase(fetchProductStats.fulfilled, (state, action) => {
                state.isLoadingProduct = false;
                state.productStats = action.payload;
            })
            .addCase(fetchProductStats.rejected, (state, action) => {
                state.isLoadingProduct = false;
                state.errorProduct = action.error.message;
            })
            // Bar Chart
            .addCase(fetchBarChartData.pending, (state) => {
                state.isLoadingChart = true;
                state.errorChart = null;
            })
            .addCase(fetchBarChartData.fulfilled, (state, action) => {
                state.isLoadingChart = false;
                state.barChartData = action.payload;
            })
            .addCase(fetchBarChartData.rejected, (state, action) => {
                state.isLoadingChart = false;
                state.errorChart = action.error.message;
            })
            // Change Chart
            .addCase(fetchChangeChartData.pending, (state) => {
                state.isLoadingChangeChart = true;
                state.errorChangeChart = null;
            })
            .addCase(fetchChangeChartData.fulfilled, (state, action) => {
                state.isLoadingChangeChart = false;
                state.changeChartData = action.payload;
            })
            .addCase(fetchChangeChartData.rejected, (state, action) => {
                state.isLoadingChangeChart = false;
                state.errorChangeChart = action.error.message;
            })
            // Tonality Chart
            .addCase(fetchTonalityChartData.pending, (state) => {
                state.isLoadingTonalityChart = true;
                state.errorTonalityChart = null;
            })
            .addCase(fetchTonalityChartData.fulfilled, (state, action) => {
                state.isLoadingTonalityChart = false;
                state.tonalityChartData = action.payload;
            })
            .addCase(fetchTonalityChartData.rejected, (state, action) => {
                state.isLoadingTonalityChart = false;
                state.errorTonalityChart = action.error.message;
            });
    },
});

export const { clearChartData, clearErrors } = chartSlice.actions;
export default chartSlice.reducer;