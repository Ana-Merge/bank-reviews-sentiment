import { configureStore } from '@reduxjs/toolkit';
import productSlice from './slices/productSlice';
import dateSlice from './slices/dateSlice';
import chartSlice from './slices/chartSlice';
import authSlice from './slices/authSlice';

export const store = configureStore({
    reducer: {
        product: productSlice,
        date: dateSlice,
        chart: chartSlice,
        auth: authSlice,
    },
});

export default store;