import { configureStore } from '@reduxjs/toolkit';
import productSlice from './slices/productSlice';
import dateSlice from './slices/dateSlice';
import chartSlice from './slices/chartSlice';

export const store = configureStore({
    reducer: {
        product: productSlice,
        date: dateSlice,
        chart: chartSlice,
    },
});

export default store;