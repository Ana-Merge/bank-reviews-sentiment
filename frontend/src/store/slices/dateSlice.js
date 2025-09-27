import { createSlice } from '@reduxjs/toolkit';

const dateSlice = createSlice({
    name: 'date',
    initialState: {
        startDate: "2025-03-01",
        endDate: "2025-05-31",
        startDate2: "2024-12-01",
        endDate2: "2025-02-28",
        aggregationType: "month",
        dateErrors: {},
    },
    reducers: {
        setStartDate: (state, action) => {
            state.startDate = action.payload;
        },
        setEndDate: (state, action) => {
            state.endDate = action.payload;
        },
        setStartDate2: (state, action) => {
            state.startDate2 = action.payload;
        },
        setEndDate2: (state, action) => {
            state.endDate2 = action.payload;
        },
        setAggregationType: (state, action) => {
            state.aggregationType = action.payload;
        },
        setDateErrors: (state, action) => {
            state.dateErrors = action.payload;
        },
        clearDateErrors: (state) => {
            state.dateErrors = {};
        },
    },
});

export const {
    setStartDate,
    setEndDate,
    setStartDate2,
    setEndDate2,
    setAggregationType,
    setDateErrors,
    clearDateErrors,
} = dateSlice.actions;

export default dateSlice.reducer;