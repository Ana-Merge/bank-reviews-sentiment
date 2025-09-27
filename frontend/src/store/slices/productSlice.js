import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { apiService } from '../../services/api';

export const fetchProductTree = createAsyncThunk(
    'product/fetchProductTree',
    async () => {
        const response = await apiService.getProductTree();
        return response;
    }
);

const productSlice = createSlice({
    name: 'product',
    initialState: {
        productTree: null,
        selectedProduct: null,
        categoryId: null,
        isLoadingTree: false,
        errorProduct: null,
    },
    reducers: {
        setSelectedProduct: (state, action) => {
            state.selectedProduct = action.payload;
        },
        setCategoryId: (state, action) => {
            state.categoryId = action.payload;
        },
        clearError: (state) => {
            state.errorProduct = null;
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchProductTree.pending, (state) => {
                state.isLoadingTree = true;
                state.errorProduct = null;
            })
            .addCase(fetchProductTree.fulfilled, (state, action) => {
                state.isLoadingTree = false;
                state.productTree = action.payload;
            })
            .addCase(fetchProductTree.rejected, (state, action) => {
                state.isLoadingTree = false;
                state.errorProduct = action.error.message;
            });
    },
});

export const { setSelectedProduct, setCategoryId, clearError } = productSlice.actions;
export default productSlice.reducer;