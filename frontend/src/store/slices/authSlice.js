import { createSlice, createAsyncThunk } from '@reduxjs/toolkit';
import { authService } from '../../services/auth';

const loadUserFromStorage = () => {
    const token = localStorage.getItem('token');
    const username = localStorage.getItem('username');
    if (token && username) {
        return { username, token };
    }
    return null;
};

export const registerUser = createAsyncThunk(
    'auth/register',
    async ({ username, password, role }) => {
        const response = await authService.register(username, password, role);
        return response;
    }
);

export const loginUser = createAsyncThunk(
    'auth/login',
    async ({ username, password }) => {
        const response = await authService.login(username, password);
        return { ...response, username };
    }
);

const authSlice = createSlice({
    name: 'auth',
    initialState: {
        user: loadUserFromStorage() ? { username: localStorage.getItem('username') } : null,
        token: localStorage.getItem('token'),
        isAuthenticated: !!localStorage.getItem('token'),
        isLoading: false,
        error: null,
        sessionExpired: false,
    },
    reducers: {
        logout: (state) => {
            state.user = null;
            state.token = null;
            state.isAuthenticated = false;
            state.sessionExpired = false;
            localStorage.removeItem('token');
            localStorage.removeItem('username');
        },
        clearError: (state) => {
            state.error = null;
        },
        setSessionExpired: (state) => {
            state.sessionExpired = true;
            state.isAuthenticated = false;
            state.user = null;
            state.token = null;
            localStorage.removeItem('token');
            localStorage.removeItem('username');
        },
        clearSessionExpired: (state) => {
            state.sessionExpired = false;
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(registerUser.pending, (state) => {
                state.isLoading = true;
                state.error = null;
            })
            .addCase(registerUser.fulfilled, (state, action) => {
                state.isLoading = false;
                localStorage.setItem('username', action.payload.username);
            })
            .addCase(registerUser.rejected, (state, action) => {
                state.isLoading = false;
                state.error = action.error.message;
            })
            .addCase(loginUser.pending, (state) => {
                state.isLoading = true;
                state.error = null;
                state.sessionExpired = false;
            })
            .addCase(loginUser.fulfilled, (state, action) => {
                state.isLoading = false;
                state.user = { username: action.payload.username };
                state.token = action.payload.access_token;
                state.isAuthenticated = true;
                state.sessionExpired = false;
                localStorage.setItem('token', action.payload.access_token);
                localStorage.setItem('username', action.payload.username);
            })
            .addCase(loginUser.rejected, (state, action) => {
                state.isLoading = false;
                state.error = action.error.message;
                state.sessionExpired = false;
            });
    },
});

export const { logout, clearError, setSessionExpired, clearSessionExpired } = authSlice.actions;
export default authSlice.reducer;