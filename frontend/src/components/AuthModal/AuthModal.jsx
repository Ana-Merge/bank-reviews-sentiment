import { useState, useEffect } from "react";
import { useAppDispatch, useAppSelector } from "../../hooks/redux";
import { registerUser, loginUser, clearError, clearSessionExpired } from "../../store/slices/authSlice";
import styles from "./AuthModal.module.scss";

const AuthModal = ({ isOpen, onClose, defaultTab = "login" }) => {
    const dispatch = useAppDispatch();
    const { isLoading, error, isAuthenticated, sessionExpired } = useAppSelector(state => state.auth);

    const [activeTab, setActiveTab] = useState(defaultTab);
    const [formData, setFormData] = useState({
        username: "",
        password: "",
        confirmPassword: ""
    });
    const [formErrors, setFormErrors] = useState({});

    useEffect(() => {
        if (isAuthenticated && isOpen) {
            onClose();
        }
    }, [isAuthenticated, isOpen, onClose]);

    useEffect(() => {
        if (error) {
            dispatch(clearError());
        }
        setFormData({
            username: "",
            password: "",
            confirmPassword: ""
        });
        setFormErrors({});
    }, [activeTab, dispatch]);

    const shouldShowModal = isOpen || sessionExpired;

    if (!shouldShowModal) return null;

    const validateForm = () => {
        const errors = {};

        if (!formData.username.trim()) {
            errors.username = "Имя пользователя обязательно";
        }

        if (!formData.password) {
            errors.password = "Пароль обязателен";
        } else if (formData.password.length < 6) {
            errors.password = "Пароль должен содержать не менее 6 символов";
        }

        if (activeTab === "register") {
            if (!formData.confirmPassword) {
                errors.confirmPassword = "Подтверждение пароля обязательно";
            } else if (formData.password !== formData.confirmPassword) {
                errors.confirmPassword = "Пароли не совпадают";
            }
        }

        setFormErrors(errors);
        return Object.keys(errors).length === 0;
    };

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({
            ...prev,
            [name]: value
        }));

        if (formErrors[name]) {
            setFormErrors(prev => ({
                ...prev,
                [name]: ""
            }));
        }
    };

    const handleSubmit = (e) => {
        e.preventDefault();

        if (!validateForm()) {
            return;
        }

        if (activeTab === "register") {
            dispatch(registerUser({
                username: formData.username,
                password: formData.password,
                role: "manager"
            })).then((result) => {
                if (result.meta.requestStatus === 'fulfilled') {
                    setFormErrors(prev => ({
                        ...prev,
                        success: "Регистрация успешна! Теперь вы можете войти."
                    }));

                    setFormData(prev => ({
                        ...prev,
                        password: "",
                        confirmPassword: ""
                    }));
                } else if (result.meta.requestStatus === 'rejected') {
                    console.error("Registration failed:", result.error);
                }
            });
        } else {
            dispatch(loginUser({
                username: formData.username,
                password: formData.password
            })).then((result) => {
                if (result.meta.requestStatus === 'fulfilled') {
                }
            });
        }
    };

    const handleOverlayClick = (e) => {
        if (e.target === e.currentTarget) {
            handleClose();
        }
    };

    const handleTabChange = (tab) => {
        setActiveTab(tab);
        dispatch(clearError());
        setFormErrors({});
        setFormData({
            username: "",
            password: "",
            confirmPassword: ""
        });
    };

    const handleClose = () => {
        if (sessionExpired) {
            dispatch(clearSessionExpired());
        }
        onClose();
        dispatch(clearError());
        setFormErrors({});
    };

    const isSubmitDisabled = () => {
        if (isLoading) return true;
        if (activeTab === "register") {
            return !formData.username || !formData.password || !formData.confirmPassword || formData.password.length < 6;
        }
        return !formData.username || !formData.password;
    };

    return (
        <div className={styles.modalOverlay} onClick={handleOverlayClick}>
            <div className={styles.modalContent}>
                <button className={styles.closeButton} onClick={handleClose}>
                    ×
                </button>

                <div className={styles.tabs}>
                    <button
                        className={`${styles.tab} ${activeTab === "login" ? styles.active : ""}`}
                        onClick={() => handleTabChange("login")}
                    >
                        Вход
                    </button>
                    <button
                        className={`${styles.tab} ${activeTab === "register" ? styles.active : ""}`}
                        onClick={() => handleTabChange("register")}
                    >
                        Регистрация
                    </button>
                </div>

                <form onSubmit={handleSubmit} className={styles.form}>
                    {sessionExpired && (
                        <div className={styles.sessionExpired}>
                            ⚠️ Ваша сессия истекла. Пожалуйста, войдите снова.
                            <br />
                            <small>Вы можете закрыть это окно для продолжения работы</small>
                        </div>
                    )}

                    <div className={styles.formGroup}>
                        <label htmlFor="username">Имя пользователя:</label>
                        <input
                            type="text"
                            id="username"
                            name="username"
                            value={formData.username}
                            onChange={handleInputChange}
                            required
                            className={formErrors.username ? styles.errorInput : ''}
                        />
                        {formErrors.username && (
                            <span className={styles.fieldError}>{formErrors.username}</span>
                        )}
                    </div>

                    <div className={styles.formGroup}>
                        <label htmlFor="password">Пароль:</label>
                        <input
                            type="password"
                            id="password"
                            name="password"
                            value={formData.password}
                            onChange={handleInputChange}
                            required
                            className={formErrors.password ? styles.errorInput : ''}
                        />
                        {formErrors.password && (
                            <span className={styles.fieldError}>{formErrors.password}</span>
                        )}
                        {activeTab === "register" && formData.password && formData.password.length < 6 && (
                            <span className={styles.passwordHint}>
                                Пароль должен содержать не менее 6 символов
                            </span>
                        )}
                    </div>

                    {activeTab === "register" && (
                        <div className={styles.formGroup}>
                            <label htmlFor="confirmPassword">Подтвердите пароль:</label>
                            <input
                                type="password"
                                id="confirmPassword"
                                name="confirmPassword"
                                value={formData.confirmPassword}
                                onChange={handleInputChange}
                                required
                                className={formErrors.confirmPassword ? styles.errorInput : ''}
                            />
                            {formErrors.confirmPassword && (
                                <span className={styles.fieldError}>{formErrors.confirmPassword}</span>
                            )}
                        </div>
                    )}

                    {formErrors.success && (
                        <div className={styles.success}>
                            {formErrors.success}
                        </div>
                    )}

                    {error && !sessionExpired && (
                        <div className={styles.error}>
                            {error}
                        </div>
                    )}

                    <button
                        type="submit"
                        className={styles.submitButton}
                        disabled={isSubmitDisabled()}
                    >
                        {isLoading ? "Загрузка..." : (activeTab === "login" ? "Войти" : "Зарегистрироваться")}
                    </button>

                    {sessionExpired && (
                        <button
                            type="button"
                            className={styles.cancelButton}
                            onClick={handleClose}
                        >
                            Закрыть и продолжить как гость
                        </button>
                    )}
                </form>
            </div>
        </div>
    );
};

export default AuthModal;