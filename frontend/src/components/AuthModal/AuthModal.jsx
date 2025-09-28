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
    }, [activeTab, dispatch]);

    const shouldShowModal = isOpen || sessionExpired;

    if (!shouldShowModal) return null;

    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData(prev => ({
            ...prev,
            [name]: value
        }));
    };

    const handleSubmit = (e) => {
        e.preventDefault();

        if (activeTab === "register") {
            if (formData.password !== formData.confirmPassword) {
                dispatch(clearError());
                return;
            }
            dispatch(registerUser({
                username: formData.username,
                password: formData.password,
                role: "manager"
            })).then((result) => {
                if (result.meta.requestStatus === 'fulfilled') {
                    setActiveTab('login');
                    setFormData({
                        username: formData.username,
                        password: "",
                        confirmPassword: ""
                    });
                }
            });
        } else {
            dispatch(loginUser({
                username: formData.username,
                password: formData.password
            }));
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
    };

    const handleClose = () => {
        if (sessionExpired) {
            dispatch(clearSessionExpired());
        }
        onClose();
        dispatch(clearError());
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
                        />
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
                        />
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
                            />
                            {formData.password !== formData.confirmPassword && formData.confirmPassword && (
                                <span className={styles.passwordError}>Пароли не совпадают</span>
                            )}
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
                        disabled={isLoading || (activeTab === "register" && formData.password !== formData.confirmPassword)}
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