import { Link, useLocation } from "react-router-dom";
import { useState, useEffect } from "react";
import { useAppDispatch, useAppSelector } from "../../../hooks/redux";
import { logout, setSessionExpired, clearSessionExpired } from "../../../store/slices/authSlice";
import { navItems } from "../../../constants/navigation";
import { AuthModal } from "../../../components";
import styles from "./Header.module.scss";
import gazpromLogo from "/gazprom-logo.png";
import uploadIcon from "../../../assets/icons/icon-upload.svg";
import downloadIcon from "../../../assets/icons/icon-download.svg";
import accountIcon from "../../../assets/icons/icon-account.svg";

const Header = () => {
  const location = useLocation();
  const dispatch = useAppDispatch();
  const { isAuthenticated, user, sessionExpired } = useAppSelector(state => state.auth);
  const [showAuthModal, setShowAuthModal] = useState(false);

  useEffect(() => {
    const handleSessionExpired = () => {
      dispatch(setSessionExpired());
      setShowAuthModal(true);
    };

    window.addEventListener('sessionExpired', handleSessionExpired);

    return () => {
      window.removeEventListener('sessionExpired', handleSessionExpired);
    };
  }, [dispatch]);

  useEffect(() => {
    if (sessionExpired && !showAuthModal) {
      setShowAuthModal(true);
    }
  }, [sessionExpired, showAuthModal]);

  const isActive = (path) => {
    return location.pathname === path ? styles.active : "";
  };

  const handleAccountClick = () => {
    if (isAuthenticated) {
      return;
    } else {
      setShowAuthModal(true);
    }
  };

  const handleLogout = () => {
    dispatch(logout());
    window.location.href = "/cards";
  };

  const handleAuthModalClose = () => {
    setShowAuthModal(false);
    if (sessionExpired) {
      dispatch(clearSessionExpired());
    }
  };

  const handleDownload = () => {
    // логика скачивания
  };

  const handlePredict = () => {
    // логика загрузки
  };

  return (
    <>
      <header className={styles.header}>
        <div className={styles.topBar}>
          <div className={styles.logoContainer}>
            <Link to="/">
              <img src={gazpromLogo} alt="Газпромбанк" width={30} height={30} />
              <span className={styles.textLogo}>ГАЗПРОМБАНК</span>
            </Link>
          </div>

          <div className={styles.actionsContainer}>
            <button
              className={styles.predictBtn}
              aria-label="Предсказать"
              onClick={handlePredict}
            >
              <span className={styles.predictText}>Предсказать</span>
              <img src={uploadIcon} alt="" width={20} height={20} />
            </button>

            <button
              className={styles.downloadBtn}
              aria-label="Скачать"
              onClick={handleDownload}
            >
              <span className={styles.downloadText}>Скачать</span>
              <img src={downloadIcon} alt="" width={20} height={20} />
            </button>

            <div className={styles.accountSection}>
              {isAuthenticated && user ? (
                <div className={styles.userInfo}>
                  <span className={styles.userName}>{user.username}</span>
                  <button
                    className={styles.logoutBtn}
                    onClick={handleLogout}
                    title="Выйти"
                  >
                    Выйти
                  </button>
                </div>
              ) : (
                <button
                  className={styles.iconBtn}
                  aria-label="Войти"
                  onClick={handleAccountClick}
                >
                  <img src={accountIcon} alt="Войти" width={20} height={20} />
                </button>
              )}
            </div>
          </div>
        </div>

        <nav className={styles.navBar} aria-label="Основная навигация">
          <ul className={styles.navList}>
            {navItems.map((item) => (
              <li key={item.name}>
                <Link
                  to={item.path}
                  className={`${styles.navLink} ${isActive(item.path)}`}
                >
                  {item.name}
                </Link>
              </li>
            ))}
          </ul>
        </nav>
      </header>

      <AuthModal
        isOpen={showAuthModal}
        onClose={handleAuthModalClose}
        defaultTab="login"
      />
    </>
  );
};

export default Header;