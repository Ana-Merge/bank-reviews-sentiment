import { Link, useLocation } from "react-router-dom";
import { navItems } from "../../../constants/navigation";
import styles from "./Header.module.scss";
import gazpromLogo from "../../../assets/icons/gazprom-logo.svg";
import uploadIcon from "../../../assets/icons/upload.svg";
import searchIcon from "../../../assets/icons/icon-search.svg";
import accountIcon from "../../../assets/icons/icon-account.svg";

const Header = () => {
  const location = useLocation();

  const isActive = (path) => {
    return location.pathname === path ? styles.active : "";
  };

  return (
    <header className={styles.header}>
      <div className={styles.topBar}>
        <div className={styles.logoContainer}>
          <Link to="/">
            <img src={gazpromLogo} alt="Газпромбанк" width={30} height={30} />
            <span className={styles.textLogo}>ГАЗПРОМБАНК</span>
          </Link>
        </div>

        <div className={styles.actionsContainer}>
          <button className={styles.predictBtn} aria-label="Предсказать">
            <span className={styles.predictText}>Предсказать</span>
            <img src={uploadIcon} alt="" width={20} height={20} />
          </button>

          <button className={styles.iconBtn} aria-label="Поиск">
            <img src={searchIcon} alt="" width={20} height={20} />
          </button>
          <button className={styles.iconBtn} aria-label="Личный кабинет">
            <img src={accountIcon} alt="" width={20} height={20} />
          </button>
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
  );
};

export default Header;