import styles from "./Footer.module.scss";

const Footer = () => {
    return (
        <footer className={styles.footer}>
            <div className={styles.footerContent}>
                <div className={styles.footerText}>
                    <span>Все отзывы взяты с </span>
                    <a
                        href="https://www.banki.ru"
                        target="_blank"
                        rel="noopener noreferrer"
                        className={styles.footerLink}
                    >
                        Banki.ru
                    </a>
                    <span> и </span>
                    <a
                        href="https://www.sravni.ru"
                        target="_blank"
                        rel="noopener noreferrer"
                        className={styles.footerLink}
                    >
                        Sravni.ru
                    </a>
                </div>
                <div className={styles.copyright}>
                    © {new Date().getFullYear()} Газпромбанк
                </div>
            </div>
        </footer>
    );
};

export default Footer;