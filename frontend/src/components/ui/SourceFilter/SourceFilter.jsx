import { useState, useRef, useEffect } from "react";
import styles from "./SourceFilter.module.scss";

const SourceFilter = ({ source, onSourceChange }) => {
    const [isOpen, setIsOpen] = useState(false);
    const dropdownRef = useRef(null);

    useEffect(() => {
        const handleClickOutside = (event) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
                setIsOpen(false);
            }
        };
        document.addEventListener("mousedown", handleClickOutside);
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    const handleToggle = () => {
        setIsOpen(!isOpen);
    };

    const handleSourceClick = (newSource) => {
        onSourceChange(newSource);
        setIsOpen(false);
    };

    const sourceOptions = [
        { value: null, label: 'Все источники' },
        { value: 'Banki.ru', label: 'Banki.ru' },
        { value: 'App Store', label: 'App Store' },
        { value: 'Google Play', label: 'Google Play' },
    ];

    const getDisplayText = () => {
        const selectedOption = sourceOptions.find(opt => opt.value === source);
        return selectedOption ? selectedOption.label : 'Все источники';
    };

    return (
        <div className={styles.filterContainer} ref={dropdownRef}>
            <button
                id="source-select"
                className={`${styles.filterButton} ${isOpen ? styles.open : ""}`}
                onClick={handleToggle}
                type="button"
            >
                <span className={styles.filterText}>
                    {getDisplayText()}
                </span>
                <div className={styles.iconWrapper}>
                    <span className={styles.arrowIcon}>&#9660;</span>
                </div>
            </button>
            {isOpen && (
                <div className={styles.dropdown}>
                    <div className={styles.itemsList}>
                        {sourceOptions.map(option => (
                            <button
                                key={option.value || 'all'}
                                className={`${styles.itemButton} ${source === option.value ? styles.selected : ""}`}
                                onClick={() => handleSourceClick(option.value)}
                                type="button"
                            >
                                <span className={styles.itemName}>{option.label}</span>
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
};

export default SourceFilter;