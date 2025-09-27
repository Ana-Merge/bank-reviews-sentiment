import { useState, useRef, useEffect } from "react";
import styles from "./AggregationFilter.module.scss";

const AggregationFilter = ({ aggregationType, onAggregationChange }) => {
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

    const handleAggregationClick = (newType) => {
        onAggregationChange(newType);
        setIsOpen(false);
    };

    const aggregationOptions = [
        { value: 'day', label: 'По дням' },
        { value: 'week', label: 'По неделям' },
        { value: 'month', label: 'По месяцам' },
    ];

    const getDisplayText = () => {
        const selectedOption = aggregationOptions.find(opt => opt.value === aggregationType);
        return selectedOption ? selectedOption.label : '';
    };

    return (
        <div className={styles.filterContainer} ref={dropdownRef}>
            <label htmlFor="aggregation-select" className={styles.filterLabel}>Группировка</label>
            <button
                id="aggregation-select"
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
                        {aggregationOptions.map(option => (
                            <button
                                key={option.value}
                                className={`${styles.itemButton} ${aggregationType === option.value ? styles.selected : ""}`}
                                onClick={() => handleAggregationClick(option.value)}
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

export default AggregationFilter;