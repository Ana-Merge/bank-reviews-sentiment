import { useState, useEffect, useRef } from "react";
import {
    DateFilter,
    ProductFilter,
    SourceFilter,
    AggregationFilter
} from "../../components";
import styles from "./AddChartModal.module.scss";
import productStatsIcon from "../../assets/icons/product-stats.png";
import tonalityIcon from "../../assets/icons/tonality-chart.png";
import dynamicsIcon from "../../assets/icons/dynamics-chart.png";
import generalStatsIcon from "../../assets/icons/general-stats.png";

// Компонент для выбора режима фильтра дат
const DateModeFilter = ({ dateMode, onDateModeChange }) => {
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

    const handleModeClick = (newMode) => {
        onDateModeChange(newMode);
        setIsOpen(false);
    };

    const modeOptions = [
        { value: 'month', label: 'По месяцам' },
        { value: 'day', label: 'По дням' },
    ];

    const getDisplayText = () => {
        const selectedOption = modeOptions.find(opt => opt.value === dateMode);
        return selectedOption ? selectedOption.label : '';
    };

    return (
        <div className={styles.filterContainer} ref={dropdownRef}>
            <button
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
                        {modeOptions.map(option => (
                            <button
                                key={option.value}
                                className={`${styles.itemButton} ${dateMode === option.value ? styles.selected : ""}`}
                                onClick={() => handleModeClick(option.value)}
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

const AddChartModal = ({ isOpen, onClose, onSave, productTree, token, editingChart }) => {
    const [chartType, setChartType] = useState('product_stats');
    const [selectedProduct, setSelectedProduct] = useState(null);
    const [source, setSource] = useState(null);
    const [aggregationType, setAggregationType] = useState('month');
    const [dateMode, setDateMode] = useState('month');

    // Даты по умолчанию
    const [startDate, setStartDate] = useState("2025-03-01");
    const [endDate, setEndDate] = useState("2025-05-31");
    const [startDate2, setStartDate2] = useState("2024-12-01");
    const [endDate2, setEndDate2] = useState("2025-02-28");

    const [dateErrors, setDateErrors] = useState({});
    const [isSaving, setIsSaving] = useState(false);

    // Получение типа агрегации для DateFilter
    const getEffectiveAggregationType = () => {
        return supportsAggregation(chartType) ? aggregationType : dateMode;
    };

    // Инициализация формы при открытии или изменении editingChart
    useEffect(() => {
        if (isOpen) {
            if (editingChart) {
                // Режим редактирования 
                const { type, attributes } = editingChart;
                setChartType(type);
                setSource(attributes.source || null);
                setAggregationType(attributes.aggregation_type || 'month');
                setDateMode(attributes.date_mode || 'month');
                setStartDate(attributes.date_start_1);
                setEndDate(attributes.date_end_1);
                setStartDate2(attributes.date_start_2);
                setEndDate2(attributes.date_end_2);

                if (productTree && attributes.product_id) {
                    const findProduct = (nodes) => {
                        for (let node of nodes) {
                            if (node.id === attributes.product_id) {
                                return node;
                            }
                            if (node.children) {
                                const found = findProduct(node.children);
                                if (found) return found;
                            }
                        }
                        return null;
                    };
                    const product = findProduct(productTree);
                    setSelectedProduct(product);
                }
            } else {
                resetForm();
            }
        }
    }, [isOpen, editingChart, productTree]);

    const resetForm = () => {
        setChartType('product_stats');
        setSelectedProduct(null);
        setSource(null);
        setAggregationType('month');
        setDateMode('month');
        setStartDate("2025-03-01");
        setEndDate("2025-05-31");
        setStartDate2("2024-12-01");
        setEndDate2("2025-02-28");
        setDateErrors({});
    };

    const handleSave = async () => {
        if (!selectedProduct) {
            alert("Выберите продукт");
            return;
        }

        if (Object.keys(dateErrors).length > 0) {
            alert("Исправьте ошибки в настройках дат");
            return;
        }

        setIsSaving(true);

        try {
            const chartData = {
                id: editingChart ? editingChart.id : Date.now().toString(),
                name: getChartTypeName(chartType),
                type: chartType,
                attributes: {
                    date_start_1: startDate,
                    date_end_1: endDate,
                    date_start_2: startDate2,
                    date_end_2: endDate2,
                    product_id: parseInt(selectedProduct.id),
                    product_name: selectedProduct.name,
                    source: source || '',
                    aggregation_type: supportsAggregation(chartType) ? aggregationType : 'month',
                    date_mode: supportsAggregation(chartType) ? null : dateMode
                }
            };

            await onSave(chartData);
            onClose();
        } catch (error) {
            console.error("Failed to save chart:", error);
            alert(`Ошибка сохранения: ${error.message}`);
        } finally {
            setIsSaving(false);
        }
    };

    const supportsAggregation = (type) => {
        return ['monthly-review-count', 'regional-bar-chart'].includes(type);
    };

    const getChartTypeName = (type) => {
        const typeNames = {
            'product_stats': 'Статистика продуктов',
            'monthly-review-count': 'Тональность отзывов',
            'regional-bar-chart': 'Динамика отзывов',
            'change-chart': 'Общая статистика'
        };
        return typeNames[type] || type;
    };

    const getChartTypeOptions = () => [
        {
            value: 'product_stats',
            label: 'Статистика продуктов',
            description: 'Таблица с аналитикой продуктов',
            icon: productStatsIcon
        },
        {
            value: 'monthly-review-count',
            label: 'Тональность отзывов',
            description: 'График тональности отзывов по периодам',
            icon: tonalityIcon
        },
        {
            value: 'regional-bar-chart',
            label: 'Динамика отзывов',
            description: 'График динамики количества отзывов',
            icon: dynamicsIcon
        },
        {
            value: 'change-chart',
            label: 'Общая статистика',
            description: 'Круговая диаграмма общей статистики',
            icon: generalStatsIcon
        }
    ];

    if (!isOpen) return null;

    return (
        <div className={styles.modalOverlay} onClick={onClose}>
            <div className={styles.modalContent} onClick={e => e.stopPropagation()}>
                <div className={styles.modalHeader}>
                    <h2>{editingChart ? 'Редактировать график' : 'Добавить новый график'}</h2>
                    <button className={styles.closeButton} onClick={onClose}>×</button>
                </div>

                <div className={styles.modalBody}>
                    {/* Выбор типа графика */}
                    <div className={styles.formSection}>
                        <label className={styles.sectionLabel}>Тип графика</label>
                        <div className={styles.chartTypeGrid}>
                            {getChartTypeOptions().map(option => (
                                <div
                                    key={option.value}
                                    className={`${styles.chartTypeOption} ${chartType === option.value ? styles.selected : ''}`}
                                    onClick={() => setChartType(option.value)}
                                >
                                    <div className={styles.chartTypeIcon}>
                                        <img
                                            src={option.icon}
                                            alt={option.label}
                                            className={styles.iconImage}
                                        />
                                    </div>
                                    <div className={styles.chartTypeContent}>
                                        <div className={styles.chartTypeName}>{option.label}</div>
                                        <div className={styles.chartTypeDescription}>{option.description}</div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>

                    {/* Фильтры продукта, источника и группировки/фильтра */}
                    <div className={styles.filtersRow}>
                        <div className={styles.filterGroup}>
                            <label className={styles.sectionLabel}>Продукт</label>
                            <ProductFilter
                                onProductSelect={setSelectedProduct}
                                selectedProduct={selectedProduct}
                                productTree={productTree}
                                allowBackFromRoot={true}
                            />
                        </div>
                        <div className={styles.filterGroup}>
                            <label className={styles.sectionLabel}>Источник отзывов:</label>
                            <SourceFilter
                                source={source}
                                onSourceChange={setSource}
                            />
                        </div>
                        <div className={styles.filterGroup}>
                            <label className={styles.sectionLabel}>
                                {supportsAggregation(chartType) ? 'Группировка на графике:' : 'Выбор даты:'}
                            </label>
                            {supportsAggregation(chartType) ? (
                                <AggregationFilter
                                    aggregationType={aggregationType}
                                    onAggregationChange={setAggregationType}
                                />
                            ) : (
                                <DateModeFilter
                                    dateMode={dateMode}
                                    onDateModeChange={setDateMode}
                                />
                            )}
                        </div>
                    </div>

                    {/* Фильтр дат */}
                    <div className={styles.formSection}>
                        <label className={styles.sectionLabel}>Периоды сравнения</label>
                        <DateFilter
                            startDate={startDate}
                            endDate={endDate}
                            startDate2={startDate2}
                            endDate2={endDate2}
                            onStartDateChange={setStartDate}
                            onEndDateChange={setEndDate}
                            onStartDate2Change={setStartDate2}
                            onEndDate2Change={setEndDate2}
                            selectedProduct={selectedProduct}
                            aggregationType={getEffectiveAggregationType()}
                            onDateErrorsChange={setDateErrors}
                        />
                    </div>
                </div>

                <div className={styles.modalFooter}>
                    <button
                        className={styles.cancelButton}
                        onClick={onClose}
                        disabled={isSaving}
                    >
                        Отмена
                    </button>
                    <button
                        className={styles.saveButton}
                        onClick={handleSave}
                        disabled={isSaving || !selectedProduct || Object.keys(dateErrors).length > 0}
                    >
                        {isSaving ? "Сохранение..." : (editingChart ? "Сохранить изменения" : "Сохранить график")}
                    </button>
                </div>
            </div>
        </div>
    );
};

export default AddChartModal;