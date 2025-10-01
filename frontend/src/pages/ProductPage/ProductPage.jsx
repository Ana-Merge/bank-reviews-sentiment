import { useEffect } from "react";
import { ProductAnalyticsTable, ProductFilter, DateFilter, SourceFilter, BarChartReviews, AggregationFilter, LoadingSpinner, ChangeChart, TonalityChart } from "../../components";
import styles from "./ProductPage.module.scss";
import { useAppDispatch, useAppSelector } from "../../hooks/redux";
import { fetchProductTree, setSelectedProduct, setCategoryId } from "../../store/slices/productSlice";
import { setStartDate, setEndDate, setStartDate2, setEndDate2, setAggregationType, setSource, setDateErrors } from "../../store/slices/dateSlice";
import { fetchProductStats, fetchBarChartData, fetchChangeChartData, fetchTonalityChartData, clearChartData } from "../../store/slices/chartSlice";
import { useCategoryFromPath } from "../../hooks/useCategoryFromPath";
import { useNavigate } from "react-router-dom";

const ProductPage = () => {
    const dispatch = useAppDispatch();
    const navigate = useNavigate();
    const { categoryName } = useCategoryFromPath();

    const {
        productTree,
        selectedProduct,
        categoryId,
        isLoadingTree,
        errorProduct
    } = useAppSelector(state => state.product);

    const {
        startDate,
        endDate,
        startDate2,
        endDate2,
        aggregationType,
        source,
        dateErrors,
        showComparison
    } = useAppSelector(state => state.date);

    const {
        productStats,
        barChartData,
        changeChartData,
        tonalityChartData,
        isLoadingProduct,
        isLoadingChart,
        isLoadingChangeChart,
        isLoadingTonalityChart,
        errorChart,
        errorChangeChart,
        errorTonalityChart
    } = useAppSelector(state => state.chart);

    useEffect(() => {
        dispatch(fetchProductTree());
    }, [dispatch]);

    useEffect(() => {
        if (!productTree) return;

        dispatch(clearChartData());
        dispatch(setSelectedProduct(null));
        dispatch(setCategoryId(null));

        const category = productTree.find(item =>
            item.name.toLowerCase() === categoryName.toLowerCase()
        );

        if (category) {
            dispatch(setCategoryId(category.id));
            dispatch(setSelectedProduct(category));
        }
    }, [categoryName, productTree, dispatch]);

    useEffect(() => {
        if (!startDate || !endDate || !selectedProduct || hasDateErrors()) return;

        const productId = selectedProduct.id;

        const effectiveStartDate2 = showComparison ? startDate2 : "2026-01-01";
        const effectiveEndDate2 = showComparison ? endDate2 : "2026-01-01";

        dispatch(fetchChangeChartData({
            productId,
            startDate,
            endDate,
            startDate2: effectiveStartDate2,
            endDate2: effectiveEndDate2,
            source
        }));
        dispatch(fetchProductStats({
            startDate,
            endDate,
            startDate2: effectiveStartDate2,
            endDate2: effectiveEndDate2,
            selectedProduct,
            categoryId,
            source
        }));
        dispatch(fetchBarChartData({
            productId,
            startDate,
            endDate,
            startDate2: effectiveStartDate2,
            endDate2: effectiveEndDate2,
            aggregationType,
            source
        }));
        dispatch(fetchTonalityChartData({
            productId,
            startDate,
            endDate,
            startDate2: effectiveStartDate2,
            endDate2: effectiveEndDate2,
            aggregationType,
            source
        }));
    }, [startDate, endDate, startDate2, endDate2, selectedProduct, aggregationType, source, dateErrors, dispatch, categoryId, showComparison]);

    const handleProductSelect = (product) => {
        dispatch(setSelectedProduct(product));
    };

    const handleDateErrorsChange = (errors) => {
        dispatch(setDateErrors(errors));
    };

    const handleViewReviews = () => {
        if (!selectedProduct) return;

        const queryParams = new URLSearchParams({
            product_id: selectedProduct.id,
            start_date: startDate || '',
            end_date: endDate || '',
            source: source || '',
        });

        navigate(`/reviews?${queryParams.toString()}`);
    };

    const hasDateErrors = () => Object.keys(dateErrors).length > 0;

    if (isLoadingTree) {
        return <div className={styles.pageContainer}><div className={styles.loading}> данных продуктов...</div></div>;
    }

    if (!productTree && errorProduct) {
        return <div className={styles.pageContainer}><div className={styles.error}>{errorProduct}</div></div>;
    }

    return (
        <div className={styles.pageContainer}>
            <div className={styles.filtersContainer}>
                <div className={styles.filtersHeader}><h3>Фильтры</h3></div>
                <div className={styles.filtersContent}>
                    <div className={styles.dateFilterSection}>
                        <DateFilter
                            startDate={startDate}
                            endDate={endDate}
                            startDate2={startDate2}
                            endDate2={endDate2}
                            onStartDateChange={(date) => dispatch(setStartDate(date))}
                            onEndDateChange={(date) => dispatch(setEndDate(date))}
                            onStartDate2Change={(date) => dispatch(setStartDate2(date))}
                            onEndDate2Change={(date) => dispatch(setEndDate2(date))}
                            selectedProduct={selectedProduct}
                            aggregationType={aggregationType}
                            onDateErrorsChange={handleDateErrorsChange}
                        />
                    </div>
                    <div className={styles.filtersRow}>
                        <div className={styles.filterGroup}>
                            <label>Продукт:</label>
                            <ProductFilter
                                onProductSelect={handleProductSelect}
                                selectedProduct={selectedProduct}
                                productTree={productTree}
                            />
                        </div>
                        <div className={styles.filterGroup}>
                            <label>Источник отзывов:</label>
                            <SourceFilter
                                source={source}
                                onSourceChange={(source) => dispatch(setSource(source))}
                            />
                        </div>
                        <div className={styles.filterGroup}>
                            <label>Группировка на графике:</label>
                            <AggregationFilter
                                aggregationType={aggregationType}
                                onAggregationChange={(type) => dispatch(setAggregationType(type))}
                            />
                        </div>
                        <div className={styles.reviewsButtonContainer}>
                            <button
                                className={styles.reviewsButton}
                                onClick={handleViewReviews}
                                disabled={!selectedProduct}
                            >
                                Посмотреть отзывы
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            {hasDateErrors() && (
                <div className={styles.globalValidationBlocked}>
                    Данные не загружаются из-за ошибок в настройках дат
                </div>
            )}

            {!hasDateErrors() && (
                <>
                    <ChangeChartSection
                        isLoading={isLoadingChangeChart}
                        error={errorChangeChart}
                        data={changeChartData}
                        productName={selectedProduct?.name}
                        showComparison={showComparison}
                    />

                    <BarChartSection
                        isLoading={isLoadingChart}
                        error={errorChart}
                        data={barChartData}
                        aggregationType={aggregationType}
                        productName={selectedProduct?.name}
                        showComparison={showComparison}
                    />

                    <TonalityChartSection
                        isLoading={isLoadingTonalityChart}
                        error={errorTonalityChart}
                        data={tonalityChartData}
                        aggregationType={aggregationType}
                        productName={selectedProduct?.name}
                        showComparison={showComparison}
                    />

                    <TableSection
                        isLoading={isLoadingProduct}
                        error={errorProduct}
                        data={productStats}
                        showComparison={showComparison}
                    />
                </>
            )}
        </div>
    );
};

const ChangeChartSection = ({ isLoading, error, data, productName, showComparison }) => (
    <div className={styles.changeChartSection}>
        {isLoading && <div className={styles.loading}><LoadingSpinner /></div>}
        {error && <div className={styles.error}>{error}</div>}
        {!isLoading && !error && data && <ChangeChart data={data} productName={productName} showComparison={showComparison} />}
        {!isLoading && !error && !data && <div className={styles.noData}>Нет данных для отображения общей статистики</div>}
    </div>
);

const BarChartSection = ({ isLoading, error, data, aggregationType, productName, showComparison }) => (
    <div className={styles.chartSection}>
        {isLoading && <div className={styles.loading}><LoadingSpinner /></div>}
        {error && <div className={styles.error}>{error}</div>}
        {!isLoading && !error && data && <BarChartReviews chartData={data} aggregationType={aggregationType} productName={productName} showComparison={showComparison} />}
        {!isLoading && !error && (!data || !data.period1?.length) && <div className={styles.noData}>Нет данных для отображения графика</div>}
    </div>
);

const TonalityChartSection = ({ isLoading, error, data, aggregationType, productName, showComparison }) => (
    <div className={styles.tonalityChartSection}>
        {isLoading && <div className={styles.loading}><LoadingSpinner /></div>}
        {error && <div className={styles.error}>{error}</div>}
        {!isLoading && !error && data && <TonalityChart chartData={data} aggregationType={aggregationType} productName={productName} showComparison={showComparison} />}
        {!isLoading && !error && (!data || !data.period1?.length) && <div className={styles.noData}>Нет данных для отображения графика тональности</div>}
    </div>
);

const TableSection = ({ isLoading, error, data, showComparison }) => (
    <div className={styles.tableSection}>
        {isLoading && <div className={styles.loading}><LoadingSpinner /></div>}
        {error && <div className={styles.error}>{error}</div>}
        {!isLoading && !error && data?.length > 0 && <ProductAnalyticsTable productStats={data} showComparison={showComparison} />}
        {!isLoading && !error && (!data || data.length === 0) && <div className={styles.noData}>Данные не найдены для выбранного периода.</div>}
    </div>
);

export default ProductPage;