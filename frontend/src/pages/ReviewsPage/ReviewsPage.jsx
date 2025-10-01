import { useState, useEffect } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { useAppDispatch, useAppSelector } from "../../hooks/redux";
import { apiService } from "../../services/api";
import { fetchProductTree, setSelectedProduct } from "../../store/slices/productSlice";
import { ProductFilter, SourceFilter, LoadingSpinner } from "../../components";
import styles from "./ReviewsPage.module.scss";

const ReviewsPage = () => {
    const location = useLocation();
    const navigate = useNavigate();
    const dispatch = useAppDispatch();

    const { productTree, selectedProduct } = useAppSelector(state => state.product);

    const [reviews, setReviews] = useState([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const [filters, setFilters] = useState({
        sentiment: '',
        source: null,
        orderBy: 'asc'
    });
    const [pagination, setPagination] = useState({
        currentPage: 0,
        totalPages: 0,
        total: 0
    });
    const [pageInput, setPageInput] = useState("");

    const queryParams = new URLSearchParams(location.search);
    const initialProductId = queryParams.get('product_id');
    const startDate = queryParams.get('start_date');
    const endDate = queryParams.get('end_date');
    const initialSource = queryParams.get('source');

    useEffect(() => {
        dispatch(fetchProductTree());
    }, [dispatch]);

    useEffect(() => {
        if (initialSource) {
            setFilters(prev => ({ ...prev, source: initialSource }));
        }
    }, [initialSource]);

    useEffect(() => {
        if (productTree && initialProductId && !selectedProduct) {
            const findProductById = (items, targetId) => {
                for (const item of items) {
                    if (item.id === parseInt(targetId)) return item;
                    if (item.children) {
                        const found = findProductById(item.children, targetId);
                        if (found) return found;
                    }
                }
                return null;
            };

            const product = findProductById(productTree, initialProductId);
            if (product) {
                dispatch(setSelectedProduct(product));
            }
        }
    }, [productTree, initialProductId, selectedProduct, dispatch]);

    useEffect(() => {
        if (selectedProduct) {
            loadReviews(0);
        }
    }, [selectedProduct, filters, startDate, endDate]);

    const loadReviews = async (page = 0) => {
        if (!selectedProduct) return;

        setIsLoading(true);
        setError(null);

        try {
            const response = await apiService.getReviews(
                selectedProduct.id,
                startDate || null,
                endDate || null,
                filters.source || null,
                filters.sentiment || null,
                filters.orderBy,
                page,
                30
            );

            setReviews(response.reviews);
            setPagination({
                currentPage: page,
                totalPages: Math.ceil(response.total / 30),
                total: response.total
            });
            setPageInput("");
        } catch (err) {
            setError(err.message);
        } finally {
            setIsLoading(false);
        }
    };

    const handleProductSelect = (product) => {
        dispatch(setSelectedProduct(product));

        const newParams = new URLSearchParams();
        newParams.set('product_id', product.id);
        if (startDate) newParams.set('start_date', startDate);
        if (endDate) newParams.set('end_date', endDate);
        if (filters.source) newParams.set('source', filters.source);

        navigate(`/reviews?${newParams.toString()}`, { replace: true });
    };

    const handleFilterChange = (key, value) => {
        setFilters(prev => ({
            ...prev,
            [key]: value
        }));
    };

    const handleSourceChange = (source) => {
        handleFilterChange('source', source);

        const newParams = new URLSearchParams(location.search);
        if (source) {
            newParams.set('source', source);
        } else {
            newParams.delete('source');
        }
        navigate(`/reviews?${newParams.toString()}`, { replace: true });
    };

    const handlePageChange = (newPage) => {
        if (newPage >= 0 && newPage < pagination.totalPages) {
            loadReviews(newPage);
        }
    };

    const handlePageInputChange = (e) => {
        const value = e.target.value;
        if (value === "" || (/^\d+$/.test(value) && parseInt(value) >= 1 && parseInt(value) <= pagination.totalPages)) {
            setPageInput(value);
        }
    };

    const handlePageInputSubmit = () => {
        if (pageInput && parseInt(pageInput) >= 1 && parseInt(pageInput) <= pagination.totalPages) {
            handlePageChange(parseInt(pageInput) - 1);
        }
    };

    const handlePageInputKeyPress = (e) => {
        if (e.key === 'Enter') {
            handlePageInputSubmit();
        }
    };

    const getVisiblePages = () => {
        const current = pagination.currentPage + 1;
        const total = pagination.totalPages;
        const delta = 2;
        const range = [];
        const rangeWithDots = [];

        for (let i = Math.max(2, current - delta); i <= Math.min(total - 1, current + delta); i++) {
            range.push(i);
        }

        if (current - delta > 2) {
            rangeWithDots.push(1, '...');
        } else {
            rangeWithDots.push(1);
        }

        rangeWithDots.push(...range);

        if (current + delta < total - 1) {
            rangeWithDots.push('...', total);
        } else if (total > 1) {
            rangeWithDots.push(total);
        }

        return rangeWithDots;
    };

    const getProductName = (productId) => {
        if (!productTree) return `Продукт ${productId}`;

        const findProduct = (items, targetId) => {
            for (const item of items) {
                if (item.id === targetId) return item;
                if (item.children) {
                    const found = findProduct(item.children, targetId);
                    if (found) return found;
                }
            }
            return null;
        };

        const product = findProduct(productTree, productId);
        return product ? product.name : `Продукт ${productId}`;
    };

    const getSentimentLabel = (sentiment) => {
        switch (sentiment) {
            case 'positive': return 'Позитивный';
            case 'neutral': return 'Нейтральный';
            case 'negative': return 'Негативный';
            default: return sentiment;
        }
    };

    const getRatingClass = (rating) => {
        if (rating <= 2) return 'rating-negative';
        if (rating === 3) return 'rating-neutral';
        return 'rating-positive';
    };

    return (
        <div className={styles.pageContainer}>
            <div className={styles.header}>
                <button
                    className={styles.backButton}
                    onClick={() => navigate(-1)}
                >
                    ← Назад
                </button>
            </div>

            <div className={styles.filtersContainer}>
                <div className={styles.filtersHeader}>
                    <h3>Фильтры</h3>
                </div>
                <div className={styles.filtersRow}>
                    <div className={styles.filterGroup}>
                        <label>Продукт:</label>
                        <ProductFilter
                            onProductSelect={handleProductSelect}
                            selectedProduct={selectedProduct}
                            productTree={productTree}
                            allowBackFromRoot={true}
                        />
                    </div>

                    <div className={styles.filterGroup}>
                        <label>Источник отзывов:</label>
                        <SourceFilter
                            source={filters.source}
                            onSourceChange={handleSourceChange}
                        />
                    </div>

                    <div className={styles.filterGroup}>
                        <label>Тональность:</label>
                        <select
                            value={filters.sentiment}
                            onChange={(e) => handleFilterChange('sentiment', e.target.value)}
                            className={styles.filterSelect}
                        >
                            <option value="">Все</option>
                            <option value="positive">Позитивные</option>
                            <option value="neutral">Нейтральные</option>
                            <option value="negative">Негативные</option>
                        </select>
                    </div>

                    <div className={styles.filterGroup}>
                        <label>Сортировка по дате:</label>
                        <select
                            value={filters.orderBy}
                            onChange={(e) => handleFilterChange('orderBy', e.target.value)}
                            className={styles.filterSelect}
                        >
                            <option value="asc">Сначала старые</option>
                            <option value="desc">Сначала новые</option>
                        </select>
                    </div>
                </div>
            </div>

            {!selectedProduct && (
                <div className={styles.infoMessage}>
                    Выберите продукт для просмотра отзывов
                </div>
            )}

            {selectedProduct && isLoading && <div className={styles.loading}><LoadingSpinner /></div>}

            {selectedProduct && error && <div className={styles.error}>{error}</div>}

            {selectedProduct && !isLoading && !error && (
                <>
                    <div className={styles.resultsInfo}>
                        Найдено отзывов: {pagination.total}
                    </div>

                    <div className={styles.reviewsList}>
                        {reviews.map((review) => (
                            <div key={review.id} className={styles.reviewCard}>
                                <div className={styles.reviewHeader}>
                                    <span className={styles.source}>{review.source}</span>
                                    <span className={styles.date}>{review.date}</span>
                                    <span className={`${styles.rating} ${styles[getRatingClass(review.rating)]}`}>
                                        Рейтинг: {review.rating}/5
                                    </span>
                                </div>

                                <div className={styles.reviewText}>
                                    {review.text}
                                </div>

                                <div className={styles.sentiments}>
                                    {review.sentiment && review.sentiment.map((sentimentItem, index) => (
                                        <div key={index} className={`${styles.sentimentItem} ${styles[sentimentItem.sentiment]}`}>
                                            <span className={styles.productName}>
                                                {getProductName(sentimentItem.product_id)}
                                            </span>
                                            <span className={`${styles.sentiment} ${styles[sentimentItem.sentiment]}`}>
                                                {getSentimentLabel(sentimentItem.sentiment)}
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ))}
                    </div>

                    {pagination.totalPages > 1 && (
                        <div className={styles.pagination}>
                            <div className={styles.paginationControls}>
                                <button
                                    disabled={pagination.currentPage === 0}
                                    onClick={() => handlePageChange(pagination.currentPage - 1)}
                                    className={styles.paginationButton}
                                >
                                    ← Предыдущая
                                </button>

                                <div className={styles.pageNumbers}>
                                    {getVisiblePages().map((page, index) => (
                                        page === '...' ? (
                                            <span key={index} className={styles.pageDots}>...</span>
                                        ) : (
                                            <button
                                                key={index}
                                                onClick={() => handlePageChange(page - 1)}
                                                className={`${styles.pageNumber} ${pagination.currentPage + 1 === page ? styles.active : ''}`}
                                            >
                                                {page}
                                            </button>
                                        )
                                    ))}
                                </div>

                                <button
                                    disabled={pagination.currentPage >= pagination.totalPages - 1}
                                    onClick={() => handlePageChange(pagination.currentPage + 1)}
                                    className={styles.paginationButton}
                                >
                                    Следующая →
                                </button>
                            </div>

                            <div className={styles.pageJump}>
                                <span>Перейти на страницу:</span>
                                <input
                                    type="text"
                                    value={pageInput}
                                    onChange={handlePageInputChange}
                                    onKeyPress={handlePageInputKeyPress}
                                    className={styles.pageInput}
                                    placeholder="№"
                                />
                                <button
                                    onClick={handlePageInputSubmit}
                                    disabled={!pageInput}
                                    className={styles.pageJumpButton}
                                >
                                    Перейти
                                </button>
                            </div>
                        </div>
                    )}
                </>
            )}
        </div>
    );
};

export default ReviewsPage;