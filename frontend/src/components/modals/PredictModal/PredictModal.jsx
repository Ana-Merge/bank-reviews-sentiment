import { useState, useRef } from "react";
import { predictService } from "../../../services/predict";
import { LoadingSpinner } from "../..";
import styles from "./PredictModal.module.scss";

const PredictModal = ({ isOpen, onClose }) => {
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(false);
    const [isDragOver, setIsDragOver] = useState(false);
    const fileInputRef = useRef(null);

    if (!isOpen) return null;

    const processFile = async (file) => {
        if (!file) return;

        if (file.type !== "application/json") {
            setError("Пожалуйста, выберите JSON файл");
            return;
        }

        setIsLoading(true);
        setError(null);
        setSuccess(false);

        try {
            const text = await file.text();
            const jsonData = JSON.parse(text);

            if (!Array.isArray(jsonData)) {
                throw new Error("JSON должен содержать массив объектов");
            }

            // Валидация структуры данных
            const isValid = jsonData.every(item =>
                item && typeof item === 'object' &&
                'id' in item && 'text' in item &&
                typeof item.text === 'string'
            );

            if (!isValid) {
                throw new Error("Каждый объект должен содержать 'id' и 'text' поля");
            }

            // Отправка на ML-сервис (без токена)
            const result = await predictService.predict(jsonData);

            // Создание и скачивание результата
            const blob = new Blob([JSON.stringify(result, null, 2)], {
                type: 'application/json'
            });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `predictions_${Date.now()}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);

            setSuccess(true);
            setTimeout(() => {
                onClose();
                resetState();
            }, 2000);

        } catch (err) {
            setError(err.message || "Ошибка при обработке файла");
        } finally {
            setIsLoading(false);
        }
    };

    const handleFileSelect = async (event) => {
        const file = event.target.files[0];
        await processFile(file);
    };

    const handleDragOver = (e) => {
        e.preventDefault();
        setIsDragOver(true);
    };

    const handleDragLeave = (e) => {
        e.preventDefault();
        setIsDragOver(false);
    };

    const handleDrop = (e) => {
        e.preventDefault();
        setIsDragOver(false);

        const files = e.dataTransfer.files;
        if (files.length > 0) {
            processFile(files[0]);
        }
    };

    const handleUploadAreaClick = () => {
        if (!isLoading) {
            fileInputRef.current?.click();
        }
    };

    const resetState = () => {
        setIsLoading(false);
        setError(null);
        setSuccess(false);
        setIsDragOver(false);
        if (fileInputRef.current) {
            fileInputRef.current.value = "";
        }
    };

    const handleClose = () => {
        onClose();
        resetState();
    };

    const handleOverlayClick = (e) => {
        if (e.target === e.currentTarget) {
            handleClose();
        }
    };

    return (
        <div className={styles.modalOverlay} onClick={handleOverlayClick}>
            <div className={styles.modal}>
                <div className={styles.modalHeader}>
                    <h2>Анализ отзывов</h2>
                    <button
                        className={styles.closeButton}
                        onClick={handleClose}
                        disabled={isLoading}
                    >
                        ×
                    </button>
                </div>

                <div className={styles.modalContent}>
                    <div className={styles.instructions}>
                        <p>Загрузите JSON файл с отзывами для анализа тональности и тем.</p>
                        <div className={styles.example}>
                            <strong>Пример структуры:</strong>
                            <pre>{`[
  {
    "id": 1,
    "text": "Текст отзыва..."
  }
]`}</pre>
                        </div>
                    </div>

                    <div
                        className={`${styles.uploadSection} ${isDragOver ? styles.dragOver : ''} ${isLoading ? styles.disabled : ''}`}
                        onDragOver={handleDragOver}
                        onDragLeave={handleDragLeave}
                        onDrop={handleDrop}
                        onClick={handleUploadAreaClick}
                    >
                        <input
                            ref={fileInputRef}
                            type="file"
                            accept=".json"
                            onChange={handleFileSelect}
                            disabled={isLoading}
                            className={styles.fileInput}
                            id="predict-file"
                        />
                        <div className={styles.uploadContent}>
                            {isLoading ? (
                                <div className={styles.loadingContainer}>
                                    <LoadingSpinner />
                                </div>
                            ) : (
                                <div className={styles.uploadText}>
                                    <div className={styles.uploadTitle}>Загрузить файл</div>
                                    <div className={styles.uploadSubtitle}>
                                        Перетащите JSON файл сюда или нажмите для выбора
                                    </div>
                                    <div className={styles.uploadHint}>
                                        Поддерживается только JSON формат
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>

                    {error && (
                        <div className={styles.error}>
                            {error}
                        </div>
                    )}

                    {success && (
                        <div className={styles.success}>
                            <div className={styles.successTitle}>Анализ завершен!</div>
                            <div className={styles.successSubtitle}>Файл с результатами скачивается</div>
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

export default PredictModal;