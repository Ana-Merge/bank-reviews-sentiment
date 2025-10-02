import { useState, useEffect, useRef } from "react";
import { useCategoryFromPath } from "../../../hooks/useCategoryFromPath";
import styles from "./ProductFilter.module.scss";
import LoadingSpinner from "../../ui/LoadingSpinner/LoadingSpinner";

const ProductFilter = ({ onProductSelect, selectedProduct, productTree, allowBackFromRoot = false }) => {
  const [filteredTree, setFilteredTree] = useState([]);
  const [isOpen, setIsOpen] = useState(false);
  const [currentLevel, setCurrentLevel] = useState(0);
  const [selectedItems, setSelectedItems] = useState([]);
  const dropdownRef = useRef(null);

  const { autoCategory } = useCategoryFromPath();

  const findProductPathInTree = (tree, productId) => {
    for (const item of tree) {
      if (item.id === productId) {
        return [item];
      }
      if (item.children) {
        const path = findProductPathInTree(item.children, productId);
        if (path.length > 0) {
          return [item, ...path];
        }
      }
    }
    return [];
  };

  useEffect(() => {
    if (!productTree) {
      setFilteredTree([]);
      return;
    }

    const filtered = autoCategory
      ? productTree.filter(
        (category) =>
          category.name.toLowerCase() === autoCategory.toLowerCase()
      )
      : productTree;

    setFilteredTree(filtered);

    if (filtered.length > 0) {
      const defaultSelection = filtered[0];
      if (!selectedProduct) {
        onProductSelect(defaultSelection);
        setSelectedItems([defaultSelection]);
      } else {
        const path = findProductPathInTree(filtered, selectedProduct.id);
        setSelectedItems(path);
      }
    } else {
      onProductSelect(null);
      setSelectedItems([]);
    }
  }, [autoCategory, productTree, selectedProduct, onProductSelect]);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
        if (selectedProduct && filteredTree.length > 0) {
          const path = findProductPathInTree(filteredTree, selectedProduct.id);
          setSelectedItems(path);
          setCurrentLevel(path.length - 1);
        } else {
          setSelectedItems(filteredTree.length > 0 ? [filteredTree[0]] : []);
          setCurrentLevel(0);
        }
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [selectedProduct, filteredTree]);

  const handleToggle = () => {
    if (filteredTree.length === 0) {
      return;
    }

    setIsOpen(!isOpen);
    if (!isOpen && selectedProduct) {
      const path = findProductPathInTree(filteredTree, selectedProduct.id);
      setSelectedItems(path);
      setCurrentLevel(path.length);
    }
  };

  const handleItemClick = (item, level) => {
    onProductSelect(item);
    if (item.children && item.children.length > 0) {
      const newSelected = [...selectedItems.slice(0, level), item];
      setSelectedItems(newSelected);
      setCurrentLevel(level + 1);
    } else {
      setIsOpen(false);
    }
  };

  const handleBack = () => {
    if (currentLevel > 1) {
      const newLevel = currentLevel - 1;
      setCurrentLevel(newLevel);
      const newSelectedItems = selectedItems.slice(0, newLevel);
      setSelectedItems(newSelectedItems);

      if (newSelectedItems.length > 0) {
        onProductSelect(newSelectedItems[newSelectedItems.length - 1]);
      }
    } else if (currentLevel === 1 && allowBackFromRoot) {
      setCurrentLevel(0);
      setSelectedItems([]);
      onProductSelect(null);
    }
  };

  const handleClear = () => {
    if (filteredTree.length > 0) {
      onProductSelect(filteredTree[0]);
    }
    setIsOpen(false);
  };

  const getCurrentItems = () => {
    if (currentLevel === 0) {
      return filteredTree;
    }
    const parent = selectedItems[currentLevel - 1];
    return parent && parent.children ? parent.children : [];
  };

  const getDisplayText = () => {
    if (!selectedProduct) {
      return autoCategory || "Выберите продукт";
    }
    return selectedProduct.name;
  };

  const isRootCategorySelected = () => {
    if (!selectedProduct || filteredTree.length === 0) return false;
    return selectedProduct.id === filteredTree[0]?.id;
  };

  const canShowBackButton = () => {
    if (currentLevel > 1) {
      return true;
    }
    if (currentLevel === 1 && allowBackFromRoot) {
      return true;
    }
    return false;
  };

  if (!productTree) {
    return <div className={styles.filter}><LoadingSpinner /></div>;
  }

  if (filteredTree.length === 0) {
    return <div className={styles.filter}>Нет доступных продуктов</div>;
  }

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
          {canShowBackButton() && (
            <div className={styles.breadcrumbs}>
              <button
                className={styles.backButton}
                onClick={handleBack}
              >
                ← Назад
              </button>
              <span className={styles.breadcrumbText}>
                {selectedItems.slice(0, currentLevel).map((item) => item.name).join(" / ")}
              </span>
            </div>
          )}

          <div className={styles.itemsList}>
            {getCurrentItems().map((item) => (
              <button
                key={item.id}
                className={`${styles.itemButton} ${selectedProduct?.id === item.id ? styles.selected : ""
                  }`}
                onClick={() => handleItemClick(item, currentLevel)}
                type="button"
              >
                <span className={styles.itemName}>{item.name}</span>
                {item.children && item.children.length > 0 && (
                  <span className={styles.arrow}>→</span>
                )}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default ProductFilter;