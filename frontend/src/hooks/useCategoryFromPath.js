import { useLocation } from "react-router-dom";
import { categoryMappings } from "../constants/navigation";

export const useCategoryFromPath = () => {
    const location = useLocation();

    const getCategoryName = () => {
        const path = location.pathname;

        for (const [pathPattern, categoryName] of Object.entries(categoryMappings)) {
            if (path.includes(pathPattern)) {
                return categoryName;
            }
        }
        return "Все продукты";
    };

    const getAutoCategory = () => {
        const path = location.pathname;

        for (const [pathPattern, categoryName] of Object.entries(categoryMappings)) {
            if (path.includes(pathPattern)) {
                return categoryName;
            }
        }
        return null;
    };

    return {
        categoryName: getCategoryName(),
        autoCategory: getAutoCategory(),
    };
};