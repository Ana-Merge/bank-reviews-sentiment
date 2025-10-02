import { useState } from "react";
import { authService } from "../services/auth";

export const usePageOperations = (token) => {
    const [isSaving, setIsSaving] = useState(false);
    const [error, setError] = useState(null);

    const savePageToMyDashboards = async (page, sourceUsername) => {
        if (!confirm(`Сохранить страницу "${page.name}" к себе?`)) {
            return;
        }

        setIsSaving(true);
        setError(null);

        try {
            const newPageId = Date.now().toString();
            const newPage = {
                ...page,
                id: newPageId,
                name: `${page.name} (скопировано у ${sourceUsername})`,
                charts: page.charts?.map(chart => ({
                    ...chart,
                    id: Date.now().toString() + Math.random().toString(36).substr(2, 9)
                })) || []
            };

            const currentConfig = await authService.getUserDashboardsConfig(token);
            const updatedPages = [...(currentConfig.pages || []), newPage];

            await authService.saveUserDashboardsConfig(token, { pages: updatedPages });

            return true;
        } catch (err) {
            setError(`Ошибка сохранения страницы: ${err.message}`);
            console.error("Failed to save page:", err);
            return false;
        } finally {
            setIsSaving(false);
        }
    };

    return {
        isSaving,
        error,
        savePageToMyDashboards,
        setError
    };
};